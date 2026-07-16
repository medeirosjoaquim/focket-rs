//! [`NodeCore`] — the plumbing every node embeds: identity, retry/timeout
//! config, params, successor edges, and the per-run cancellation signal.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::types::*;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Construction options for a node (the analogue of the TS constructor opts:
/// `{ name, timeoutMs, maxRetries, waitMs, concurrency, failFast }`).
#[derive(Clone, Debug, Default)]
pub struct NodeOpts {
    /// Human-readable name used in tracing/hooks.
    pub name: Option<String>,
    /// Per-node wall-clock budget (ms) for the whole `prep→exec→post`
    /// lifecycle. `0` (default) = no timeout.
    pub timeout_ms: u64,
    /// Number of retries **after** the first attempt (total attempts =
    /// `1 + max_retries`). Default `0` = a single attempt.
    pub max_retries: u32,
    /// Backoff between retries (ms).
    pub wait_ms: u64,
    /// Batch parallelism: `1` (default) = sequential, `>1` = bounded parallel.
    pub concurrency: usize,
    /// Batch failure policy: `false` (default) = failed items/branches become
    /// `null` slots; `true` = first failure throws and cancels siblings.
    pub fail_fast: bool,
    /// Whether this node processes `prep()`'s array item-by-item via
    /// `exec_item()` (the TS `BatchNode`).
    pub batch: bool,
}

impl NodeOpts {
    pub fn new() -> Self {
        Self::default()
    }

    /// Options for a batch node (`exec_item` per item).
    pub fn batch() -> Self {
        Self {
            batch: true,
            ..Self::default()
        }
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    pub fn timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }
    pub fn wait_ms(mut self, ms: u64) -> Self {
        self.wait_ms = ms;
        self
    }
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }
    pub fn fail_fast(mut self, f: bool) -> Self {
        self.fail_fast = f;
        self
    }
}

/// The per-node state the engine needs. Embed one in your node struct and
/// expose it via [`crate::FlowNode::core`] (the [`crate::impl_node_core`] macro
/// generates that plus graph cloning).
pub struct NodeCore {
    id: u64,
    name: String,
    timeout_ms: u64,
    max_retries: u32,
    wait_ms: u64,
    concurrency: usize,
    fail_fast: bool,
    batch: bool,
    params: RwLock<Params>,
    own_params: RwLock<Option<Params>>,
    successors: RwLock<HashMap<String, NodeRef>>,
    signal: Mutex<Option<CancellationToken>>,
}

impl Default for NodeCore {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeCore {
    pub fn new() -> Self {
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            name: "Node".to_string(),
            timeout_ms: 0,
            max_retries: 0,
            wait_ms: 0,
            concurrency: 1,
            fail_fast: false,
            batch: false,
            params: RwLock::new(Params::new()),
            own_params: RwLock::new(None),
            successors: RwLock::new(HashMap::new()),
            signal: Mutex::new(None),
        }
    }

    pub fn named(name: impl Into<String>) -> Self {
        let mut c = Self::new();
        c.name = name.into();
        c
    }

    pub fn with_opts(opts: NodeOpts) -> Self {
        let mut c = Self::new();
        c.apply_opts(opts);
        c
    }

    pub fn apply_opts(&mut self, opts: NodeOpts) {
        if let Some(n) = opts.name {
            self.name = n;
        }
        self.timeout_ms = opts.timeout_ms;
        self.max_retries = opts.max_retries;
        self.wait_ms = opts.wait_ms;
        if opts.concurrency > 0 {
            self.concurrency = opts.concurrency;
        }
        self.fail_fast = opts.fail_fast;
        self.batch = opts.batch;
    }

    pub fn set_name(&mut self, name: impl Into<String>) {
        self.name = name.into();
    }

    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }
    pub fn wait_ms(&self) -> u64 {
        self.wait_ms
    }
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }
    pub fn fail_fast(&self) -> bool {
        self.fail_fast
    }
    pub fn is_batch(&self) -> bool {
        self.batch
    }

    /// A copy of the current params dict.
    pub fn params(&self) -> Params {
        self.params.read().unwrap().clone()
    }

    /// One param value by key.
    pub fn param(&self, key: &str) -> Option<Value> {
        self.params.read().unwrap().get(key).cloned()
    }

    /// Params set explicitly by the user (these win over flow-injected params).
    pub fn own_params(&self) -> Option<Params> {
        self.own_params.read().unwrap().clone()
    }

    /// Set this node's own parameters. They are preserved when the node runs
    /// inside a flow — flow-level params merge in as defaults, but the node's
    /// own params win.
    pub fn set_params(&self, params: Value) {
        self.set_params_map(to_params(params));
    }

    /// [`set_params`](Self::set_params) with an already-materialized dict.
    pub fn set_params_map(&self, params: Params) {
        *self.own_params.write().unwrap() = Some(params.clone());
        *self.params.write().unwrap() = params;
    }

    /// Inject params from a flow/bundle. The node's own params (if any) are
    /// merged on top so explicitly-configured nodes keep their config.
    pub fn inject_params(&self, injected: &Params) {
        let own = self.own_params.read().unwrap().clone();
        let mut guard = self.params.write().unwrap();
        *guard = match own {
            Some(own) => {
                let mut m = injected.clone();
                m.extend(own);
                m
            }
            None => injected.clone(),
        };
    }

    pub fn has_successors(&self) -> bool {
        !self.successors.read().unwrap().is_empty()
    }

    pub fn successor_entries(&self) -> Vec<(String, NodeRef)> {
        self.successors
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Wire a successor for an action. Warns when overwriting an existing edge.
    pub fn set_successor(&self, action: &str, node: NodeRef) {
        let mut succs = self.successors.write().unwrap();
        if succs.contains_key(action) {
            log::warn!(
                "[focket-rs] {}: overwriting existing edge for action '{}'",
                self.name,
                action
            );
        }
        succs.insert(action.to_string(), node);
    }

    /// Like [`set_successor`](Self::set_successor) but never warns (used by
    /// graph cloning, which rewrites edges it just created).
    pub(crate) fn replace_successor(&self, action: &str, node: NodeRef) {
        self.successors
            .write()
            .unwrap()
            .insert(action.to_string(), node);
    }

    /// Resolve the successor for an action. Clean termination (no match)
    /// returns `None`. A soft warn is emitted only when a *named* action is
    /// returned that isn't wired but other edges exist — i.e. a likely typo.
    pub fn resolve_successor(&self, action: &Action) -> Option<NodeRef> {
        let key = action.as_deref().unwrap_or(DEFAULT_ACTION);
        let succs = self.successors.read().unwrap();
        let nxt = succs.get(key).cloned();
        if nxt.is_none() && action.is_some() && !succs.is_empty() {
            let have = succs
                .keys()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            log::warn!(
                "[focket-rs] {} returned action '{}' but no such edge is wired (have: {}). Flow ends.",
                self.name,
                action.as_ref().unwrap(),
                have
            );
        }
        nxt
    }

    /// The cancellation token for the current run, if inside one. Use it in
    /// `prep`/`exec`/`exec_item` for **cooperative** cancellation.
    pub fn signal(&self) -> Option<CancellationToken> {
        self.signal.lock().unwrap().clone()
    }

    pub(crate) fn set_signal(&self, signal: CancellationToken) {
        *self.signal.lock().unwrap() = Some(signal);
    }
}

impl Clone for NodeCore {
    /// Cloning a node copies its config and params dicts (so the clone is
    /// independent) and shares successor `Arc`s (graph cloning rewrites them
    /// afterwards). The clone gets a fresh id and no run signal.
    fn clone(&self) -> Self {
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            name: self.name.clone(),
            timeout_ms: self.timeout_ms,
            max_retries: self.max_retries,
            wait_ms: self.wait_ms,
            concurrency: self.concurrency,
            fail_fast: self.fail_fast,
            batch: self.batch,
            params: RwLock::new(self.params.read().unwrap().clone()),
            own_params: RwLock::new(self.own_params.read().unwrap().clone()),
            successors: RwLock::new(self.successors.read().unwrap().clone()),
            signal: Mutex::new(None),
        }
    }
}
