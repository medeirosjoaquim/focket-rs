//! The [`FlowNode`] trait — the fundamental building block of a flow: a
//! `prep → exec → post` lifecycle plus directed-graph wiring to successors.
//!
//! The engine is **async-first**: lifecycle bodies may be cheap or
//! long-running — the engine always `.await`s them. Retry (`max_retries`,
//! `wait_ms`, `exec_fallback`) and batch processing (`exec_item` per element
//! of `prep()`'s array) are configured through [`NodeCore`] / [`NodeOpts`],
//! which collapses the TS `BaseNode` / `Node` / `BatchNode` hierarchy into a
//! single trait (a TS `BaseNode` is observably identical to a `Node` with
//! `maxRetries: 0` and the default rethrowing `execFallback`).

use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::core::NodeCore;
use crate::error::FlowError;
use crate::hooks::*;
use crate::internal::*;
use crate::types::*;

/// A node in the graph. Implement this for your own struct (embed a
/// [`NodeCore`] and use the [`crate::impl_node_core`] macro), or use one of
/// the engine-provided kinds: [`crate::Flow`], [`crate::BatchFlow`],
/// [`crate::Subflow`], [`crate::ForkJoin`].
#[async_trait]
pub trait FlowNode: Send + Sync + 'static {
    /// Access the node's plumbing (name, params, successors, config).
    fn core(&self) -> &NodeCore;

    /// Deep-clone this node and its successor subgraph, preserving behavior
    /// and handling cycles via the memo map. Used by batch flows (parallel)
    /// to give each concurrent bundle a fresh node graph so bundles can't
    /// race on node params/state.
    fn clone_node(&self, memo: &mut CloneMemo) -> NodeRef;

    /* ------------------------- wiring (provided) ------------------------- */

    /// Human-readable name, used in tracing/hooks.
    fn name(&self) -> &str {
        self.core().name()
    }

    /// Wire the default-edge successor. Returns the successor for chaining.
    fn next(&self, node: NodeRef) -> NodeRef {
        self.core().set_successor(DEFAULT_ACTION, node.clone());
        node
    }

    /// Wire a successor for a named action. Returns the successor for chaining.
    fn next_action(&self, node: NodeRef, action: &str) -> NodeRef {
        self.core().set_successor(action, node.clone());
        node
    }

    /// Fluent wiring: `node.on("ok").to(other)`.
    fn on<'a>(&'a self, action: &'a str) -> OnBuilder<'a> {
        OnBuilder {
            core: self.core(),
            action,
        }
    }

    /// Resolve the successor for an action (`None` = clean termination).
    fn resolve_successor(&self, action: &Action) -> Option<NodeRef> {
        self.core().resolve_successor(action)
    }

    /// Set this node's own params (they win over flow-injected params).
    fn set_params(&self, params: Value) {
        self.core().set_params(params);
    }

    /// A copy of the current params dict.
    fn params(&self) -> Params {
        self.core().params()
    }

    /// One param value by key.
    fn param(&self, key: &str) -> Option<Value> {
        self.core().param(key)
    }

    /// The cancellation token for the current run (cooperative cancellation).
    fn signal(&self) -> Option<CancellationToken> {
        self.core().signal()
    }

    /* --------------------- lifecycle (override these) -------------------- */

    /// Prepare phase: extract/setup from shared context. Return value feeds `exec()`.
    async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
        Ok(Value::Null)
    }

    /// Execute phase: main logic. Return value feeds `post()`.
    async fn exec(&self, _prep_res: &Value) -> Result<Value> {
        Ok(Value::Null)
    }

    /// Post phase: update shared context and/or choose the next action.
    async fn post(
        &self,
        _shared: &mut Shared,
        _prep_res: &Value,
        _exec_res: &Value,
    ) -> Result<Action> {
        Ok(None)
    }

    /// Called when `exec()` fails on the final attempt. Default: rethrow.
    async fn exec_fallback(&self, _prep_res: &Value, err: FlowError) -> Result<Value> {
        Err(err)
    }

    /// Per-item exec for batch nodes: receives a single item from `prep()`'s
    /// array, plus the per-item cancellation token.
    async fn exec_item(&self, _item: &Value, _signal: &CancellationToken) -> Result<Value> {
        Ok(Value::Null)
    }

    /// Per-item fallback when `exec_item()` fails on the final attempt. Default: rethrow.
    async fn exec_item_fallback(&self, _item: &Value, err: FlowError) -> Result<Value> {
        Err(err)
    }

    /* --------------------------- engine (provided) ------------------------ */

    /// Single `exec()` call (no retry).
    async fn exec_once(&self, prep_res: &Value) -> Result<Value> {
        self.exec(prep_res).await
    }

    /// `exec()` with the node's retry policy — or, for batch nodes, the
    /// bounded-parallel per-item loop.
    async fn exec_with_retry(
        &self,
        self_ref: &NodeRef,
        prep_res: &Value,
        hooks: &[HookRef],
        shared: &Shared,
        path: &[String],
        signal: &CancellationToken,
    ) -> Result<Value> {
        let core = self.core();
        if core.is_batch() {
            let items: Vec<Value> = prep_res.as_array().cloned().unwrap_or_default();
            let fail_fast = core.fail_fast();
            let max_retries = core.max_retries();
            let wait_ms = core.wait_ms();
            let results = run_parallel(
                items,
                core.concurrency(),
                |item: Value, _i, sig| async move {
                    let r = retrying(
                        self_ref,
                        max_retries,
                        wait_ms,
                        || self.exec_item(&item, &sig),
                        |e| self.exec_item_fallback(&item, e),
                        hooks,
                        shared,
                        path,
                    )
                    .await;
                    match r {
                        Ok(v) => Ok(v),
                        Err(e) => {
                            if e.is_aborted() {
                                return Err(e); // cancel always propagates
                            }
                            if fail_fast {
                                return Err(e);
                            }
                            fire_on_error(hooks, self_ref, shared, path, &e).await?;
                            Ok(Value::Null) // allSettled-style: failed item → null slot
                        }
                    }
                },
                Some(signal),
            )
            .await?;
            Ok(Value::Array(
                results
                    .into_iter()
                    .map(|r| r.unwrap_or(Value::Null))
                    .collect(),
            ))
        } else {
            retrying(
                self_ref,
                core.max_retries(),
                core.wait_ms(),
                || self.exec_once(prep_res),
                |e| self.exec_fallback(prep_res, e),
                hooks,
                shared,
                path,
            )
            .await
        }
    }

    /// Full lifecycle with hooks + timeout + cancellation + error policy.
    ///
    /// Cancellation ([`FlowError::Aborted`]) is terminal: it bypasses
    /// `on_error` and always propagates. Any other failure consults
    /// `on_error`, which may route to a recovery edge (skipping `post()`).
    async fn run_lifecycle(
        &self,
        self_ref: &NodeRef,
        shared: &mut Shared,
        hooks: &[HookRef],
        path: &[String],
        signal: &CancellationToken,
    ) -> Result<Action> {
        let start = std::time::Instant::now();
        self.core().set_signal(signal.clone());
        let mut action: Action = None;
        let mut thrown: Option<FlowError> = None;
        let mut skipped = false;

        let shared2 = &mut *shared;
        let outcome: Result<Action> = async {
            let gate = fire_on_node_start(hooks, self_ref, shared2, path).await?;
            if let Some(g) = gate {
                // HITL / approval gate: skip this node's lifecycle and route via `g`.
                skipped = true;
                Ok(Some(g))
            } else {
                // Run prep→exec→post, raced against the node's timeout and the
                // cancellation token. Losing the race drops the work future, so
                // a late `post()` can never run with stale data.
                race_work(
                    async {
                        let p = self.prep(&mut *shared2).await?;
                        let e = self
                            .exec_with_retry(self_ref, &p, hooks, shared2, path, signal)
                            .await?;
                        self.post(shared2, &p, &e).await
                    },
                    self.core().timeout_ms(),
                    signal,
                )
                .await
            }
        }
        .await;

        match outcome {
            Ok(a) => action = a,
            Err(e) => {
                if e.is_aborted() {
                    // Cancellation is terminal — never routed via onError.
                    thrown = Some(e);
                } else {
                    match fire_on_error(hooks, self_ref, shared, path, &e).await {
                        Ok(Some(ErrorPolicy::Route(a))) => action = Some(a), // route; post() skipped
                        Ok(_) => thrown = Some(e),
                        Err(he) => thrown = Some(he), // a throwing onError replaces the error
                    }
                }
            }
        }

        // finally: on_node_end always fires (a throwing onNodeEnd replaces the error).
        fire_on_node_end(
            hooks,
            self_ref,
            shared,
            path,
            &action,
            start.elapsed().as_millis() as u64,
            skipped,
            thrown.as_ref(),
        )
        .await?;

        if let Some(e) = thrown {
            return Err(e);
        }
        Ok(action)
    }
}

/// Fluent wiring builder returned by [`FlowNode::on`].
pub struct OnBuilder<'a> {
    core: &'a NodeCore,
    action: &'a str,
}

impl OnBuilder<'_> {
    /// Wire `node` as the successor for the action. Returns the successor.
    pub fn to(self, node: NodeRef) -> NodeRef {
        self.core.set_successor(self.action, node.clone());
        node
    }
}

/// Convenience runner for a bare node: runs this node in isolation
/// (successors are ignored — use a [`crate::Flow`] to traverse them).
#[async_trait]
pub trait NodeRunExt {
    /// Run this node in isolation. Pass `Some(token)` to make it cancellable.
    async fn run(&self, shared: &mut Shared, signal: Option<CancellationToken>) -> Result<Action>;
}

#[async_trait]
impl NodeRunExt for NodeRef {
    async fn run(&self, shared: &mut Shared, signal: Option<CancellationToken>) -> Result<Action> {
        if self.core().has_successors() {
            log::warn!(
                "[focket-rs] {} has successors; run() ignores them. Use a Flow.",
                self.name()
            );
        }
        let sig = signal.unwrap_or_default();
        let path = vec![self.name().to_string()];
        self.run_lifecycle(self, shared, &[], &path, &sig).await
    }
}

/* --------------------------- graph cloning ---------------------------- */

pub(crate) fn clone_successors(memo: &mut CloneMemo, target_core: &NodeCore) {
    for (act, succ) in target_core.successor_entries() {
        let c = succ.clone_node(memo);
        target_core.replace_successor(&act, c);
    }
}

/// Deep-clone a node and its successor subgraph (cycle-safe via the memo).
/// This is what the [`crate::impl_node_core`] macro wires up for user nodes.
pub fn clone_graph<T>(node: &T, memo: &mut CloneMemo) -> NodeRef
where
    T: FlowNode + Clone + 'static,
{
    if let Some(n) = memo.get(&node.core().id()) {
        return n.clone();
    }
    let copy = node.clone();
    let arc: NodeRef = Arc::new(copy);
    memo.insert(node.core().id(), arc.clone());
    clone_successors(memo, arc.core());
    arc
}

/// Generates the [`FlowNode::core`] and [`FlowNode::clone_node`] methods for
/// a node struct embedding a [`NodeCore`]. Requires the struct to be `Clone`.
///
/// Generates the [`FlowNode::core`] and [`FlowNode::clone_node`] methods for
/// a node struct embedding a [`NodeCore`]. Requires the struct to be `Clone`.
///
/// ```
/// use async_trait::async_trait;
/// use focket_rs::{FlowNode, NodeCore, Result, impl_node_core};
/// use serde_json::{json, Value};
///
/// #[derive(Clone)]
/// struct Greet { core: NodeCore }
///
/// #[async_trait]
/// impl FlowNode for Greet {
///     impl_node_core!(core);
///     async fn exec(&self, _p: &Value) -> Result<Value> { Ok(json!("hi")) }
/// }
///
/// let node = Greet { core: NodeCore::named("Greet") };
/// let mut memo = focket_rs::CloneMemo::new();
/// let cloned = node.clone_node(&mut memo);
/// assert_eq!(cloned.name(), "Greet");
/// ```
#[macro_export]
macro_rules! impl_node_core {
    ($field:ident) => {
        fn core(&self) -> &$crate::NodeCore {
            &self.$field
        }
        fn clone_node(&self, memo: &mut $crate::CloneMemo) -> $crate::NodeRef {
            $crate::clone_graph(self, memo)
        }
    };
}
