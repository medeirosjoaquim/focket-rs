//! [`BatchFlow`] — a flow that runs its orchestration once per "bundle" from
//! `prep()`, merging each bundle into the params. `concurrency` controls
//! parallel bundles; `fail_fast` controls whether one bundle's failure cancels
//! the rest.
//!
//! In parallel mode (`concurrency > 1`) each bundle gets a **fresh node graph**
//! (cloned) and its **own cloned shared**, so concurrent bundles neither race
//! on node params/state nor stomp each other's shared. Fold results back by
//! overriding [`BatchFlowOps::merge`]. In sequential mode (`concurrency == 1`)
//! bundles accumulate into the single real `shared`.

use async_trait::async_trait;
use serde_json::Value;
use std::sync::{Arc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::core::{NodeCore, NodeOpts};
use crate::error::FlowError;
use crate::flow::{Budget, Flow, orchestrate};
use crate::hooks::*;
use crate::internal::{isolated, race_abort, run_parallel};
use crate::node::{FlowNode, clone_successors};
use crate::types::*;

/// Overrides for a [`BatchFlow`].
#[async_trait]
pub trait BatchFlowOps: Send + Sync + Clone {
    /// Return the bundles array (each object merges into the params of one
    /// bundle run). Default / `null`: no bundles.
    async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
        Ok(Value::Null)
    }

    /// Fold per-bundle isolated results back into the parent shared. Called
    /// **only in parallel mode** (`concurrency > 1`), where each bundle ran
    /// over its own cloned shared. Default: no-op.
    async fn merge(&self, _parent: &mut Shared, _bundle_shareds: &[Shared]) -> Result<()> {
        Ok(())
    }

    /// Runs after all bundles settle. Default: `None` (end of flow).
    async fn post(&self, _shared: &mut Shared, _bundles: &[Value]) -> Result<Action> {
        Ok(None)
    }
}

#[async_trait]
impl BatchFlowOps for () {}

/// A flow that runs its orchestration once per bundle from `prep()`.
pub struct BatchFlow<B: BatchFlowOps = ()> {
    core: NodeCore,
    start_node: RwLock<Option<NodeRef>>,
    max_steps: usize,
    hooks: RwLock<Vec<HookRef>>,
    concurrency: usize,
    fail_fast: bool,
    clone_shared: Option<SharedCloner>,
    ops: B,
}

impl BatchFlow<()> {
    pub fn new(start: NodeRef) -> Self {
        Self::with_ops(start, ())
    }

    /// A batch flow with no start node yet (set it via [`BatchFlow::start`]).
    pub fn unstarted() -> Self {
        Self::with_ops_opt(None, ())
    }
}

impl<B: BatchFlowOps> Clone for BatchFlow<B> {
    fn clone(&self) -> Self {
        BatchFlow {
            core: self.core.clone(),
            start_node: RwLock::new(self.start_node.read().unwrap().clone()),
            max_steps: self.max_steps,
            hooks: RwLock::new(self.hooks.read().unwrap().clone()),
            concurrency: self.concurrency,
            fail_fast: self.fail_fast,
            clone_shared: self.clone_shared.clone(),
            ops: self.ops.clone(),
        }
    }
}

impl<B: BatchFlowOps + 'static> BatchFlow<B> {
    pub fn with_ops(start: NodeRef, ops: B) -> Self {
        Self::with_ops_opt(Some(start), ops)
    }

    /// A batch flow with custom ops and no start node yet.
    pub fn without_start(ops: B) -> Self {
        Self::with_ops_opt(None, ops)
    }

    fn with_ops_opt(start: Option<NodeRef>, ops: B) -> Self {
        BatchFlow {
            core: NodeCore::named("BatchFlow"),
            start_node: RwLock::new(start),
            max_steps: 1000,
            hooks: RwLock::new(vec![]),
            concurrency: 1,
            fail_fast: false,
            clone_shared: None,
            ops,
        }
    }

    /// `1` (default) = sequential bundles; `>1` = bounded parallel + isolation.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// `true`: the first bundle failure throws and cancels in-flight bundles.
    pub fn with_fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    /// Custom strategy for copying `shared` per bundle (parallel mode).
    pub fn with_clone_shared(mut self, cloner: SharedCloner) -> Self {
        self.clone_shared = Some(cloner);
        self
    }

    pub fn with_max_steps(mut self, max_steps: usize) -> Self {
        self.max_steps = max_steps;
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.core.set_name(name);
        self
    }

    pub fn with_node_opts(mut self, opts: NodeOpts) -> Self {
        self.core.apply_opts(opts);
        self
    }

    pub fn start(&self, node: NodeRef) -> NodeRef {
        *self.start_node.write().unwrap() = Some(node.clone());
        node
    }

    pub fn use_hooks(&self, hooks: HookRef) -> &Self {
        self.hooks.write().unwrap().push(hooks);
        self
    }

    pub fn start_node(&self) -> Option<NodeRef> {
        self.start_node.read().unwrap().clone()
    }

    pub fn max_steps(&self) -> usize {
        self.max_steps
    }

    pub fn hooks(&self) -> Vec<HookRef> {
        self.hooks.read().unwrap().clone()
    }

    pub(crate) async fn execute(
        &self,
        self_ref: &NodeRef,
        shared: &mut Shared,
        hooks: &[HookRef],
        path: &[String],
        budget: &mut Budget,
        signal: &CancellationToken,
    ) -> Result<Action> {
        let bundles: Vec<Value> = match self.ops.prep(shared).await? {
            Value::Array(a) => a,
            _ => vec![],
        };
        let parallel = self.concurrency > 1 && !bundles.is_empty();

        if parallel {
            // Each bundle gets a FRESH node graph (cloned) and its OWN shared clone.
            let start = self.start_node().ok_or_else(|| {
                FlowError::flow(
                    "BatchFlow has no start node. Call bf.start(node) or pass it to the constructor.",
                )
            })?;
            let bundle_shareds: Arc<Mutex<Vec<Shared>>> = Arc::new(Mutex::new(Vec::new()));
            let shared_ref: &Shared = shared;
            let flow_params = self.core.params();
            let fail_fast = self.fail_fast;
            let cloner = self.clone_shared.clone();
            let max_steps = self.max_steps;
            let name = self.name().to_string();

            run_parallel(
                bundles.clone(),
                self.concurrency,
                |bundle: Value, _i, sig| {
                    let flow_params = flow_params.clone();
                    let mut bundle_shared = isolated(shared_ref, cloner.as_ref());
                    let start = start.clone();
                    let bundle_shareds = bundle_shareds.clone();
                    let name = name.clone();
                    async move {
                        let merged = merge_params(&flow_params, &bundle);
                        let r: Result<()> = async {
                            let mut memo = CloneMemo::new();
                            let graph = start.clone_node(&mut memo);
                            let runner = Flow::new(graph)
                                .with_max_steps(max_steps)
                                .with_name(format!("{name}~bundle"));
                            runner.set_params(Value::Object(merged));
                            let r: NodeRef = Arc::new(runner);
                            r.run_lifecycle(&r, &mut bundle_shared, hooks, path, &sig)
                                .await?;
                            Ok(())
                        }
                        .await;
                        match r {
                            Ok(()) => {}
                            Err(e) => {
                                if e.is_aborted() {
                                    return Err(e); // cancel always propagates
                                }
                                if fail_fast {
                                    return Err(e);
                                }
                                fire_on_error(hooks, self_ref, &bundle_shared, path, &e).await?;
                            }
                        }
                        bundle_shareds.lock().unwrap().push(bundle_shared);
                        Ok(())
                    }
                },
                Some(signal),
            )
            .await?;

            let shareds = std::mem::take(&mut *bundle_shareds.lock().unwrap());
            self.ops.merge(shared, &shareds).await?;
        } else {
            // Sequential: bundles share the real `shared` and the single node
            // graph (no concurrency → no isolation needed). Budget shared.
            for bundle in &bundles {
                if signal.is_cancelled() {
                    return Err(FlowError::Aborted);
                }
                let merged = merge_params(&self.core.params(), bundle);
                let r = race_abort(
                    orchestrate(
                        self.start_node(),
                        shared,
                        &merged,
                        budget,
                        hooks,
                        path,
                        signal,
                    ),
                    signal,
                )
                .await;
                match r {
                    Ok(_) => {}
                    Err(e) => {
                        if e.is_aborted() {
                            return Err(e); // cancel always propagates
                        }
                        if self.fail_fast {
                            return Err(e);
                        }
                        fire_on_error(hooks, self_ref, shared, path, &e).await?;
                    }
                }
            }
        }

        self.ops.post(shared, &bundles).await
    }

    /// Execute the batch flow (fires `on_start`/`on_end`).
    pub async fn run(
        self: &Arc<Self>,
        shared: &mut Shared,
        signal: Option<CancellationToken>,
    ) -> Result<Action> {
        let self_ref: NodeRef = self.clone();
        let sig = signal.unwrap_or_default();
        self.core.set_signal(sig.clone());
        let hooks = self.hooks();
        fire_on_start(&hooks, shared).await?;
        let mut action: Action = None;
        let mut err: Option<FlowError> = None;
        match self
            .execute(
                &self_ref,
                shared,
                &hooks,
                &[],
                &mut Budget::new(self.max_steps),
                &sig,
            )
            .await
        {
            Ok(a) => action = a,
            Err(e) => err = Some(e),
        }
        fire_on_end(&hooks, shared, &action).await?;
        if let Some(e) = err {
            return Err(e);
        }
        Ok(action)
    }
}

#[async_trait]
impl<B: BatchFlowOps + 'static> FlowNode for BatchFlow<B> {
    fn core(&self) -> &NodeCore {
        &self.core
    }

    fn clone_node(&self, memo: &mut CloneMemo) -> NodeRef {
        if let Some(n) = memo.get(&self.core.id()) {
            return n.clone();
        }
        let copy = self.clone();
        let arc = Arc::new(copy);
        memo.insert(self.core.id(), arc.clone());
        clone_successors(memo, arc.core());
        if let Some(sn) = self.start_node() {
            let cloned = sn.clone_node(memo);
            *arc.start_node.write().unwrap() = Some(cloned);
        }
        arc
    }

    async fn run_lifecycle(
        &self,
        self_ref: &NodeRef,
        shared: &mut Shared,
        parent_hooks: &[HookRef],
        path: &[String],
        signal: &CancellationToken,
    ) -> Result<Action> {
        self.core.set_signal(signal.clone());
        let mut hooks = self.hooks();
        hooks.extend(parent_hooks.iter().cloned());
        let mut p = path.to_vec();
        p.push(self.name().to_string());
        self.execute(
            self_ref,
            shared,
            &hooks,
            &p,
            &mut Budget::new(self.max_steps),
            signal,
        )
        .await
    }
}
