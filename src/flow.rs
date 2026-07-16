//! [`Flow`] — orchestrates a graph of nodes from a start node, following the
//! [`Action`] each node returns. Supports hooks ([`Flow::use_hooks`]), a cycle
//! guard (`max_steps`), cancellation, and can itself be used as a node inside
//! another flow.

use async_trait::async_trait;
use serde_json::Value;
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::core::{NodeCore, NodeOpts};
use crate::error::FlowError;
use crate::hooks::*;
use crate::node::{FlowNode, clone_successors};
use crate::types::*;

/// Per-run step budget (cycle guard).
pub(crate) struct Budget {
    pub steps: usize,
    pub max: usize,
}

impl Budget {
    pub fn new(max: usize) -> Self {
        Budget { steps: 0, max }
    }
}

/// Optional overrides for a [`Flow`]'s own `prep`/`post` (the flow itself is a
/// node; these run around orchestration when nested).
#[async_trait]
pub trait FlowOps: Send + Sync + Clone {
    /// Runs before orchestration. Default: no-op.
    async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
        Ok(Value::Null)
    }

    /// Runs after orchestration; receives the final action. Default: echo it.
    async fn post(
        &self,
        _shared: &mut Shared,
        _prep_res: &Value,
        exec_res: &Action,
    ) -> Result<Action> {
        Ok(exec_res.clone())
    }
}

#[async_trait]
impl FlowOps for () {}

/// Core graph traversal shared by [`Flow`] and [`crate::BatchFlow`].
pub(crate) async fn orchestrate(
    start: Option<NodeRef>,
    shared: &mut Shared,
    params: &Params,
    budget: &mut Budget,
    hooks: &[HookRef],
    path: &[String],
    signal: &CancellationToken,
) -> Result<Action> {
    let start = start.ok_or_else(|| {
        FlowError::flow(
            "Flow has no start node. Call flow.start(node) or pass it to the constructor.",
        )
    })?;
    let mut curr = Some(start);
    let mut last_action: Action = None;
    while let Some(node) = curr {
        budget.steps += 1;
        if budget.steps > budget.max {
            return Err(FlowError::cycle(budget.max));
        }
        if signal.is_cancelled() {
            return Err(FlowError::Aborted);
        }
        node.core().inject_params(params);
        let mut p = path.to_vec();
        p.push(node.name().to_string());
        last_action = node.run_lifecycle(&node, shared, hooks, &p, signal).await?;
        curr = node.resolve_successor(&last_action);
    }
    Ok(last_action)
}

/// Orchestrates a graph of nodes. The default `B = ()` gives the standard
/// flow; provide a [`FlowOps`] to override the flow's own `prep`/`post`.
pub struct Flow<B: FlowOps = ()> {
    core: NodeCore,
    start_node: RwLock<Option<NodeRef>>,
    max_steps: usize,
    hooks: RwLock<Vec<HookRef>>,
    ops: B,
}

impl Flow<()> {
    /// A flow starting at `start`.
    pub fn new(start: NodeRef) -> Self {
        Self::with_ops(start, ())
    }

    /// A flow with no start node yet (set it via [`Flow::start`]).
    pub fn empty() -> Self {
        Self::with_ops_opt(None, ())
    }
}

impl<B: FlowOps> Clone for Flow<B> {
    fn clone(&self) -> Self {
        Flow {
            core: self.core.clone(),
            start_node: RwLock::new(self.start_node.read().unwrap().clone()),
            max_steps: self.max_steps,
            hooks: RwLock::new(self.hooks.read().unwrap().clone()),
            ops: self.ops.clone(),
        }
    }
}

impl<B: FlowOps> Flow<B> {
    /// A flow starting at `start`, with custom `prep`/`post` ops.
    pub fn with_ops(start: NodeRef, ops: B) -> Self {
        Self::with_ops_opt(Some(start), ops)
    }

    fn with_ops_opt(start: Option<NodeRef>, ops: B) -> Self {
        Flow {
            core: NodeCore::named("Flow"),
            start_node: RwLock::new(start),
            max_steps: 1000,
            hooks: RwLock::new(vec![]),
            ops,
        }
    }

    /// Max node visits per `run()` before [`FlowError::Cycle`] is thrown. Default: 1000.
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

    /// Set the starting node. Returns it so wiring chains naturally.
    pub fn start(&self, node: NodeRef) -> NodeRef {
        *self.start_node.write().unwrap() = Some(node.clone());
        node
    }

    /// Register a hook set. Multiple sets are composed (node hooks fan out in
    /// registration order; `on_error` is first-wins).
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

    /// prep → orchestrate → post (no start/end hooks — used by `run()` and nested flows).
    pub(crate) async fn execute(
        &self,
        shared: &mut Shared,
        hooks: &[HookRef],
        path: &[String],
        budget: &mut Budget,
        signal: &CancellationToken,
    ) -> Result<Action> {
        let p = self.ops.prep(shared).await?;
        let params = self.core.params();
        let o = orchestrate(
            self.start_node(),
            shared,
            &params,
            budget,
            hooks,
            path,
            signal,
        )
        .await?;
        self.ops.post(shared, &p, &o).await
    }

    /// Execute the flow. Fires `on_start`/`on_end` hooks around
    /// prep→orchestrate→post. Pass `Some(token)` to make the run cancellable:
    /// cancelling it fails with [`FlowError::Aborted`] (propagated from
    /// wherever execution was when it cancelled).
    pub async fn run(
        &self,
        shared: &mut Shared,
        signal: Option<CancellationToken>,
    ) -> Result<Action> {
        let sig = signal.unwrap_or_default();
        self.core.set_signal(sig.clone());
        let hooks = self.hooks();
        fire_on_start(&hooks, shared).await?;
        let mut action: Action = None;
        let mut err: Option<FlowError> = None;
        match self
            .execute(shared, &hooks, &[], &mut Budget::new(self.max_steps), &sig)
            .await
        {
            Ok(a) => action = a,
            Err(e) => err = Some(e),
        }
        // finally: on_end always fires
        fire_on_end(&hooks, shared, &action).await?;
        if let Some(e) = err {
            return Err(e);
        }
        Ok(action)
    }
}

#[async_trait]
impl<B: FlowOps + 'static> FlowNode for Flow<B> {
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

    /// When nested in a parent flow, the parent orchestrator calls this.
    /// The parent's hooks are appended after this flow's own hooks, and the
    /// parent's cancellation token is adopted so cancellation propagates into
    /// the sub-graph. (Note: like TS, a nested flow does not fire
    /// node-level hooks for itself, and `timeout_ms` does not apply to flows.)
    async fn run_lifecycle(
        &self,
        _self_ref: &NodeRef,
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
        self.execute(shared, &hooks, &p, &mut Budget::new(self.max_steps), signal)
            .await
    }
}
