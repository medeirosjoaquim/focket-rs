//! [`ForkJoin`] — fan out to N **distinct** branch nodes (typically
//! [`crate::Flow`] / [`crate::Subflow`] sub-agents), run them concurrently
//! (bounded by `concurrency`), each over its own isolated copy of the shared
//! context, then `join()` their resulting contexts back into the parent. This
//! is the primitive for "delegate to parallel sub-agents, then synthesize".

use async_trait::async_trait;
use serde_json::Value;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::core::{NodeCore, NodeOpts};
use crate::error::FlowError;
use crate::hooks::*;
use crate::impl_node_core;
use crate::internal::{isolated, retrying, run_parallel};
use crate::node::FlowNode;
use crate::types::*;

/// Overrides for a [`ForkJoin`].
#[async_trait]
pub trait ForkJoinOps: Send + Sync + Clone {
    /// Build the branch nodes to run for THIS `shared`. Default: the
    /// constructor branches. Override to build **dynamic** branches from
    /// `shared`. Each branch must be a distinct instance — concurrent
    /// lifecycles on one instance race on its `params`.
    fn make_branches(&self, _shared: &Shared, default: &[NodeRef]) -> Vec<NodeRef> {
        default.to_vec()
    }

    /// Merge the populated branch contexts back into the parent shared.
    /// Receives one entry per branch (in order); `None` where a branch failed
    /// under `fail_fast: false`. Default: no-op.
    async fn join(&self, _shared: &mut Shared, _branch_shareds: &[Option<Shared>]) -> Result<()> {
        Ok(())
    }

    /// Called when the whole fan-out fails on the final attempt. Default: rethrow.
    async fn exec_fallback(&self, _prep_res: &Value, err: FlowError) -> Result<Value> {
        Err(err)
    }
}

#[async_trait]
impl ForkJoinOps for () {}

/// Fan out to N distinct branch nodes in parallel (isolated shareds), then join.
///
/// Each branch runs its own full lifecycle (hooks fire per branch, HITL
/// `on_node_start` gates work per branch, retry applies) over a private shared
/// clone, so branches neither observe nor clobber one another.
pub struct ForkJoin<B: ForkJoinOps = ()> {
    core: NodeCore,
    branches: Vec<NodeRef>,
    resolved: Mutex<Vec<NodeRef>>,
    concurrency: usize,
    fail_fast: bool,
    clone_shared: Option<SharedCloner>,
    ops: B,
}

impl ForkJoin<()> {
    pub fn new(branches: Vec<NodeRef>) -> Self {
        Self::with_ops(branches, ())
    }
}

impl<B: ForkJoinOps> ForkJoin<B> {
    pub fn with_ops(branches: Vec<NodeRef>, ops: B) -> Self {
        ForkJoin {
            core: NodeCore::named("ForkJoin"),
            branches,
            resolved: Mutex::new(vec![]),
            concurrency: 0,
            fail_fast: false,
            clone_shared: None,
            ops,
        }
    }

    /// `0` (default) = run ALL resolved branches in flight (clamped to branch
    /// count); `>0` = bound parallelism.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// `true`: a failing branch cancels the rest. Default `false`: a failed
    /// branch yields a `None` slot (and fires `on_error`) while siblings finish.
    pub fn with_fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    /// How to copy the parent shared per branch. Default: deep clone.
    pub fn with_clone_shared(mut self, cloner: SharedCloner) -> Self {
        self.clone_shared = Some(cloner);
        self
    }

    pub fn with_node_opts(mut self, opts: NodeOpts) -> Self {
        self.core.apply_opts(opts);
        self
    }

    pub fn branches(&self) -> &[NodeRef] {
        &self.branches
    }
}

impl<B: ForkJoinOps> Clone for ForkJoin<B> {
    fn clone(&self) -> Self {
        ForkJoin {
            core: self.core.clone(),
            branches: self.branches.clone(),
            resolved: Mutex::new(self.resolved.lock().unwrap().clone()),
            concurrency: self.concurrency,
            fail_fast: self.fail_fast,
            clone_shared: self.clone_shared.clone(),
            ops: self.ops.clone(),
        }
    }
}

#[async_trait]
impl<B: ForkJoinOps + 'static> FlowNode for ForkJoin<B> {
    impl_node_core!(core);

    async fn prep(&self, shared: &mut Shared) -> Result<Value> {
        let b = self.ops.make_branches(shared, &self.branches);
        *self.resolved.lock().unwrap() = b;
        Ok(shared.clone())
    }

    async fn exec_with_retry(
        &self,
        self_ref: &NodeRef,
        prep_res: &Value,
        hooks: &[HookRef],
        shared: &Shared,
        path: &[String],
        signal: &CancellationToken,
    ) -> Result<Value> {
        let branches = self.resolved.lock().unwrap().clone();
        let limit = if self.concurrency > 0 {
            self.concurrency
        } else {
            branches.len().max(1)
        };
        let fail_fast = self.fail_fast;
        let cloner = self.clone_shared.clone();

        retrying(
            self_ref,
            self.core.max_retries(),
            self.core.wait_ms(),
            || async {
                let out = run_parallel(
                    branches.clone(),
                    limit,
                    |branch: NodeRef, _i, sig| {
                        let mut branch_shared = isolated(prep_res, cloner.as_ref());
                        async move {
                            let mut p = path.to_vec();
                            p.push(branch.name().to_string());
                            match branch
                                .run_lifecycle(&branch, &mut branch_shared, hooks, &p, &sig)
                                .await
                            {
                                Ok(_) => Ok(Some(branch_shared)),
                                Err(e) => {
                                    if e.is_aborted() {
                                        return Err(e); // cancel always propagates
                                    }
                                    if fail_fast {
                                        return Err(e);
                                    }
                                    fire_on_error(hooks, &branch, &branch_shared, path, &e).await?;
                                    Ok(None)
                                }
                            }
                        }
                    },
                    Some(signal),
                )
                .await?;
                Ok(Value::Array(
                    out.into_iter()
                        .map(|slot| slot.flatten().unwrap_or(Value::Null))
                        .collect(),
                ))
            },
            |e| self.ops.exec_fallback(prep_res, e),
            hooks,
            shared,
            path,
        )
        .await
    }

    async fn post(
        &self,
        shared: &mut Shared,
        _prep_res: &Value,
        exec_res: &Value,
    ) -> Result<Action> {
        let branch_shareds: Vec<Option<Shared>> = match exec_res {
            Value::Array(items) => items
                .iter()
                .map(|v| if v.is_null() { None } else { Some(v.clone()) })
                .collect(),
            _ => vec![],
        };
        self.ops.join(shared, &branch_shareds).await?;
        Ok(None)
    }
}
