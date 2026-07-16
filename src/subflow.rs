//! [`Subflow`] — run a nested flow over an **isolated** context derived from
//! the parent shared, then fold selected results back via `reduce()`. The
//! sub-flow sees only what `derive_shared()` returns and cannot mutate the
//! parent's shared directly — this is the "subagent with isolated context"
//! primitive.

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::core::{NodeCore, NodeOpts};
use crate::error::FlowError;
use crate::impl_node_core;
use crate::internal::{isolated, retrying};
use crate::node::FlowNode;
use crate::types::*;

/// Overrides for a [`Subflow`].
#[async_trait]
pub trait SubflowOps: Send + Sync + Clone {
    /// Build the isolated sub-context.
    async fn derive_shared(
        &self,
        shared: &Shared,
        clone_shared: Option<&SharedCloner>,
    ) -> Result<Shared>;

    /// Fold the populated sub-context back into the parent shared. Default: no-op.
    async fn reduce(&self, _sub_shared: &Shared, _shared: &mut Shared) -> Result<()> {
        Ok(())
    }

    /// Called when the sub-run fails on the final attempt. Default: rethrow.
    async fn exec_fallback(&self, _sub_shared: &Value, err: FlowError) -> Result<Value> {
        Err(err)
    }
}

#[async_trait]
impl SubflowOps for () {
    /// Default: clone the parent shared.
    async fn derive_shared(
        &self,
        shared: &Shared,
        clone_shared: Option<&SharedCloner>,
    ) -> Result<Shared> {
        Ok(isolated(shared, clone_shared))
    }
}

/// Run a nested flow (usually a [`crate::Flow`]) over an isolated context,
/// then fold results back. The nested flow inherits the parent's hooks
/// (tracing continuity) and cancellation token, and is retried per the
/// Subflow's `max_retries`/`wait_ms`.
///
/// Note: on a retry the sub-run starts from a **fresh** derivation of the
/// sub-context (in TS, mutations from a failed attempt persist into the next
/// attempt — an implementation quirk we intentionally do not replicate).
#[derive(Clone)]
pub struct Subflow<B: SubflowOps> {
    core: NodeCore,
    sub: NodeRef,
    clone_shared: Option<SharedCloner>,
    ops: B,
}

impl Subflow<()> {
    /// A subflow with the default ops (derive = clone parent; reduce = no-op).
    pub fn new(sub: NodeRef) -> Self {
        Self::with_ops(sub, ())
    }
}

impl<B: SubflowOps> Subflow<B> {
    pub fn with_ops(sub: NodeRef, ops: B) -> Self {
        Subflow {
            core: NodeCore::named("Subflow"),
            sub,
            clone_shared: None,
            ops,
        }
    }

    /// How the isolated sub-context is derived when `derive_shared()` isn't
    /// overridden. Default: clone the parent shared.
    pub fn with_clone_shared(mut self, cloner: SharedCloner) -> Self {
        self.clone_shared = Some(cloner);
        self
    }

    pub fn with_node_opts(mut self, opts: NodeOpts) -> Self {
        self.core.apply_opts(opts);
        self
    }

    pub fn sub(&self) -> &NodeRef {
        &self.sub
    }
}

#[async_trait]
impl<B: SubflowOps + 'static> FlowNode for Subflow<B> {
    impl_node_core!(core);

    async fn prep(&self, shared: &mut Shared) -> Result<Value> {
        self.ops
            .derive_shared(shared, self.clone_shared.as_ref())
            .await
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
        retrying(
            self_ref,
            self.core.max_retries(),
            self.core.wait_ms(),
            || async {
                let mut sub_shared = prep_res.clone();
                let mut p = path.to_vec();
                p.push(self.sub.name().to_string());
                self.sub
                    .run_lifecycle(&self.sub, &mut sub_shared, hooks, &p, signal)
                    .await?;
                Ok(sub_shared)
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
        self.ops.reduce(exec_res, shared).await?;
        Ok(None)
    }
}
