//! Cross-cutting hooks for tracing, logging, metrics, and error recovery.
//!
//! All hooks are optional. Register one or more sets on a flow via
//! `flow.use_hooks(...)`. Multiple sets are composed: side-effecting hooks
//! (`on_start`/`on_node_end`/`on_retry`/`on_end`) fan out in registration
//! order; routing hooks are first-wins (`on_node_start`'s gate, `on_error`'s
//! route — and `on_error` short-circuits after the first `Some`).
//!
//! Note: unlike TS (where hooks receive the mutable `shared`), Rust hooks
//! receive `&Shared` (read-only) so they can fire from parallel contexts.

use async_trait::async_trait;

use crate::error::FlowError;
use crate::types::*;

/// Context passed to per-node hooks.
pub struct NodeHookCtx<'a> {
    /// The node that is/about to run.
    pub node: &'a NodeRef,
    /// The shared context (read-only).
    pub shared: &'a Shared,
    /// Ordered list of node names visited so far in this run (path tracing).
    pub path: &'a [String],
}

/// Context for [`Hooks::on_node_end`].
pub struct NodeEndCtx<'a> {
    pub node: &'a NodeRef,
    pub shared: &'a Shared,
    pub path: &'a [String],
    /// The action the node produced (or the gate/routed action).
    pub action: &'a Action,
    /// Wall-clock duration of the node lifecycle.
    pub duration_ms: u64,
    /// True when an `on_node_start` gate skipped this node's lifecycle.
    pub skipped: bool,
    /// The error, when the node failed (after error policy was consulted).
    pub error: Option<&'a FlowError>,
}

/// Context for [`Hooks::on_retry`].
pub struct RetryCtx<'a> {
    pub node: &'a NodeRef,
    pub shared: &'a Shared,
    pub path: &'a [String],
    pub error: &'a FlowError,
    pub attempt: u32,
    pub wait_ms: u64,
}

/// Context for [`Hooks::on_error`].
pub struct ErrorCtx<'a> {
    pub node: &'a NodeRef,
    pub shared: &'a Shared,
    pub path: &'a [String],
    pub error: &'a FlowError,
}

/// What an `on_error` hook can decide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorPolicy {
    /// Route to the successor wired for this action (`post()` is skipped).
    Route(String),
    /// Propagate the error (same as returning `None`).
    Throw,
}

/// Cross-cutting hooks. Implement any subset; the defaults are no-ops.
#[async_trait]
pub trait Hooks: Send + Sync {
    /// Fired once at the start of a top-level `flow.run()`.
    async fn on_start(&self, _shared: &Shared) -> Result<()> {
        Ok(())
    }

    /// Fired before each node runs. Return `Some(action)` to **gate** the
    /// node: its `prep`/`exec`/`post` are skipped entirely and the flow routes
    /// via that action's edge (the human-in-the-loop / approval seam). Return
    /// `None` to run the node normally.
    async fn on_node_start(&self, _ctx: NodeHookCtx<'_>) -> Result<Action> {
        Ok(None)
    }

    /// Fired after each node runs (always, including on error / skip).
    async fn on_node_end(&self, _ctx: NodeEndCtx<'_>) -> Result<()> {
        Ok(())
    }

    /// Fired before each retry of `exec()`.
    async fn on_retry(&self, _ctx: RetryCtx<'_>) -> Result<()> {
        Ok(())
    }

    /// Fired when a node's `exec()` ultimately fails (after retries +
    /// `exec_fallback`), including a [`FlowError::Timeout`]. Return
    /// `Some(ErrorPolicy::Route(action))` to route to that successor (skipping
    /// `post()`), or `Some(ErrorPolicy::Throw)` / `None` to propagate.
    /// NOTE: [`FlowError::Aborted`] (cancellation) bypasses this entirely — it
    /// always propagates and is never routed.
    async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
        Ok(None)
    }

    /// Fired once at the end of a top-level `flow.run()`.
    async fn on_end(&self, _shared: &Shared, _action: &Action) -> Result<()> {
        Ok(())
    }
}

/* ------- composition (the analogue of TS `composeHooks`) ------- */

/// `on_start`: fan out in order.
pub async fn fire_on_start(hooks: &[HookRef], shared: &Shared) -> Result<()> {
    for h in hooks {
        h.on_start(shared).await?;
    }
    Ok(())
}

/// `on_node_start`: all hooks run (side effects); the first non-`None` gate wins.
pub async fn fire_on_node_start(
    hooks: &[HookRef],
    node: &NodeRef,
    shared: &Shared,
    path: &[String],
) -> Result<Action> {
    let mut gate: Action = None;
    for h in hooks {
        let r = h.on_node_start(NodeHookCtx { node, shared, path }).await?;
        if r.is_some() && gate.is_none() {
            gate = r;
        }
    }
    Ok(gate)
}

/// `on_node_end`: fan out in order.
#[allow(clippy::too_many_arguments)]
pub async fn fire_on_node_end(
    hooks: &[HookRef],
    node: &NodeRef,
    shared: &Shared,
    path: &[String],
    action: &Action,
    duration_ms: u64,
    skipped: bool,
    error: Option<&FlowError>,
) -> Result<()> {
    for h in hooks {
        h.on_node_end(NodeEndCtx {
            node,
            shared,
            path,
            action,
            duration_ms,
            skipped,
            error,
        })
        .await?;
    }
    Ok(())
}

/// `on_retry`: fan out in order.
pub async fn fire_on_retry(
    hooks: &[HookRef],
    node: &NodeRef,
    shared: &Shared,
    path: &[String],
    error: &FlowError,
    attempt: u32,
    wait_ms: u64,
) -> Result<()> {
    for h in hooks {
        h.on_retry(RetryCtx {
            node,
            shared,
            path,
            error,
            attempt,
            wait_ms,
        })
        .await?;
    }
    Ok(())
}

/// `on_error`: first `Some` wins **and short-circuits** (later hooks do not run).
pub async fn fire_on_error(
    hooks: &[HookRef],
    node: &NodeRef,
    shared: &Shared,
    path: &[String],
    error: &FlowError,
) -> Result<Option<ErrorPolicy>> {
    for h in hooks {
        if let Some(r) = h
            .on_error(ErrorCtx {
                node,
                shared,
                path,
                error,
            })
            .await?
        {
            return Ok(Some(r));
        }
    }
    Ok(None)
}

/// `on_end`: fan out in order.
pub async fn fire_on_end(hooks: &[HookRef], shared: &Shared, action: &Action) -> Result<()> {
    for h in hooks {
        h.on_end(shared, action).await?;
    }
    Ok(())
}
