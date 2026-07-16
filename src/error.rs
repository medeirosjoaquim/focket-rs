//! Error types for the flow engine.
//!
//! Mirrors the TypeScript error hierarchy:
//! - [`FlowError::Flow`] ↔ `FlowError` (generic engine misuse)
//! - [`FlowError::Cycle`] ↔ `FlowCycleError` (`maxSteps` budget exceeded)
//! - [`FlowError::Timeout`] ↔ `FlowTimeoutError` (per-node `timeoutMs`; routable via `onError`)
//! - [`FlowError::Aborted`] ↔ `FlowAbortError` (cancellation; **terminal**, never routed/retried)
//! - [`FlowError::Msg`] / [`FlowError::User`] ↔ user-code `Error`s thrown from lifecycle bodies

use std::fmt;

/// The single error type used by the engine and returned from user lifecycle code.
#[derive(Debug)]
pub enum FlowError {
    /// Generic engine misuse (e.g. a flow with no start node).
    Flow(String),
    /// A flow exceeded its `max_steps` budget (infinite-loop / cycle guard).
    Cycle { max_steps: usize },
    /// A node's lifecycle exceeded its `timeout_ms` budget. Goes through the
    /// normal `onError` path, so a timeout can be routed to a recovery node
    /// like any failure. (Contrast [`FlowError::Aborted`], which is terminal.)
    Timeout { timeout_ms: u64 },
    /// A run was cancelled via its [`tokio_util::sync::CancellationToken`]
    /// (`run(.., Some(token))`, or `fail_fast` cancelling a sibling).
    /// Cancellation is **terminal**: it bypasses `onError` and propagates
    /// immediately — you cannot route around a cancel.
    Aborted,
    /// An error raised by user code with a plain message (`throw new Error("boom")`).
    Msg(String),
    /// A foreign error raised by user code, wrapped.
    User(Box<dyn std::error::Error + Send + Sync>),
}

impl FlowError {
    /// Generic engine error with a message.
    pub fn flow(msg: impl Into<String>) -> Self {
        FlowError::Flow(msg.into())
    }

    /// `max_steps` budget exceeded.
    pub fn cycle(max_steps: usize) -> Self {
        FlowError::Cycle { max_steps }
    }

    /// Per-node timeout expired.
    pub fn timeout(timeout_ms: u64) -> Self {
        FlowError::Timeout { timeout_ms }
    }

    /// A user-code error with a plain message.
    pub fn msg(msg: impl Into<String>) -> Self {
        FlowError::Msg(msg.into())
    }

    /// Wrap any foreign error from user code.
    pub fn user<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        FlowError::User(Box::new(e))
    }

    /// True for [`FlowError::Aborted`] (terminal cancellation).
    pub fn is_aborted(&self) -> bool {
        matches!(self, FlowError::Aborted)
    }

    /// The `timeout_ms` budget, if this is a [`FlowError::Timeout`].
    pub fn timeout_ms(&self) -> Option<u64> {
        match self {
            FlowError::Timeout { timeout_ms } => Some(*timeout_ms),
            _ => None,
        }
    }
}

impl fmt::Display for FlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlowError::Flow(m) => write!(f, "{m}"),
            FlowError::Cycle { max_steps } => write!(
                f,
                "Flow exceeded maxSteps={max_steps} — likely an infinite loop. \
                 Pass a higher {{ maxSteps }} if this is intended."
            ),
            FlowError::Timeout { timeout_ms } => {
                write!(f, "Node exceeded timeoutMs={timeout_ms}.")
            }
            FlowError::Aborted => write!(f, "Flow aborted."),
            FlowError::Msg(m) => write!(f, "{m}"),
            FlowError::User(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for FlowError {}

/// Convenient `Result` alias used across the engine.
pub type FlowResult<T> = Result<T, FlowError>;
