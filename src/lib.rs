//! # focket-rs
//!
//! A minimal node-graph / workflow orchestration engine for Rust — a port of
//! the TypeScript [focketplow](https://github.com/medeirosjoaquim/focketplow)
//! library (itself inspired by PocketFlow).
//!
//! One durable primitive — a `prep → exec → post` **node** wired into a
//! directed graph — plus a hooks layer for tracing and error recovery,
//! built-in cycle protection, retries, timeouts, cancellation, and bounded
//! parallelism.
//!
//! ## The 30-second tour
//!
//! ```rust,no_run
//! use async_trait::async_trait;
//! use focket_rs::*;
//! use serde_json::{json, Value};
//! use std::sync::Arc;
//!
//! #[derive(Clone)]
//! struct Greet { core: NodeCore }
//!
//! #[async_trait]
//! impl FlowNode for Greet {
//!     impl_node_core!(core);
//!     async fn prep(&self, shared: &mut Shared) -> Result<Value> {
//!         Ok(shared["name"].clone())
//!     }
//!     async fn exec(&self, name: &Value) -> Result<Value> {
//!         Ok(json!(format!("hello, {}", name.as_str().unwrap())))
//!     }
//!     async fn post(&self, shared: &mut Shared, _p: &Value, out: &Value) -> Result<Action> {
//!         shared["greeting"] = out.clone();
//!         Ok(if self.param("loud") == Some(json!(true)) {
//!             Some("shout".to_string())
//!         } else {
//!             None // follow the default edge
//!         })
//!     }
//! }
//!
//! # async fn demo() -> Result<()> {
//! let greet: NodeRef = Arc::new(Greet { core: NodeCore::named("Greet") });
//! let done: NodeRef = Arc::new(Greet { core: NodeCore::named("Done") });
//! greet.next(done.clone());              // default edge
//! greet.on("shout").to(done.clone());    // conditional edge
//!
//! let flow = Flow::new(greet);
//! flow.set_params(json!({"loud": false}));
//! let mut shared = json!({"name": "world"});
//! flow.run(&mut shared, None).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## TS → Rust mapping highlights
//!
//! - `shared` / params / `prep` / `exec` values are [`serde_json::Value`] (TS
//!   generics are erased at runtime; a `Value` deep-clone is the analogue of
//!   `structuredClone`).
//! - `AbortController`/`AbortSignal` → [`tokio_util::sync::CancellationToken`].
//! - `BaseNode`/`Node`/`BatchNode` → one [`FlowNode`] trait; retry and batch
//!   behavior are configured via [`NodeOpts`] on the embedded [`NodeCore`].
//! - `Flow`/`BatchFlow`/`Subflow`/`ForkJoin` → structs generic over small ops
//!   traits ([`FlowOps`], [`BatchFlowOps`], [`SubflowOps`], [`ForkJoinOps`]).
//! - `console.warn` → `log::warn!`.
//! - Losing a timeout/abort race **drops** the in-flight work future (Rust
//!   cancellation) instead of letting it settle in the background like a JS
//!   promise. The guarantee that matters is preserved: a node that loses the
//!   race never runs `post()`.

mod batch_flow;
mod core;
mod error;
mod flow;
mod fork_join;
mod node;
mod subflow;
mod types;

pub mod hooks;
pub mod internal;

pub use batch_flow::{BatchFlow, BatchFlowOps};
pub use core::{NodeCore, NodeOpts};
pub use error::{FlowError, FlowResult};
pub use flow::{Flow, FlowOps};
pub use fork_join::{ForkJoin, ForkJoinOps};
pub use hooks::{
    ErrorCtx, ErrorPolicy, Hooks, NodeEndCtx, NodeHookCtx, RetryCtx, fire_on_end, fire_on_error,
    fire_on_node_end, fire_on_node_start, fire_on_retry, fire_on_start,
};
pub use node::{FlowNode, NodeRunExt, OnBuilder, clone_graph};
pub use subflow::{Subflow, SubflowOps};
pub use types::{
    Action, CloneMemo, DEFAULT_ACTION, HookRef, NodeRef, Params, Result, Shared, SharedCloner,
    merge_params, to_params,
};

// Re-exported so users can drive cancellation without adding tokio-util themselves.
pub use tokio_util::sync::CancellationToken;
