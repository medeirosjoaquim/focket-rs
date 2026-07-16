//! Core type aliases.
//!
//! The TypeScript engine is generic over `S` (shared context), `P` (params),
//! `Prep` and `Out` — but those types are erased at runtime. The Rust port
//! therefore uses one uniform dynamic representation, [`serde_json::Value`],
//! which is behaviorally identical to the TS runtime semantics (and deep
//! cloning a `Value` is the exact analogue of TS's `structuredClone`).

use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::FlowError;

/// The action a node returns from `post()` to select the next edge.
/// `None` follows the default edge (wired via `.next(node)`); `Some("name")`
/// follows the edge wired via `.on("name").to(node)`.
pub const DEFAULT_ACTION: &str = "default";

/// The shared context threaded through a whole flow. The *same* mutable value
/// is handed to every node's `prep()`/`post()`.
pub type Shared = Value;

/// Node-specific config dictionary, propagated by flows and merged under the
/// node's own params.
pub type Params = Map<String, Value>;

/// The output of a node's `post()` — selects the next edge in the graph.
pub type Action = Option<String>;

/// A node behind an `Arc`. All graph wiring is done with these.
pub type NodeRef = Arc<dyn crate::FlowNode>;

/// A hook set behind an `Arc`.
pub type HookRef = Arc<dyn crate::Hooks>;

/// Optional override for how a parallel/isolated primitive copies `shared` per unit.
pub type SharedCloner = Arc<dyn Fn(&Shared) -> Shared + Send + Sync>;

/// Memo map used by graph cloning to preserve cycles (original node id → clone).
pub type CloneMemo = HashMap<u64, NodeRef>;

/// Convert a [`Value`] into [`Params`] (objects pass through; anything else
/// becomes an empty dict, mirroring how spreading a non-object yields no keys).
pub fn to_params(v: Value) -> Params {
    match v {
        Value::Object(m) => m,
        _ => Params::new(),
    }
}

/// Merge a bundle/value on top of a params dict (`{ ...base, ...over }`).
pub fn merge_params(base: &Params, over: &Value) -> Params {
    let mut m = base.clone();
    if let Value::Object(o) = over {
        m.extend(o.clone());
    }
    m
}

/// Result alias for engine fallible operations.
pub type Result<T> = std::result::Result<T, FlowError>;
