# focket-rs

A minimal node-graph / workflow orchestration engine for Rust — a port of the
TypeScript [focketplow](https://github.com/medeirosjoaquim/focketplow) library
(itself inspired by [PocketFlow](https://github.com/The-Pocket/PocketFlow/)).

One durable primitive — a `prep → exec → post` **node** wired into a directed
graph — and the engine gets out of your way. It is async-first (tokio), ships a
hooks layer for tracing and error recovery, and has built-in cycle protection,
retries, timeouts, cancellation, and bounded parallelism.

> **What this is — and isn't.** focket-rs is the *graph engine*. It deliberately
> does **not** ship an LLM client, tool-calling, streaming, or memory. You bring
> those and compose them inside nodes.

## The 30-second tour

```rust
use async_trait::async_trait;
use focket_rs::*;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone)]
struct Greet { core: NodeCore }

#[async_trait]
impl FlowNode for Greet {
    impl_node_core!(core); // generates core() + clone_node()

    async fn prep(&self, shared: &mut Shared) -> Result<Value> {
        Ok(shared["name"].clone())            // `shared` is the value passed to run()
    }
    async fn exec(&self, name: &Value) -> Result<Value> {
        Ok(json!(format!("hello, {}", name.as_str().unwrap())))
    }
    async fn post(&self, shared: &mut Shared, _p: &Value, out: &Value) -> Result<Action> {
        shared["greeting"] = out.clone();
        Ok(if self.param("loud") == Some(json!(true)) {
            Some("shout".to_string())         // follow the "shout" edge
        } else {
            None                              // follow the default edge
        })
    }
}

# async fn demo() -> Result<()> {
let greet: NodeRef = Arc::new(Greet { core: NodeCore::named("Greet") });
let done: NodeRef = Arc::new(Greet { core: NodeCore::named("Done") });

greet.next(done.clone());           // default edge
greet.on("shout").to(done.clone()); // conditional edge

let flow = Flow::new(greet);
flow.set_params(json!({"loud": false}));
// The value you pass to run() IS `shared` — that same mutable value is handed
// to every node's prep()/post(), so nodes communicate by reading/writing it.
let mut shared = json!({"name": "world"});
flow.run(&mut shared, None).await?;
# Ok(())
# }
```

## Core concepts

### Nodes & lifecycle

Every node has three overridable phases. The engine always `.await`s them, so
bodies may do any amount of async work — no separate async base classes.

| Phase | Receives | Returns | Purpose |
|-------|----------|---------|---------|
| `prep(shared)`   | `&mut Shared` | `Value` for `exec` | Read/setup |
| `exec(prep_res)` | result of `prep` | `Value` for `post` | Do the work (retried) |
| `post(shared, prep_res, exec_res)` | all of the above | `Action` | Write back & choose the next edge |

### Actions & routing

`post()` returns an **action** (`Option<String>`):
- `None` → follow the **default** edge (`.next(node)`).
- `Some(name)` → follow the edge registered with `.on(name).to(node)`.

Clean termination (no matching edge) just ends the flow. A *named* action with
no matching edge emits a soft warning (via `log::warn!`) — typo detection
without false alarms.

### Shared context & params

- **`shared`** is the `serde_json::Value` you pass to `flow.run(&mut shared, _)`.
  That **exact same** mutable value is handed to every node's `prep()`/`post()`.
  (It is *not* immutable — design it accordingly.)
- **`params`** is node config. A `Flow` propagates its own params to every node
  as **defaults**; a node's *own* params (set via `node.set_params(...)`) are
  merged **on top** and are never clobbered. `BatchFlow` bundles layer in below
  a node's own params too.

## The node kinds

| TS class | Rust | Role |
|----------|------|------|
| `BaseNode` / `Node` | any struct implementing [`FlowNode`] + [`NodeCore`] | Lifecycle + graph wiring + retry (`max_retries`, `wait_ms`, `exec_fallback`). |
| `BatchNode` | a `FlowNode` with [`NodeOpts::batch()`] | Processes `prep()`'s array item-by-item via `exec_item()`, with `concurrency` + `fail_fast`. |
| `Flow` | [`Flow`] / `Flow<B: FlowOps>` | Orchestrates a graph: hooks, cycle guard, nestable as a node. |
| `BatchFlow` | [`BatchFlow`] / `BatchFlow<B: BatchFlowOps>` | Runs a sub-flow once per bundle (`concurrency` + `fail_fast` + per-bundle isolation). |
| `Subflow` | [`Subflow<B: SubflowOps>`] | Runs a nested flow over an **isolated** context, then folds results back via `reduce()`. |
| `ForkJoin` | [`ForkJoin<B: ForkJoinOps>`] | Fans out to N **distinct** branch nodes in parallel (isolated shareds), then `join()`s. |

(A TS `BaseNode` is observably identical to a `Node` with `maxRetries: 0` and
the default rethrowing `execFallback`, so the Rust port collapses the three
node classes into one `FlowNode` trait configured by `NodeOpts`.)

## Retries (fixed semantics)

`max_retries` is the number of retries **after** the first attempt — so total
attempts are `1 + max_retries`, and the default `0` means "run once, don't
retry." On final failure, `exec_fallback(prep_res, error)` is called (default:
rethrow).

```rust
#[async_trait]
impl FlowNode for Flaky {
    impl_node_core!(core);
    async fn exec(&self, req: &Value) -> Result<Value> { /* may fail; retried */ }
    async fn exec_fallback(&self, req: &Value, err: FlowError) -> Result<Value> { /* last-ditch */ }
}
// core: NodeCore::with_opts(NodeOpts::new().max_retries(3).wait_ms(1000))
```

## Hooks — tracing, metrics & error recovery

Register one or more hook sets on a flow. They're your cross-cutting layer for
logging, spans, and turning failures into routes.

```rust
struct MyHooks;
#[async_trait]
impl Hooks for MyHooks {
    async fn on_start(&self, _shared: &Shared) -> Result<()> { Ok(()) }
    async fn on_node_start(&self, ctx: NodeHookCtx<'_>) -> Result<Action> { Ok(None) }
    async fn on_node_end(&self, ctx: NodeEndCtx<'_>) -> Result<()> { Ok(()) }
    async fn on_retry(&self, ctx: RetryCtx<'_>) -> Result<()> { Ok(()) }
    async fn on_error(&self, ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
        // Route to an error node (post() is skipped), or Throw/None to propagate.
        Ok(Some(ErrorPolicy::Route("error_path".to_string())))
    }
    async fn on_end(&self, _shared: &Shared, _action: &Action) -> Result<()> { Ok(()) }
}
flow.use_hooks(Arc::new(MyHooks));
```

Because `on_error` can return a route, a failing `exec` no longer kills the
whole flow — you can route to a recovery node, exactly like any other edge.

### Human-in-the-loop gates

`on_node_start` can **return an action** to *gate* a node: its
`prep`/`exec`/`post` are skipped and the flow routes via that action's edge.
A gated node still fires `on_node_end` (with `skipped: true`) for tracing.

## Parallelism: concurrency + fail_fast

Batch nodes and `BatchFlow` take `concurrency` / `fail_fast`:

- `concurrency: 1` (default) = sequential, order preserved.
- `concurrency: > 1` = bounded parallel, **still** order-preserved.
- `fail_fast: false` (default) = all-settled style: failed items become `null`
  and fire `on_error`; the rest finish. `fail_fast: true` cancels in-flight
  siblings on the first failure (via the shared cancellation token) and throws.

> **BatchFlow isolation.** In `BatchFlow`, `concurrency > 1` runs each bundle
> over its own cloned shared **and** a freshly cloned node graph, so concurrent
> bundles neither stomp each other's shared nor race on node params. Fold
> results back by overriding `merge(parent, bundle_shareds)`.
> (`concurrency == 1` keeps the classic behaviour — bundles accumulate into the
> single real `shared`.)

## Timeouts

`max_steps` bounds *step count*, not wall-clock. For a node that can hang, give
it `timeout_ms`. The whole `prep → exec → post` lifecycle is raced against it;
on expiry a `FlowError::Timeout` goes through `on_error` like any failure, so
you can **route to a recovery node**.

## Cancellation

Pass a `CancellationToken` to make a run cancellable. Cancelling fails the run
with `FlowError::Aborted`, propagated from wherever execution was:

```rust
let tok = CancellationToken::new();
let t2 = tok.clone();
tokio::spawn(async move { sleep(Duration::from_secs(5)).await; t2.cancel(); });
let result = flow.run(&mut shared, Some(tok)).await; // Err(FlowError::Aborted)
```

For **cooperative** cancellation, read the node's `self.signal()` (or the
`signal` passed to `exec_item`) and `select!` on it in your I/O.

Error kinds: `FlowError::Cycle` (`max_steps`), `FlowError::Timeout`
(`timeout_ms` — routable via `on_error`), and `FlowError::Aborted`
(cancellation — **terminal**, never routed; never retried).

## Nesting

A `Flow` is itself a node (`FlowNode`), so you can drop one inside another.
The nested flow's hooks run before the parent's, and each flow has its own
`max_steps` budget.

## TS → Rust mapping

| TypeScript | Rust |
|------------|------|
| `shared: S` (generic) | `Shared = serde_json::Value` |
| `params: Dict` | `Params = serde_json::Map<String, Value>` |
| `Prep` / `Out` generics | `serde_json::Value` |
| `Action = string \| undefined` | `Action = Option<String>` |
| `AbortController` / `AbortSignal` | `tokio_util::sync::CancellationToken` |
| `structuredClone(shared)` | `shared.clone()` (deep) |
| `console.warn(...)` | `log::warn!(...)` |
| `class X extends Node` | `struct X { core: NodeCore }` + `impl FlowNode` + `impl_node_core!(core)` |
| `class X extends Flow/BatchFlow/Subflow/ForkJoin` | `Flow<B>` / `BatchFlow<B>` / `Subflow<B>` / `ForkJoin<B>` with an ops trait |
| `flow.use({...})` | `flow.use_hooks(Arc::new(MyHooks))` |
| `new Flow(start, { maxSteps: 50 })` | `Flow::new(start).with_max_steps(50)` |
| `node.run(shared, { signal })` | `node_ref.run(&mut shared, Some(token))` ([`NodeRunExt`]) |
| `new Promise(r => setTimeout(r, ms))` | `tokio::time::sleep(Duration::from_millis(ms))` |

## Intentional divergences from the TS version

1. **Types are dynamic.** TS generics (`S`, `P`, `Prep`, `Out`) are erased at
   runtime anyway, so the Rust port uses `serde_json::Value` uniformly — the
   runtime behavior is identical.
2. **Losing a timeout/abort race cancels in-flight work.** In JS, a raced
   promise keeps running in the background (only its result is abandoned). In
   Rust the losing future is **dropped** (cancelled). The guarantee that
   matters is preserved: a node that loses the race never runs `post()`.
3. **Hooks see `&Shared` (read-only)**, not the mutable context, so they can
   fire from parallel contexts. TS hooks technically can mutate `shared`.
4. **Abort reasons are not carried.** `FlowError::Aborted` has no payload
   (`CancellationToken` carries no reason).
5. **Node behaviors must be `Clone`.** Graph cloning (used by parallel
   `BatchFlow` for per-bundle isolation) is automatic in JS via prototype
   tricks; in Rust you `#[derive(Clone)]` and use the `impl_node_core!(core)`
   macro.
6. **`exec_item` does not receive `shared`** (it doesn't in TS either); write
   results back in `post()`.
7. **`Subflow` retries start from a freshly derived sub-context** per attempt
   (in TS, mutations from a failed attempt persist into the retry — an
   implementation quirk, not a contract).
8. **Rust futures are lazy.** Some interleavings that JS gets "for free" from
   the microtask queue need an explicit `yield_now().await` (e.g. a
   synchronously-failing batch item lets siblings start first in JS).

## What's intentionally not here

Streaming, tool/structured-output helpers, message memory, checkpointing/resume,
and human-in-the-loop interrupts — same as the TS version. These belong to the
layer you build *on top* of focket-rs.

## Tests

`cargo test` runs the port of focketplow's full behavioral suite
(`tests/flow.rs`, 122 tests mapped from `tests/flow.test.ts`).

## License

ISC
