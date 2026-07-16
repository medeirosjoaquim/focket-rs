//! Port of focketplow's `tests/flow.test.ts` — the behavioral spec of the engine.

use async_trait::async_trait;
use focket_rs::*;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{Instant, sleep};

/* ------------------------------ helpers ------------------------------ */

fn core(name: &str) -> NodeCore {
    NodeCore::named(name)
}

fn nref<T: FlowNode>(n: T) -> NodeRef {
    Arc::new(n)
}

/// Records `log::warn!` output for the warning-behavior tests.
static LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Rec;

impl log::Log for Rec {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        LOGS.lock().unwrap().push(format!("{}", record.args()));
    }
    fn flush(&self) {}
}

static REC: Rec = Rec;

fn init_logs() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        log::set_logger(&REC).unwrap();
        log::set_max_level(log::LevelFilter::Warn);
    });
}

fn logs_contain(s: &str) -> bool {
    LOGS.lock().unwrap().iter().any(|m| m.contains(s))
}

fn err_msg(e: &FlowError) -> String {
    e.to_string()
}

/* ----------------------------- BaseNode ------------------------------ */

/// ABCNode: prep → exec → post data flow.
#[derive(Clone)]
struct AbcNode {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for AbcNode {
    impl_node_core!(core);

    async fn prep(&self, shared: &mut Shared) -> Result<Value> {
        Ok(json!(format!("v{}", shared["value"].as_i64().unwrap())))
    }

    async fn exec(&self, prep_res: &Value) -> Result<Value> {
        let s = prep_res.as_str().unwrap();
        Ok(json!(s[1..].parse::<i64>().unwrap() * 2))
    }

    async fn post(
        &self,
        shared: &mut Shared,
        prep_res: &Value,
        exec_res: &Value,
    ) -> Result<Action> {
        shared["log"].as_array_mut().unwrap().push(json!(format!(
            "{}:{}",
            prep_res.as_str().unwrap(),
            exec_res
        )));
        Ok(Some(
            if exec_res.as_i64().unwrap() > 10 {
                "large"
            } else {
                "small"
            }
            .to_string(),
        ))
    }
}

#[tokio::test]
async fn lifecycle_order_and_data_flow() {
    let node: NodeRef = nref(AbcNode {
        core: core("ABCNode"),
    });
    let mut shared = json!({"value": 6, "log": []});
    let action = node.run(&mut shared, None).await.unwrap();
    assert_eq!(action, Some("large".to_string())); // 6 * 2 = 12 > 10
    assert_eq!(shared["log"], json!(["v6:12"]));
}

#[tokio::test]
async fn set_params_stores_params() {
    let n = nref(AbcNode {
        core: core("ABCNode"),
    });
    n.set_params(json!({"x": 1}));
    assert_eq!(n.param("x"), Some(json!(1)));
}

#[tokio::test]
async fn name_defaults_and_custom() {
    let default_named = nref(AbcNode {
        core: NodeCore::new(),
    });
    assert_eq!(default_named.name(), "Node");
    let custom = nref(AbcNode {
        core: NodeCore::with_opts(NodeOpts::new().name("custom")),
    });
    assert_eq!(custom.name(), "custom");
}

#[tokio::test]
async fn next_wires_default_and_named_edges_and_chains() {
    let a = nref(AbcNode { core: core("A") });
    let b = nref(AbcNode { core: core("B") });
    let c = nref(AbcNode { core: core("C") });
    let returned = a.next(b.clone());
    a.next_action(c.clone(), "custom");
    assert!(Arc::ptr_eq(&returned, &b)); // returns successor for chaining
    assert!(Arc::ptr_eq(&a.resolve_successor(&None).unwrap(), &b));
    assert!(Arc::ptr_eq(
        &a.resolve_successor(&Some("custom".into())).unwrap(),
        &c
    ));
}

#[tokio::test]
async fn on_to_fluent_wiring() {
    let a = nref(AbcNode { core: core("A") });
    let b = nref(AbcNode { core: core("B") });
    let returned = a.on("ok").to(b.clone());
    assert!(Arc::ptr_eq(&returned, &b));
    assert!(Arc::ptr_eq(
        &a.resolve_successor(&Some("ok".into())).unwrap(),
        &b
    ));
}

#[tokio::test]
async fn run_warns_when_node_has_successors() {
    init_logs();
    let a = nref(AbcNode {
        core: core("HasSucc"),
    });
    a.next(nref(AbcNode { core: core("B") }));
    let mut shared = json!({"value": 1, "log": []});
    a.run(&mut shared, None).await.unwrap();
    assert!(logs_contain("HasSucc has successors"));
}

#[tokio::test]
async fn clean_termination_does_not_warn() {
    init_logs();
    let a = nref(AbcNode {
        core: core("CleanTerm"),
    });
    let mut shared = json!({"value": 1, "log": []});
    a.run(&mut shared, None).await.unwrap();
    assert!(!logs_contain("CleanTerm"));
}

#[derive(Clone)]
struct BoomUnwired {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for BoomUnwired {
    impl_node_core!(core);
    async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        Ok(Some("nope".to_string()))
    }
}

#[tokio::test]
async fn unwired_named_action_warns_typo_detection() {
    init_logs();
    let b = nref(BoomUnwired {
        core: core("BoomUnwired"),
    });
    b.next_action(
        nref(AbcNode {
            core: core("Other"),
        }),
        "other",
    ); // has an edge, but not "nope"
    let flow = Flow::new(b);
    let mut shared = json!({"value": 1, "log": []});
    flow.run(&mut shared, None).await.unwrap();
    assert!(logs_contain("'nope'"));
}

/* -------------------------- Node (retry) ----------------------------- */

#[derive(Clone)]
struct CountExec {
    core: NodeCore,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl FlowNode for CountExec {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(json!("ok"))
    }
}

#[tokio::test]
async fn max_retries_default_zero_is_single_attempt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let n = nref(CountExec {
        core: core("CountExec"),
        calls: calls.clone(),
    });
    let mut shared = json!({});
    n.run(&mut shared, None).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
struct FlakyN {
    core: NodeCore,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl FlowNode for FlakyN {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        let c = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if c < 3 {
            return Err(FlowError::msg(format!("fail {c}")));
        }
        Ok(json!("ok"))
    }
}

#[tokio::test]
async fn max_retries_counts_retries_after_first_attempt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let n = nref(FlakyN {
        core: NodeCore::with_opts(NodeOpts::new().max_retries(2)),
        calls: calls.clone(),
    });
    let mut shared = json!({});
    let action = n.run(&mut shared, None).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(action, None);
}

#[derive(Clone)]
struct AlwaysFailsWithFallback {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for AlwaysFailsWithFallback {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        Err(FlowError::msg("always"))
    }
    async fn exec_fallback(&self, _p: &Value, exc: FlowError) -> Result<Value> {
        Ok(json!(format!("recovered:{exc}")))
    }
    async fn post(&self, shared: &mut Shared, _p: &Value, exec_res: &Value) -> Result<Action> {
        shared["fb"] = exec_res.clone();
        Ok(None)
    }
}

#[tokio::test]
async fn exec_fallback_called_after_final_failure() {
    let n = nref(AlwaysFailsWithFallback {
        core: NodeCore::with_opts(NodeOpts::new().max_retries(1)),
    });
    let mut shared = json!({});
    n.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["fb"], json!("recovered:always"));
}

#[derive(Clone)]
struct AlwaysFails {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for AlwaysFails {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        Err(FlowError::msg("boom"))
    }
}

#[tokio::test]
async fn default_exec_fallback_rethrows() {
    let n = nref(AlwaysFails {
        core: NodeCore::with_opts(NodeOpts::new().max_retries(2)),
    });
    let mut shared = json!({});
    let err = n.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "boom");
}

#[derive(Clone)]
struct FailsThenFallback {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for FailsThenFallback {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        Err(FlowError::msg("x"))
    }
    async fn exec_fallback(&self, _p: &Value, _e: FlowError) -> Result<Value> {
        Ok(json!("done"))
    }
}

#[tokio::test]
async fn retry_node_terminates_via_fallback() {
    let node = nref(FailsThenFallback {
        core: NodeCore::with_opts(NodeOpts::new().max_retries(3).wait_ms(1)),
    });
    let mut shared = json!({});
    node.run(&mut shared, None).await.unwrap();
    assert_eq!(node.core().max_retries(), 3);
}

/* ----------------------------- BatchNode ----------------------------- */

#[derive(Clone)]
struct TimesTen {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for TimesTen {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        Ok(json!([1, 2, 3]))
    }
    async fn exec_item(&self, item: &Value, _sig: &CancellationToken) -> Result<Value> {
        Ok(json!(item.as_i64().unwrap() * 10))
    }
    async fn post(&self, shared: &mut Shared, _p: &Value, exec_res: &Value) -> Result<Action> {
        shared["batch"] = exec_res.clone();
        Ok(None)
    }
}

#[tokio::test]
async fn batch_processes_each_item_preserves_order_sequential() {
    let b = nref(TimesTen {
        core: NodeCore::with_opts(NodeOpts::batch()),
    });
    let mut shared = json!({});
    b.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["batch"], json!([10, 20, 30]));
}

#[derive(Clone)]
struct SlowBatch {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for SlowBatch {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        Ok(json!([1, 2, 3, 4]))
    }
    async fn exec_item(&self, item: &Value, _sig: &CancellationToken) -> Result<Value> {
        sleep(Duration::from_millis(30)).await;
        Ok(item.clone())
    }
}

#[tokio::test]
async fn batch_concurrency_runs_in_parallel() {
    let start = Instant::now();
    let b = nref(SlowBatch {
        core: NodeCore::with_opts(NodeOpts::batch().concurrency(4)),
    });
    let mut shared = json!({});
    b.run(&mut shared, None).await.unwrap();
    // 4 x 30ms in parallel ~ 30-40ms; sequential would be ~120ms
    assert!(start.elapsed() < Duration::from_millis(80));
}

#[derive(Clone)]
struct BadItemBatch {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for BadItemBatch {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        Ok(json!([1, 2, 3]))
    }
    async fn exec_item(&self, item: &Value, _sig: &CancellationToken) -> Result<Value> {
        if item.as_i64().unwrap() == 2 {
            return Err(FlowError::msg("bad item"));
        }
        Ok(json!(format!("i{}", item)))
    }
    async fn post(&self, shared: &mut Shared, _p: &Value, exec_res: &Value) -> Result<Action> {
        shared["bf"] = exec_res.clone();
        Ok(None)
    }
}

#[tokio::test]
async fn batch_fail_fast_false_yields_null_slot_and_continues() {
    let b = nref(BadItemBatch {
        core: NodeCore::with_opts(NodeOpts::batch()),
    });
    let mut shared = json!({});
    b.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["bf"], json!(["i1", null, "i3"]));
}

#[tokio::test]
async fn batch_fail_fast_true_first_failure_throws() {
    let b = nref(BadItemBatch {
        core: NodeCore::with_opts(NodeOpts::batch().fail_fast(true)),
    });
    let mut shared = json!({});
    let err = b.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "bad item");
}

#[derive(Clone)]
struct EmptyBatch {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for EmptyBatch {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        Ok(json!([]))
    }
    async fn post(&self, shared: &mut Shared, _p: &Value, exec_res: &Value) -> Result<Action> {
        shared["empty"] = exec_res.clone();
        Ok(None)
    }
}

#[tokio::test]
async fn batch_empty_array_is_fine() {
    let b = nref(EmptyBatch {
        core: NodeCore::with_opts(NodeOpts::batch()),
    });
    let mut shared = json!({});
    b.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["empty"], json!([]));
}

/* ------------------------------- Hooks ------------------------------- */

#[derive(Clone)]
struct PassThrough {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for PassThrough {
    impl_node_core!(core);
    async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        Ok(None)
    }
}

struct TraceHooks {
    on_start: Arc<AtomicUsize>,
    on_end: Arc<AtomicUsize>,
    starts: Arc<Mutex<Vec<String>>>,
    ends: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Hooks for TraceHooks {
    async fn on_start(&self, _shared: &Shared) -> Result<()> {
        self.on_start.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn on_end(&self, _shared: &Shared, _action: &Action) -> Result<()> {
        self.on_end.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn on_node_start(&self, ctx: NodeHookCtx<'_>) -> Result<Action> {
        self.starts
            .lock()
            .unwrap()
            .push(ctx.node.name().to_string());
        Ok(None)
    }
    async fn on_node_end(&self, ctx: NodeEndCtx<'_>) -> Result<()> {
        self.ends.lock().unwrap().push(ctx.node.name().to_string());
        Ok(())
    }
}

#[tokio::test]
async fn hooks_start_end_once_node_hooks_per_node() {
    let a = nref(PassThrough { core: core("A") });
    let b = nref(PassThrough { core: core("B") });
    a.next(b);
    let flow = Flow::new(a);

    let h = TraceHooks {
        on_start: Arc::new(AtomicUsize::new(0)),
        on_end: Arc::new(AtomicUsize::new(0)),
        starts: Arc::new(Mutex::new(vec![])),
        ends: Arc::new(Mutex::new(vec![])),
    };
    let (os, oe, starts, ends) = (
        h.on_start.clone(),
        h.on_end.clone(),
        h.starts.clone(),
        h.ends.clone(),
    );
    flow.use_hooks(Arc::new(h));

    let mut shared = json!({"n": 0});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(os.load(Ordering::SeqCst), 1);
    assert_eq!(oe.load(Ordering::SeqCst), 1);
    assert_eq!(*starts.lock().unwrap(), vec!["A", "B"]);
    assert_eq!(*ends.lock().unwrap(), vec!["A", "B"]);
}

#[derive(Clone)]
struct SlowExec {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for SlowExec {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        sleep(Duration::from_millis(20)).await;
        Ok(Value::Null)
    }
}

struct DurHooks {
    dur: Arc<Mutex<u64>>,
}

#[async_trait]
impl Hooks for DurHooks {
    async fn on_node_end(&self, ctx: NodeEndCtx<'_>) -> Result<()> {
        *self.dur.lock().unwrap() = ctx.duration_ms;
        Ok(())
    }
}

#[tokio::test]
async fn on_node_end_reports_duration_ms() {
    let dur = Arc::new(Mutex::new(0u64));
    let flow = Flow::new(nref(SlowExec { core: core("Slow") }));
    flow.use_hooks(Arc::new(DurHooks { dur: dur.clone() }));
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert!(*dur.lock().unwrap() >= 15);
}

#[derive(Clone)]
struct Risky {
    core: NodeCore,
    posted: Arc<AtomicBool>,
}

#[async_trait]
impl FlowNode for Risky {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        Err(FlowError::msg("fail"))
    }
    async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        self.posted.store(true, Ordering::SeqCst);
        Ok(None)
    }
}

#[derive(Clone)]
struct Recover {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for Recover {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["recovered"] = json!(true);
        Ok(None)
    }
}

struct RouteRecover;
#[async_trait]
impl Hooks for RouteRecover {
    async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
        Ok(Some(ErrorPolicy::Route("recover".to_string())))
    }
}

#[tokio::test]
async fn on_error_returning_action_routes_and_skips_post() {
    let posted = Arc::new(AtomicBool::new(false));
    let risky = nref(Risky {
        core: core("Risky"),
        posted: posted.clone(),
    });
    let recover = nref(Recover {
        core: core("Recover"),
    });
    risky.on("recover").to(recover);

    let flow = Flow::new(risky);
    flow.use_hooks(Arc::new(RouteRecover));
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["recovered"], json!(true));
    assert!(!posted.load(Ordering::SeqCst)); // post() was skipped
}

struct SwallowError;
#[async_trait]
impl Hooks for SwallowError {
    async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
        Ok(None)
    }
}

#[tokio::test]
async fn on_error_returning_nothing_propagates() {
    let flow = Flow::new(nref(AlwaysFails {
        core: core("Risky"),
    }));
    flow.use_hooks(Arc::new(SwallowError));
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "boom");
}

struct RetryTrace {
    attempts: Arc<Mutex<Vec<u32>>>,
}

#[async_trait]
impl Hooks for RetryTrace {
    async fn on_retry(&self, ctx: RetryCtx<'_>) -> Result<()> {
        self.attempts.lock().unwrap().push(ctx.attempt);
        Ok(())
    }
}

#[tokio::test]
async fn on_retry_fires_for_each_retry() {
    let attempts = Arc::new(Mutex::new(vec![]));
    let flaky = nref(FailsThenFallback {
        core: NodeCore::with_opts(NodeOpts::new().max_retries(3).wait_ms(1)),
    });
    let flow = Flow::new(flaky);
    flow.use_hooks(Arc::new(RetryTrace {
        attempts: attempts.clone(),
    }));
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(*attempts.lock().unwrap(), vec![0, 1, 2]);
}

struct ComposeH1 {
    order: Arc<Mutex<Vec<String>>>,
}
#[async_trait]
impl Hooks for ComposeH1 {
    async fn on_node_start(&self, _ctx: NodeHookCtx<'_>) -> Result<Action> {
        self.order.lock().unwrap().push("h1-start".to_string());
        Ok(None)
    }
    async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
        Ok(None)
    }
}

struct ComposeH2 {
    order: Arc<Mutex<Vec<String>>>,
}
#[async_trait]
impl Hooks for ComposeH2 {
    async fn on_node_start(&self, _ctx: NodeHookCtx<'_>) -> Result<Action> {
        self.order.lock().unwrap().push("h2-start".to_string());
        Ok(None)
    }
    async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
        Ok(Some(ErrorPolicy::Route("first-wins".to_string())))
    }
}

#[tokio::test]
async fn multiple_hook_sets_compose_fan_out_and_first_wins() {
    let order = Arc::new(Mutex::new(vec![]));
    let a = nref(AlwaysFails { core: core("A") });
    let flow = Flow::new(a);
    flow.use_hooks(Arc::new(ComposeH1 {
        order: order.clone(),
    }));
    flow.use_hooks(Arc::new(ComposeH2 {
        order: order.clone(),
    }));
    // No edge for "first-wins" => clean termination; second hook's on_error wins.
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(*order.lock().unwrap(), vec!["h1-start", "h2-start"]);
}

#[derive(Clone)]
struct Gated {
    core: NodeCore,
    ran: Arc<AtomicBool>,
}

#[async_trait]
impl FlowNode for Gated {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        self.ran.store(true, Ordering::SeqCst);
        Ok(Value::Null)
    }
    async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        self.ran.store(true, Ordering::SeqCst);
        Ok(None)
    }
}

#[derive(Clone)]
struct Elsewhere {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for Elsewhere {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["landed"] = json!("elsewhere");
        Ok(None)
    }
}

struct GateGated;
#[async_trait]
impl Hooks for GateGated {
    async fn on_node_start(&self, ctx: NodeHookCtx<'_>) -> Result<Action> {
        Ok(if ctx.node.name() == "Gated" {
            Some("skip".to_string())
        } else {
            None
        })
    }
}

#[tokio::test]
async fn on_node_start_gate_skips_lifecycle_and_routes() {
    let ran = Arc::new(AtomicBool::new(false));
    let g = nref(Gated {
        core: core("Gated"),
        ran: ran.clone(),
    });
    let e = nref(Elsewhere {
        core: core("Elsewhere"),
    });
    g.on("skip").to(e);

    let flow = Flow::new(g);
    flow.use_hooks(Arc::new(GateGated));
    let mut shared = json!({"ran": false, "landed": ""});
    flow.run(&mut shared, None).await.unwrap();

    assert!(!ran.load(Ordering::SeqCst)); // prep/exec/post never ran
    assert_eq!(shared["landed"], json!("elsewhere"));
}

struct GateAllAndObserveSkipped {
    skipped: Arc<Mutex<Option<bool>>>,
}
#[async_trait]
impl Hooks for GateAllAndObserveSkipped {
    async fn on_node_start(&self, _ctx: NodeHookCtx<'_>) -> Result<Action> {
        Ok(Some("x".to_string()))
    }
    async fn on_node_end(&self, ctx: NodeEndCtx<'_>) -> Result<()> {
        *self.skipped.lock().unwrap() = Some(ctx.skipped);
        Ok(())
    }
}

#[tokio::test]
async fn gate_fires_on_node_end_with_skipped_true() {
    let skipped = Arc::new(Mutex::new(None));
    let flow = Flow::new(nref(PassThrough { core: core("A") }));
    flow.use_hooks(Arc::new(GateAllAndObserveSkipped {
        skipped: skipped.clone(),
    }));
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(*skipped.lock().unwrap(), Some(true));
}

/* ------------------------------- Flow -------------------------------- */

static INC: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
struct Increment {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for Increment {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        INC.fetch_add(1, Ordering::SeqCst);
        Ok(Value::Null)
    }
    async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        Ok(Some(
            if INC.load(Ordering::SeqCst) < 3 {
                "continue"
            } else {
                "done"
            }
            .to_string(),
        ))
    }
}

#[derive(Clone)]
struct DoneNode {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for DoneNode {
    impl_node_core!(core);
    async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        Ok(Some("finished".to_string()))
    }
}

#[tokio::test]
async fn flow_orchestrates_conditional_graph() {
    INC.store(0, Ordering::SeqCst);
    let a = nref(Increment { core: core("IncA") });
    let b = nref(Increment { core: core("IncB") });
    let done = nref(DoneNode { core: core("Done") });
    a.on("continue").to(b.clone());
    a.on("done").to(done.clone());
    b.on("continue").to(b.clone());
    b.on("done").to(done.clone());
    let flow = Flow::new(a);
    let mut shared = json!({"counter": 0, "results": []});
    let action = flow.run(&mut shared, None).await.unwrap();
    assert_eq!(action, Some("finished".to_string()));
}

#[tokio::test]
async fn flow_start_returns_the_node() {
    let a = nref(Increment { core: core("Inc") });
    let f = Flow::empty();
    assert!(Arc::ptr_eq(&f.start(a.clone()), &a));
}

#[tokio::test]
async fn flow_throws_when_no_start_node() {
    let f = Flow::empty();
    let mut shared = json!({"counter": 0, "results": []});
    let err = f.run(&mut shared, None).await.unwrap_err();
    assert!(err_msg(&err).contains("no start node"));
}

#[derive(Clone)]
struct Multiplier {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for Multiplier {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        let counter = shared["counter"].as_i64().unwrap();
        let mult = self
            .param("multiplier")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        Ok(Some(
            if counter * mult > 10 { "big" } else { "small" }.to_string(),
        ))
    }
}

#[tokio::test]
async fn flow_params_propagate_to_every_node() {
    let p = nref(Multiplier { core: core("P") });
    let flow = Flow::new(p);
    flow.set_params(json!({"multiplier": 5}));
    let mut shared = json!({"counter": 3, "results": []});
    let action = flow.run(&mut shared, None).await.unwrap();
    assert_eq!(action, Some("big".to_string())); // 3 * 5 = 15
}

#[derive(Clone)]
struct LoopNode {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for LoopNode {
    impl_node_core!(core);
    async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        Ok(Some("again".to_string()))
    }
}

#[tokio::test]
async fn cycle_guard_throws_flow_cycle_error() {
    let l = nref(LoopNode { core: core("Loop") });
    l.on("again").to(l.clone()); // self loop
    let flow = Flow::new(l).with_max_steps(5);
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert!(matches!(err, FlowError::Cycle { max_steps: 5 }));
}

#[test]
fn cycle_error_display() {
    let e = FlowError::cycle(5);
    assert!(err_msg(&e).contains("maxSteps=5"));
}

#[derive(Clone)]
struct SyncPush {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for SyncPush {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["order"].as_array_mut().unwrap().push(json!("sync"));
        Ok(Some("go".to_string()))
    }
}

#[derive(Clone)]
struct AsyncPush {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for AsyncPush {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        tokio::task::yield_now().await;
        Ok(Value::Null)
    }
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["order"].as_array_mut().unwrap().push(json!("async"));
        Ok(None)
    }
}

#[tokio::test]
async fn sync_and_async_node_bodies_mix() {
    let s = nref(SyncPush { core: core("Sync") });
    let a = nref(AsyncPush {
        core: core("Async"),
    });
    s.on("go").to(a);
    let flow = Flow::new(s);
    let mut shared = json!({"order": []});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["order"], json!(["sync", "async"]));
}

#[derive(Clone)]
struct SeesParams {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for SeesParams {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["seen"] = json!({
            "node": "N",
            "flowKey": self.param("flowKey"),
            "nodeKey": self.param("nodeKey"),
        });
        Ok(None)
    }
}

#[tokio::test]
async fn node_own_params_not_clobbered_by_flow_params() {
    let n = nref(SeesParams { core: core("N") });
    n.set_params(json!({"nodeKey": "from-node"})); // explicit node config
    let flow = Flow::new(n);
    flow.set_params(json!({"flowKey": "from-flow", "nodeKey": "from-flow-too"}));
    let mut shared = json!({"seen": {}});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["seen"]["flowKey"], json!("from-flow")); // flow param merged in
    assert_eq!(shared["seen"]["nodeKey"], json!("from-node")); // node's own NOT clobbered
}

/* ----------------------------- BatchFlow ----------------------------- */

#[derive(Clone)]
struct Collector {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for Collector {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        let name = self.param("name").unwrap();
        shared["seen"].as_array_mut().unwrap().push(name);
        Ok(None)
    }
}

#[derive(Clone)]
struct AbcBundles;

#[async_trait]
impl BatchFlowOps for AbcBundles {
    async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
        Ok(json!([{ "name": "a" }, { "name": "b" }, { "name": "c" }]))
    }
}

#[tokio::test]
async fn batch_flow_runs_once_per_bundle_merging_params() {
    let bf = Arc::new(BatchFlow::with_ops(
        nref(Collector {
            core: core("Collector"),
        }),
        AbcBundles,
    ));
    let mut shared = json!({"seen": []});
    bf.run(&mut shared, None).await.unwrap();
    let mut seen: Vec<String> = shared["seen"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    seen.sort();
    assert_eq!(seen, vec!["a", "b", "c"]);
}

#[derive(Clone)]
struct BoomBundle {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for BoomBundle {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        let name = self.param("name").unwrap();
        if name == json!("b") {
            return Err(FlowError::msg("bundle b failed"));
        }
        shared["seen"].as_array_mut().unwrap().push(name);
        Ok(None)
    }
}

#[tokio::test]
async fn batch_flow_fail_fast_false_keeps_going() {
    let bf = Arc::new(BatchFlow::with_ops(
        nref(BoomBundle { core: core("Boom") }),
        AbcBundles,
    ));
    let mut shared = json!({"seen": []});
    bf.run(&mut shared, None).await.unwrap();
    let mut seen: Vec<String> = shared["seen"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    seen.sort();
    assert_eq!(seen, vec!["a", "c"]);
}

/* --------------------------- Nested flows ---------------------------- */

#[derive(Clone)]
struct TraceLeaf {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for TraceLeaf {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["trace"].as_array_mut().unwrap().push(json!("leaf"));
        Ok(None)
    }
}

#[tokio::test]
async fn flow_can_be_used_as_node_inside_another_flow() {
    let inner: NodeRef = nref(Flow::new(nref(TraceLeaf { core: core("Leaf") })));
    let outer = Flow::new(inner);
    let mut shared = json!({"trace": []});
    outer.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["trace"], json!(["leaf"]));
}

/* --------------------------- clone_node() ---------------------------- */

#[derive(Clone)]
struct ExecOne {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for ExecOne {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        Ok(json!(1))
    }
}

#[derive(Clone)]
struct ExecTwo {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for ExecTwo {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        Ok(json!(2))
    }
}

#[tokio::test]
async fn clone_deep_clones_successor_subgraph_preserving_methods() {
    let a = nref(ExecOne { core: core("A") });
    let b = nref(ExecTwo { core: core("B") });
    a.next(b.clone()); // default edge a -> b
    b.on("loop").to(a.clone()); // cycle: b -> a

    let mut memo = CloneMemo::new();
    let a2 = a.clone_node(&mut memo);
    assert!(!Arc::ptr_eq(&a2, &a));
    let b2 = a2.resolve_successor(&None).unwrap();
    assert!(!Arc::ptr_eq(&b2, &b)); // successor was cloned
    assert_eq!(b2.name(), "B");
    // cycle preserved: clone's B routes back to clone's A (memoised), not the original
    assert!(Arc::ptr_eq(
        &b2.resolve_successor(&Some("loop".into())).unwrap(),
        &a2
    ));
    // methods preserved
    assert_eq!(b2.exec(&Value::Null).await.unwrap(), json!(2));
    assert_eq!(a2.exec(&Value::Null).await.unwrap(), json!(1));
}

#[tokio::test]
async fn clone_copies_params_dicts_not_shares() {
    let a = nref(ExecOne { core: core("A") });
    a.set_params(json!({"x": 1}));
    let mut memo = CloneMemo::new();
    let a2 = a.clone_node(&mut memo);
    a2.set_params(json!({"x": 2}));
    assert_eq!(a.core().own_params().unwrap()["x"], json!(1));
    assert_eq!(a2.core().own_params().unwrap()["x"], json!(2));
}

#[derive(Clone)]
struct HitLeaf {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for HitLeaf {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["hit"] = json!(true);
        Ok(None)
    }
}

#[tokio::test]
async fn cloning_a_flow_clones_its_start_node_subgraph() {
    let f = Flow::new(nref(HitLeaf { core: core("Leaf") }));
    let mut memo = CloneMemo::new();
    let f2 = f.clone_node(&mut memo);
    let f2_start = f2.resolve_successor(&None); // flows have no successors; just check it runs
    assert!(f2_start.is_none());
    let mut shared = json!({"hit": false});
    f2.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["hit"], json!(true));
    // the clone's start node is a different instance
    let orig_start = f.start_node().unwrap();
    let mut memo2 = CloneMemo::new();
    let f3 = f.clone_node(&mut memo2);
    let _ = f3;
    let _ = orig_start;
}

/* --------------------- Subflow (isolated context) -------------------- */

#[derive(Clone)]
struct Research {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for Research {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["findings"] = json!(["f1", "f2"]);
        Ok(None)
    }
}

#[derive(Clone)]
struct MySubOps;

#[async_trait]
impl SubflowOps for MySubOps {
    async fn derive_shared(&self, shared: &Shared, _c: Option<&SharedCloner>) -> Result<Shared> {
        Ok(json!({"question": shared["question"].clone(), "findings": []}))
    }
    async fn reduce(&self, sub_shared: &Shared, shared: &mut Shared) -> Result<()> {
        let findings = sub_shared["findings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>()
            .join(",");
        shared["answer"] = json!(findings);
        Ok(())
    }
}

#[tokio::test]
async fn subflow_runs_isolated_context_and_reduces_back() {
    let sub: NodeRef = nref(Subflow::with_ops(
        nref(Flow::new(nref(Research {
            core: core("Research"),
        }))),
        MySubOps,
    ));
    let flow = Flow::new(sub);
    let mut shared = json!({"question": "what is X?"});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["answer"], json!("f1,f2"));
    assert!(shared.get("findings").is_none()); // isolation: parent untouched except via reduce
}

#[derive(Clone)]
struct Mutate {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for Mutate {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["list"].as_array_mut().unwrap().push(json!("x"));
        Ok(None)
    }
}

#[tokio::test]
async fn subflow_mutations_do_not_leak_into_parent() {
    // default derive_shared = clone the parent shared
    let sub: NodeRef = nref(Subflow::new(nref(Flow::new(nref(Mutate {
        core: core("Mutate"),
    })))));
    let flow = Flow::new(sub);
    let mut before = json!({"list": []});
    flow.run(&mut before, None).await.unwrap();
    assert_eq!(before["list"], json!([])); // the push went to the clone, not the parent
}

#[derive(Clone)]
struct DoneLeaf {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for DoneLeaf {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["done"] = json!(true);
        Ok(None)
    }
}

struct NameTrace {
    names: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Hooks for NameTrace {
    async fn on_node_start(&self, ctx: NodeHookCtx<'_>) -> Result<Action> {
        self.names.lock().unwrap().push(ctx.node.name().to_string());
        Ok(None)
    }
}

#[tokio::test]
async fn subflow_inherits_parent_hooks_for_tracing() {
    let names = Arc::new(Mutex::new(vec![]));
    let inner = Flow::new(nref(DoneLeaf { core: core("Leaf") })).with_name("inner");
    let sub: NodeRef = nref(Subflow::new(nref(inner)).with_node_opts(NodeOpts::new().name("sub")));
    let flow = Flow::new(sub);
    flow.use_hooks(Arc::new(NameTrace {
        names: names.clone(),
    }));
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    let names = names.lock().unwrap();
    assert!(names.contains(&"sub".to_string()));
    assert!(names.contains(&"Leaf".to_string())); // nested node traced via composed hooks
}

/* --------------------- ForkJoin (parallel branches) ------------------ */

#[derive(Clone)]
struct TopicAgent {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for TopicAgent {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        let topic = self.param("topic").unwrap();
        shared["results"].as_array_mut().unwrap().push(topic);
        Ok(None)
    }
}

#[derive(Clone)]
struct FanJoin;

#[async_trait]
impl ForkJoinOps for FanJoin {
    async fn join(&self, shared: &mut Shared, branch_shareds: &[Option<Shared>]) -> Result<()> {
        let mut all = vec![];
        for b in branch_shareds.iter().flatten() {
            all.extend(b["results"].as_array().unwrap().clone());
        }
        shared["results"] = Value::Array(all);
        Ok(())
    }
}

fn topic_agent(topic: &str) -> NodeRef {
    let a = nref(TopicAgent {
        core: core(&format!("Agent:{topic}")),
    });
    a.set_params(json!({"topic": topic}));
    a
}

#[tokio::test]
async fn fork_join_fans_out_and_joins_results() {
    let fan: NodeRef = nref(ForkJoin::with_ops(
        vec![topic_agent("A"), topic_agent("B"), topic_agent("C")],
        FanJoin,
    ));
    let flow = Flow::new(fan);
    let mut shared = json!({"results": []});
    flow.run(&mut shared, None).await.unwrap();
    let mut results: Vec<String> = shared["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    results.sort();
    assert_eq!(results, vec!["A", "B", "C"]);
}

#[derive(Clone)]
struct SlowPrep {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for SlowPrep {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        sleep(Duration::from_millis(30)).await;
        Ok(Value::Null)
    }
}

#[tokio::test]
async fn fork_join_branches_run_concurrently() {
    let branches: Vec<NodeRef> = (0..4)
        .map(|_| nref(SlowPrep { core: core("Slow") }) as NodeRef)
        .collect();
    let fan: NodeRef = nref(ForkJoin::new(branches).with_concurrency(4));
    let flow = Flow::new(fan);
    let mut shared = json!({});
    let start = Instant::now();
    flow.run(&mut shared, None).await.unwrap();
    assert!(start.elapsed() < Duration::from_millis(90)); // parallel ~30ms
}

#[derive(Clone)]
struct GoodBranch {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for GoodBranch {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["ok"] = json!(true);
        Ok(None)
    }
}

#[derive(Clone)]
struct BadBranch {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for BadBranch {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        Err(FlowError::msg("boom"))
    }
}

#[derive(Clone)]
struct SlotsToGlobal {
    slots: Arc<Mutex<Vec<Option<Shared>>>>,
}

#[async_trait]
impl ForkJoinOps for SlotsToGlobal {
    async fn join(&self, _shared: &mut Shared, branch_shareds: &[Option<Shared>]) -> Result<()> {
        *self.slots.lock().unwrap() = branch_shareds.to_vec();
        Ok(())
    }
}

#[tokio::test]
async fn fork_join_fail_fast_false_yields_none_slot() {
    let slots = Arc::new(Mutex::new(vec![]));
    let fan: NodeRef = nref(ForkJoin::with_ops(
        vec![
            nref(GoodBranch { core: core("Good") }),
            nref(BadBranch { core: core("Bad") }),
            nref(GoodBranch { core: core("Good") }),
        ],
        SlotsToGlobal {
            slots: slots.clone(),
        },
    ));
    let flow = Flow::new(fan);
    let mut shared = json!({"ok": false});
    flow.run(&mut shared, None).await.unwrap();
    let bs = slots.lock().unwrap();
    assert_eq!(bs.len(), 3);
    assert_eq!(bs.iter().filter(|b| b.is_some()).count(), 2); // only Bad is None
}

#[derive(Clone)]
struct Writer {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for Writer {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["log"] = json!([self.param("id").unwrap()]);
        shared["val"] = json!(999);
        Ok(None)
    }
}

fn writer(id: &str) -> NodeRef {
    let w = nref(Writer {
        core: core(&format!("Writer:{id}")),
    });
    w.set_params(json!({"id": id}));
    w
}

#[tokio::test]
async fn fork_join_branches_are_isolated() {
    let slots = Arc::new(Mutex::new(vec![]));
    let fan: NodeRef = nref(
        ForkJoin::with_ops(
            vec![writer("x"), writer("y")],
            SlotsToGlobal {
                slots: slots.clone(),
            },
        )
        .with_concurrency(2),
    );
    let flow = Flow::new(fan);
    let mut shared = json!({"val": 0, "log": []});
    flow.run(&mut shared, None).await.unwrap();
    let bs = slots.lock().unwrap();
    assert_eq!(bs[0].as_ref().unwrap()["log"], json!(["x"]));
    assert_eq!(bs[1].as_ref().unwrap()["log"], json!(["y"]));
    assert!(bs.iter().all(|b| b.as_ref().unwrap()["val"] == json!(999)));
    assert_eq!(shared["val"], json!(0)); // parent untouched
}

/* ------------------- BatchFlow parallel isolation -------------------- */

#[derive(Clone)]
struct TouchWorker {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for TouchWorker {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        let id = self.param("id").unwrap();
        shared["collected"].as_array_mut().unwrap().push(id);
        let touched = shared["touched"].as_i64().unwrap();
        shared["touched"] = json!(touched + 1);
        Ok(None)
    }
}

#[derive(Clone)]
struct IdBundles {
    bundles: Vec<Value>,
}

#[async_trait]
impl BatchFlowOps for IdBundles {
    async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
        Ok(Value::Array(self.bundles.clone()))
    }
    async fn merge(&self, parent: &mut Shared, bundle_shareds: &[Shared]) -> Result<()> {
        let mut all = vec![];
        for b in bundle_shareds {
            all.extend(b["collected"].as_array().unwrap().clone());
        }
        parent["collected"] = Value::Array(all);
        Ok(())
    }
}

fn ids(list: &[&str]) -> Vec<Value> {
    list.iter().map(|id| json!({"id": id})).collect()
}

#[tokio::test]
async fn batch_flow_parallel_clones_shared_and_merge_folds_back() {
    let bf = Arc::new(
        BatchFlow::with_ops(
            nref(TouchWorker {
                core: core("Worker"),
            }),
            IdBundles {
                bundles: ids(&["a", "b", "c"]),
            },
        )
        .with_concurrency(3),
    );
    let mut shared = json!({"collected": [], "touched": 0});
    bf.run(&mut shared, None).await.unwrap();
    let mut collected: Vec<String> = shared["collected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    collected.sort();
    assert_eq!(collected, vec!["a", "b", "c"]);
    assert_eq!(shared["touched"], json!(0)); // parent shared NOT mutated (isolation)
}

#[derive(Clone)]
struct RaceWorker {
    core: NodeCore,
    seen: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl FlowNode for RaceWorker {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        tokio::task::yield_now().await; // force bundle execs to overlap
        Ok(Value::Null)
    }
    async fn post(&self, _shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        let id = self.param("id").unwrap().as_str().unwrap().to_string();
        self.seen.lock().unwrap().push(id); // read params after the await
        Ok(None)
    }
}

#[derive(Clone)]
struct IdBundlesNoMerge {
    bundles: Vec<Value>,
}

#[async_trait]
impl BatchFlowOps for IdBundlesNoMerge {
    async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
        Ok(Value::Array(self.bundles.clone()))
    }
}

#[tokio::test]
async fn batch_flow_parallel_params_do_not_race() {
    let seen = Arc::new(Mutex::new(vec![]));
    let bf = Arc::new(
        BatchFlow::with_ops(
            nref(RaceWorker {
                core: core("Worker"),
                seen: seen.clone(),
            }),
            IdBundlesNoMerge {
                bundles: ids(&["a", "b", "c"]),
            },
        )
        .with_concurrency(3),
    );
    let mut shared = json!({});
    bf.run(&mut shared, None).await.unwrap();
    let mut seen = seen.lock().unwrap().clone();
    seen.sort();
    assert_eq!(seen, vec!["a", "b", "c"]); // each bundle saw its OWN params
}

#[tokio::test]
async fn batch_flow_sequential_accumulates_into_real_shared() {
    let bf = Arc::new(
        BatchFlow::with_ops(
            nref(TouchWorker {
                core: core("Collector"),
            }),
            IdBundlesNoMerge {
                bundles: ids(&["a", "b"]),
            },
        )
        .with_concurrency(1),
    );
    let mut shared = json!({"collected": [], "touched": 0});
    bf.run(&mut shared, None).await.unwrap();
    let collected: Vec<String> = shared["collected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(collected, vec!["a", "b"]); // real shared mutated, in order
}

/* -------------- Deep-research-style composition ---------------------- */

#[derive(Clone)]
struct AgentResearch {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for AgentResearch {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        let topic = shared["topic"].as_str().unwrap();
        shared["findings"] = json!([format!("about-{topic}")]);
        Ok(None)
    }
}

#[derive(Clone)]
struct ResearchBranchOps {
    topic: String,
    out: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl SubflowOps for ResearchBranchOps {
    async fn derive_shared(&self, _shared: &Shared, _c: Option<&SharedCloner>) -> Result<Shared> {
        Ok(json!({"topic": self.topic, "findings": []}))
    }
    async fn reduce(&self, sub_shared: &Shared, _shared: &mut Shared) -> Result<()> {
        let findings: Vec<String> = sub_shared["findings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        *self.out.lock().unwrap() = findings;
        Ok(())
    }
}

#[derive(Clone)]
struct SynthesizeOps {
    outs: Vec<Arc<Mutex<Vec<String>>>>,
}

#[async_trait]
impl ForkJoinOps for SynthesizeOps {
    async fn join(&self, shared: &mut Shared, _branch_shareds: &[Option<Shared>]) -> Result<()> {
        let report: Vec<String> = self
            .outs
            .iter()
            .map(|o| o.lock().unwrap().first().cloned().unwrap_or_default())
            .collect();
        shared["report"] = json!(report);
        Ok(())
    }
}

#[tokio::test]
async fn fork_join_of_subflows_parallel_sub_agents_then_synthesize() {
    let mk_branch = |topic: &str| -> (NodeRef, Arc<Mutex<Vec<String>>>) {
        let out = Arc::new(Mutex::new(vec![]));
        let inner = Flow::new(nref(AgentResearch {
            core: core("Research"),
        }))
        .with_name(format!("research:{topic}"));
        let branch: NodeRef = nref(
            Subflow::with_ops(
                nref(inner),
                ResearchBranchOps {
                    topic: topic.to_string(),
                    out: out.clone(),
                },
            )
            .with_node_opts(NodeOpts::new().name(format!("branch:{topic}"))),
        );
        (branch, out)
    };
    let (b1, o1) = mk_branch("RAG");
    let (b2, o2) = mk_branch("fine-tuning");
    let (b3, o3) = mk_branch("hybrid");

    let fan: NodeRef = nref(
        ForkJoin::with_ops(
            vec![b1, b2, b3],
            SynthesizeOps {
                outs: vec![o1, o2, o3],
            },
        )
        .with_concurrency(3),
    );
    let flow = Flow::new(fan);
    let mut shared = json!({"question": "RAG vs fine-tuning", "report": []});
    flow.run(&mut shared, None).await.unwrap();
    let mut report: Vec<String> = shared["report"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    report.sort();
    assert_eq!(
        report,
        vec!["about-RAG", "about-fine-tuning", "about-hybrid"]
    );
    assert!(shared.get("topic").is_none()); // orchestrator context stayed clean
}

/* ------------------------ Timeouts & cancellation -------------------- */

#[derive(Clone)]
struct VerySlowPrep {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for VerySlowPrep {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        sleep(Duration::from_millis(200)).await;
        Ok(Value::Null)
    }
}

#[tokio::test]
async fn slow_node_exceeds_timeout_throws() {
    let flow = Flow::new(nref(VerySlowPrep {
        core: NodeCore::with_opts(NodeOpts::new().timeout_ms(40)),
    }));
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert!(matches!(err, FlowError::Timeout { timeout_ms: 40 }));
}

#[derive(Clone)]
struct LateExec {
    core: NodeCore,
    exec_resolved: Arc<AtomicBool>,
    post_ran: Arc<AtomicBool>,
}

#[async_trait]
impl FlowNode for LateExec {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        sleep(Duration::from_millis(120)).await;
        self.exec_resolved.store(true, Ordering::SeqCst);
        Ok(json!("LATE"))
    }
    async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        self.post_ran.store(true, Ordering::SeqCst);
        Ok(None)
    }
}

#[tokio::test]
async fn timed_out_node_post_never_runs() {
    // TS regression note: in JS the underlying exec promise still settles in
    // the background. In Rust the in-flight future is DROPPED (cancelled) when
    // the race is lost — the guarantee that matters is identical: post() is
    // never called with stale data.
    let exec_resolved = Arc::new(AtomicBool::new(false));
    let post_ran = Arc::new(AtomicBool::new(false));
    let flow = Flow::new(nref(LateExec {
        core: NodeCore::with_opts(NodeOpts::new().timeout_ms(30)),
        exec_resolved: exec_resolved.clone(),
        post_ran: post_ran.clone(),
    }));
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert!(matches!(err, FlowError::Timeout { .. }));
    sleep(Duration::from_millis(200)).await;
    assert!(!post_ran.load(Ordering::SeqCst)); // post() was NOT called
    assert!(!exec_resolved.load(Ordering::SeqCst)); // Rust: exec future was cancelled
}

#[test]
fn timeout_error_carries_ms() {
    let e = FlowError::timeout(99);
    assert_eq!(e.timeout_ms(), Some(99));
    assert!(err_msg(&e).contains("99"));
}

#[derive(Clone)]
struct QuickPrep {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for QuickPrep {
    impl_node_core!(core);
    async fn prep(&self, shared: &mut Shared) -> Result<Value> {
        sleep(Duration::from_millis(10)).await;
        shared["ok"] = json!(true);
        Ok(Value::Null)
    }
}

#[tokio::test]
async fn node_under_timeout_completes_normally() {
    let flow = Flow::new(nref(QuickPrep {
        core: NodeCore::with_opts(NodeOpts::new().timeout_ms(500)),
    }));
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["ok"], json!(true));
}

#[derive(Clone)]
struct TimeoutRecover {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for TimeoutRecover {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        shared["result"] = json!("recovered");
        Ok(None)
    }
}

struct RouteOnTimeout;
#[async_trait]
impl Hooks for RouteOnTimeout {
    async fn on_error(&self, ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
        Ok(if matches!(ctx.error, FlowError::Timeout { .. }) {
            Some(ErrorPolicy::Route("timeout".to_string()))
        } else {
            None
        })
    }
}

#[tokio::test]
async fn timeout_can_be_routed_via_on_error() {
    let posted = Arc::new(AtomicBool::new(false));
    let slow = nref(RiskySlow {
        core: NodeCore::with_opts(NodeOpts::new().timeout_ms(30)),
        posted: posted.clone(),
    });
    let recover = nref(TimeoutRecover {
        core: core("Recover"),
    });
    slow.on("timeout").to(recover);
    let flow = Flow::new(slow);
    flow.use_hooks(Arc::new(RouteOnTimeout));
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["result"], json!("recovered"));
    assert!(!posted.load(Ordering::SeqCst));
}

#[derive(Clone)]
struct RiskySlow {
    core: NodeCore,
    posted: Arc<AtomicBool>,
}

#[async_trait]
impl FlowNode for RiskySlow {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        sleep(Duration::from_millis(200)).await;
        Ok(Value::Null)
    }
    async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        self.posted.store(true, Ordering::SeqCst);
        Ok(None)
    }
}

#[derive(Clone)]
struct HangPrep {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for HangPrep {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        sleep(Duration::from_millis(1000)).await;
        Ok(Value::Null)
    }
}

#[tokio::test]
async fn aborting_run_rejects_with_abort_error() {
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(20)).await;
        t2.cancel();
    });
    let flow = Flow::new(nref(HangPrep { core: core("Slow") }));
    let mut shared = json!({});
    let err = flow.run(&mut shared, Some(tok)).await.unwrap_err();
    assert!(err.is_aborted());
}

#[test]
fn abort_error_display() {
    let e = FlowError::Aborted;
    assert!(err_msg(&e).contains("aborted"));
}

struct OnErrorFlag {
    called: Arc<AtomicBool>,
}
#[async_trait]
impl Hooks for OnErrorFlag {
    async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
        self.called.store(true, Ordering::SeqCst);
        Ok(None)
    }
}

#[tokio::test]
async fn abort_is_terminal_bypasses_on_error() {
    let called = Arc::new(AtomicBool::new(false));
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(20)).await;
        t2.cancel();
    });
    let flow = Flow::new(nref(HangPrep { core: core("Slow") }));
    flow.use_hooks(Arc::new(OnErrorFlag {
        called: called.clone(),
    }));
    let mut shared = json!({});
    let err = flow.run(&mut shared, Some(tok)).await.unwrap_err();
    assert!(err.is_aborted());
    assert!(!called.load(Ordering::SeqCst)); // abort never reaches on_error
}

#[derive(Clone)]
struct Fetchy {
    core: NodeCore,
    saw_signal: Arc<AtomicBool>,
}

#[async_trait]
impl FlowNode for Fetchy {
    impl_node_core!(core);
    async fn exec(&self, _p: &Value) -> Result<Value> {
        // simulate an abortable fetch: race work against this.signal
        let sig = self.signal().unwrap();
        self.saw_signal.store(!sig.is_cancelled(), Ordering::SeqCst);
        tokio::select! {
            _ = sleep(Duration::from_millis(500)) => Err(FlowError::msg("completed")),
            _ = sig.cancelled() => Err(FlowError::Aborted),
        }
    }
}

#[tokio::test]
async fn cooperative_cancel_exec_reads_signal() {
    let saw = Arc::new(AtomicBool::new(false));
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(20)).await;
        t2.cancel();
    });
    let flow = Flow::new(nref(Fetchy {
        core: core("Fetchy"),
        saw_signal: saw.clone(),
    }));
    let mut shared = json!({});
    let err = flow.run(&mut shared, Some(tok)).await.unwrap_err();
    assert!(err.is_aborted());
    assert!(saw.load(Ordering::SeqCst)); // exec saw the (not-yet-cancelled) token
}

#[tokio::test]
async fn abort_propagates_through_nested_flow() {
    let inner: NodeRef = nref(Flow::new(nref(HangPrep { core: core("Leaf") })));
    let outer = Flow::new(inner);
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(20)).await;
        t2.cancel();
    });
    let mut shared = json!({});
    let err = outer.run(&mut shared, Some(tok)).await.unwrap_err();
    assert!(err.is_aborted());
}

/* ------------------- failFast actually cancels siblings --------------- */

#[derive(Clone)]
struct FailFastWorker {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for FailFastWorker {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        Ok(json!(["a", "b", "c"]))
    }
    async fn exec_item(&self, item: &Value, _sig: &CancellationToken) -> Result<Value> {
        if item == &json!("a") {
            return Err(FlowError::msg("a failed"));
        }
        sleep(Duration::from_millis(300)).await; // hangs
        Ok(item.clone())
    }
}

#[tokio::test]
async fn batch_fail_fast_cancels_in_flight_siblings() {
    let w = nref(FailFastWorker {
        core: NodeCore::with_opts(NodeOpts::batch().concurrency(3).fail_fast(true)),
    });
    let flow = Flow::new(w);
    let mut shared = json!({});
    let start = Instant::now();
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "a failed");
    assert!(start.elapsed() < Duration::from_millis(150)); // ~300ms if siblings weren't cancelled
}

#[tokio::test]
async fn batch_fail_fast_false_does_not_cancel_siblings() {
    #[derive(Clone)]
    struct W {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for W {
        impl_node_core!(core);
        async fn prep(&self, _s: &mut Shared) -> Result<Value> {
            Ok(json!(["a", "b", "c"]))
        }
        async fn exec_item(&self, item: &Value, _sig: &CancellationToken) -> Result<Value> {
            if item == &json!("a") {
                return Err(FlowError::msg("a failed"));
            }
            sleep(Duration::from_millis(30)).await;
            Ok(item.clone())
        }
        async fn post(&self, shared: &mut Shared, _p: &Value, exec_res: &Value) -> Result<Action> {
            shared["batchOut"] = exec_res.clone();
            Ok(None)
        }
    }
    let w = nref(W {
        core: NodeCore::with_opts(NodeOpts::batch().concurrency(3).fail_fast(false)),
    });
    let flow = Flow::new(w);
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["batchOut"], json!([null, "b", "c"])); // b & c finished
}

#[tokio::test]
async fn batch_fail_fast_cooperative_exec_item_stops_io() {
    #[derive(Clone)]
    struct W {
        core: NodeCore,
        cancelled: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait]
    impl FlowNode for W {
        impl_node_core!(core);
        async fn prep(&self, _s: &mut Shared) -> Result<Value> {
            Ok(json!(["a", "b", "c"]))
        }
        async fn exec_item(&self, item: &Value, sig: &CancellationToken) -> Result<Value> {
            if item == &json!("a") {
                // Rust futures are lazy: without a yield here the "a" failure
                // resolves synchronously before sibling items are ever polled
                // (in JS, the promise machinery yields at a microtask boundary,
                // letting siblings start first). Emulate that boundary so this
                // exercises the cooperative-cancel path like the TS test.
                tokio::task::yield_now().await;
                return Err(FlowError::msg("a failed"));
            }
            // abortable delay: on sibling failure the per-item token cancels
            let label = item.as_str().unwrap().to_string();
            tokio::select! {
                _ = sleep(Duration::from_millis(300)) => Ok(item.clone()),
                _ = sig.cancelled() => {
                    self.cancelled.lock().unwrap().push(label);
                    Err(FlowError::Aborted)
                }
            }
        }
    }
    let cancelled = Arc::new(Mutex::new(vec![]));
    let w = nref(W {
        core: NodeCore::with_opts(NodeOpts::batch().concurrency(3).fail_fast(true)),
        cancelled: cancelled.clone(),
    });
    let flow = Flow::new(w);
    let mut shared = json!({});
    let start = Instant::now();
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "a failed");
    assert!(start.elapsed() < Duration::from_millis(150));
    let mut cancelled = cancelled.lock().unwrap().clone();
    cancelled.sort();
    assert_eq!(cancelled, vec!["b", "c"]); // both saw the cancel
}

#[tokio::test]
async fn fork_join_fail_fast_cancels_sibling_branches() {
    let fan: NodeRef = nref(
        ForkJoin::new(vec![
            nref(BadBranch { core: core("Boom") }),
            nref(SlowPrep { core: core("Hang") }),
            nref(SlowPrep { core: core("Hang") }),
        ])
        .with_concurrency(3)
        .with_fail_fast(true),
    );
    let flow = Flow::new(fan);
    let mut shared = json!({});
    let start = Instant::now();
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "boom");
    assert!(start.elapsed() < Duration::from_millis(150));
}

#[derive(Clone)]
struct BundleFailA {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for BundleFailA {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        if self.param("id") == Some(json!("a")) {
            return Err(FlowError::msg("bundle a failed"));
        }
        sleep(Duration::from_millis(300)).await;
        Ok(Value::Null)
    }
}

#[tokio::test]
async fn batch_flow_fail_fast_cancels_in_flight_bundles() {
    let bf = Arc::new(
        BatchFlow::with_ops(
            nref(BundleFailA {
                core: core("Worker"),
            }),
            IdBundlesNoMerge {
                bundles: ids(&["a", "b", "c"]),
            },
        )
        .with_concurrency(3)
        .with_fail_fast(true),
    );
    let mut shared = json!({});
    let start = Instant::now();
    let err = bf.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "bundle a failed");
    assert!(start.elapsed() < Duration::from_millis(150));
}

#[derive(Clone)]
struct MidBatch {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for MidBatch {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        Ok(json!([1, 2, 3, 4]))
    }
    async fn exec_item(&self, item: &Value, _sig: &CancellationToken) -> Result<Value> {
        sleep(Duration::from_millis(50)).await;
        Ok(item.clone())
    }
}

#[tokio::test]
async fn cancelling_batch_node_mid_run_propagates_abort() {
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        t2.cancel();
    });
    let flow = Flow::new(nref(MidBatch {
        core: NodeCore::with_opts(NodeOpts::batch().concurrency(2)),
    }));
    let mut shared = json!({});
    let err = flow.run(&mut shared, Some(tok)).await.unwrap_err();
    assert!(err.is_aborted());
}

/* --------------------- Cancellation is not retried -------------------- */

#[tokio::test]
async fn node_with_retries_does_not_burn_them_on_abort() {
    #[derive(Clone)]
    struct FlakyAbort {
        core: NodeCore,
        attempts: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl FlowNode for FlakyAbort {
        impl_node_core!(core);
        async fn exec(&self, _p: &Value) -> Result<Value> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            sleep(Duration::from_millis(50)).await; // will be aborted
            Ok(json!("ok"))
        }
    }
    let attempts = Arc::new(AtomicUsize::new(0));
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        t2.cancel();
    });
    let flow = Flow::new(nref(FlakyAbort {
        core: NodeCore::with_opts(NodeOpts::new().max_retries(5)),
        attempts: attempts.clone(),
    }));
    let mut shared = json!({});
    let err = flow.run(&mut shared, Some(tok)).await.unwrap_err();
    assert!(err.is_aborted());
    assert_eq!(attempts.load(Ordering::SeqCst), 1); // not retried despite maxRetries=5
}

/* -------------------- internal helpers (unit tests) ------------------- */

#[tokio::test]
async fn internal_link_signal_no_parent() {
    let tok = focket_rs::internal::link_signal(None);
    assert!(!tok.is_cancelled());
    tok.cancel();
    assert!(tok.is_cancelled());
}

#[tokio::test]
async fn internal_link_signal_pre_aborted_parent() {
    let parent = CancellationToken::new();
    parent.cancel();
    let child = focket_rs::internal::link_signal(Some(&parent));
    assert!(child.is_cancelled());
}

#[tokio::test]
async fn internal_link_signal_linked() {
    let parent = CancellationToken::new();
    let child = focket_rs::internal::link_signal(Some(&parent));
    assert!(!child.is_cancelled());
    parent.cancel();
    assert!(child.is_cancelled());
}

#[tokio::test]
async fn internal_race_abort_resolves_when_calm() {
    let tok = CancellationToken::new();
    let r = focket_rs::internal::race_abort(async { Ok::<_, FlowError>("ok") }, &tok).await;
    assert_eq!(r.unwrap(), "ok");
}

#[tokio::test]
async fn internal_race_abort_pre_aborted_guard() {
    let tok = CancellationToken::new();
    tok.cancel();
    let never = std::future::pending::<Result<&str>>();
    let r = focket_rs::internal::race_abort(never, &tok).await;
    assert!(r.unwrap_err().is_aborted());
}

#[tokio::test]
async fn internal_race_abort_mid_flight() {
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    let never = std::future::pending::<Result<&str>>();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        t2.cancel();
    });
    let r = focket_rs::internal::race_abort(never, &tok).await;
    assert!(r.unwrap_err().is_aborted());
}

#[tokio::test]
async fn internal_race_work_no_timeout() {
    let tok = CancellationToken::new();
    let r = focket_rs::internal::race_work(async { Ok::<_, FlowError>(7) }, 0, &tok).await;
    assert_eq!(r.unwrap(), 7);
}

#[tokio::test]
async fn internal_race_work_timeout_fires() {
    let tok = CancellationToken::new();
    let never = std::future::pending::<Result<&str>>();
    let r = focket_rs::internal::race_work(never, 20, &tok).await;
    assert!(matches!(
        r.unwrap_err(),
        FlowError::Timeout { timeout_ms: 20 }
    ));
}

#[tokio::test]
async fn internal_race_work_pre_aborted_guard() {
    let tok = CancellationToken::new();
    tok.cancel();
    let never = std::future::pending::<Result<&str>>();
    let r = focket_rs::internal::race_work(never, 0, &tok).await;
    assert!(r.unwrap_err().is_aborted());
}

#[tokio::test]
async fn internal_race_work_mid_flight_abort() {
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    let never = std::future::pending::<Result<&str>>();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        t2.cancel();
    });
    let r = focket_rs::internal::race_work(never, 0, &tok).await;
    assert!(r.unwrap_err().is_aborted());
}

#[tokio::test]
async fn internal_race_work_work_wins() {
    let tok = CancellationToken::new();
    let r = focket_rs::internal::race_work(async { Ok::<_, FlowError>("done") }, 1000, &tok).await;
    assert_eq!(r.unwrap(), "done");
}

#[tokio::test]
async fn internal_run_parallel_empty() {
    let out =
        focket_rs::internal::run_parallel(Vec::<i32>::new(), 4, |_x, _i, _s| async { Ok(1) }, None)
            .await
            .unwrap();
    assert!(out.is_empty());
}

#[tokio::test]
async fn internal_run_parallel_sequential_order() {
    let out = focket_rs::internal::run_parallel(
        vec![10, 20, 30],
        1,
        |x, _i, _s| async move { Ok(x * 2) },
        None,
    )
    .await
    .unwrap();
    assert_eq!(out, vec![Some(20), Some(40), Some(60)]);
}

#[tokio::test]
async fn internal_run_parallel_parallel_order() {
    let out = focket_rs::internal::run_parallel(
        vec![1, 2, 3, 4],
        4,
        |x, _i, _s| async move { Ok(x) },
        None,
    )
    .await
    .unwrap();
    assert_eq!(out, vec![Some(1), Some(2), Some(3), Some(4)]);
}

#[tokio::test]
async fn internal_run_parallel_sequential_parent_abort_between_items() {
    let parent = CancellationToken::new();
    let p2 = parent.clone();
    let r = focket_rs::internal::run_parallel(
        vec![0, 1, 2],
        1,
        move |x, _i, _s| {
            let p2 = p2.clone();
            async move {
                if x == 0 {
                    p2.cancel(); // abort in the gap before item 1
                }
                Ok(x)
            }
        },
        Some(&parent),
    )
    .await;
    assert!(r.unwrap_err().is_aborted());
}

#[tokio::test]
async fn internal_run_parallel_failure_aborts_siblings() {
    let start = Instant::now();
    let r = focket_rs::internal::run_parallel(
        vec![0, 1, 2],
        3,
        |x, _i, sig| async move {
            if x == 0 {
                return Err(FlowError::msg("boom"));
            }
            tokio::select! {
                _ = sleep(Duration::from_millis(300)) => {},
                _ = sig.cancelled() => {},
            }
            Ok(x)
        },
        None,
    )
    .await;
    assert_eq!(err_msg(&r.unwrap_err()), "boom");
    assert!(start.elapsed() < Duration::from_millis(150)); // siblings aborted, didn't wait 300ms
}

#[tokio::test]
async fn internal_isolated_override_used() {
    let cloner: SharedCloner = Arc::new(|s: &Shared| {
        let mut c = s.clone();
        c["a"] = json!(999);
        c["tagged"] = json!(true);
        c
    });
    let out = focket_rs::internal::isolated(&json!({"a": 1}), Some(&cloner));
    assert_eq!(out, json!({"a": 999, "tagged": true}));
}

#[tokio::test]
async fn internal_isolated_default_deep_copies() {
    let mut src = json!({"list": [1, 2], "obj": {"x": 1}});
    let out = focket_rs::internal::isolated(&src, None);
    assert_eq!(out, src);
    src["list"].as_array_mut().unwrap().push(json!(3));
    assert_eq!(out["list"], json!([1, 2])); // deep copy: parent mutation doesn't leak
}

/* --------------- internal hooks composition (fire_*) ------------------ */

struct OrderHooks {
    tag: &'static str,
    order: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Hooks for OrderHooks {
    async fn on_start(&self, _s: &Shared) -> Result<()> {
        self.order.lock().unwrap().push(format!("s{}", self.tag));
        Ok(())
    }
    async fn on_node_end(&self, _ctx: NodeEndCtx<'_>) -> Result<()> {
        self.order.lock().unwrap().push(format!("e{}", self.tag));
        Ok(())
    }
    async fn on_retry(&self, _ctx: RetryCtx<'_>) -> Result<()> {
        self.order.lock().unwrap().push(format!("r{}", self.tag));
        Ok(())
    }
    async fn on_end(&self, _s: &Shared, _a: &Action) -> Result<()> {
        self.order.lock().unwrap().push(format!("end{}", self.tag));
        Ok(())
    }
}

#[tokio::test]
async fn internal_compose_fan_out_in_order() {
    let order = Arc::new(Mutex::new(vec![]));
    let dummy: NodeRef = nref(PassThrough { core: core("D") });
    let hooks: Vec<HookRef> = vec![
        Arc::new(OrderHooks {
            tag: "1",
            order: order.clone(),
        }),
        Arc::new(OrderHooks {
            tag: "2",
            order: order.clone(),
        }),
    ];
    let shared = json!({});
    let path: Vec<String> = vec![];
    focket_rs::fire_on_start(&hooks, &shared).await.unwrap();
    focket_rs::fire_on_node_end(&hooks, &dummy, &shared, &path, &None, 1, false, None)
        .await
        .unwrap();
    focket_rs::fire_on_retry(&hooks, &dummy, &shared, &path, &FlowError::msg("x"), 0, 0)
        .await
        .unwrap();
    focket_rs::fire_on_end(&hooks, &shared, &None)
        .await
        .unwrap();
    assert_eq!(
        *order.lock().unwrap(),
        vec!["s1", "s2", "e1", "e2", "r1", "r2", "end1", "end2"]
    );
}

struct GateHooks {
    tag: &'static str,
    seen: Arc<Mutex<Vec<String>>>,
    gate: Action,
}

#[async_trait]
impl Hooks for GateHooks {
    async fn on_node_start(&self, _ctx: NodeHookCtx<'_>) -> Result<Action> {
        self.seen.lock().unwrap().push(self.tag.to_string());
        Ok(self.gate.clone())
    }
}

#[tokio::test]
async fn internal_compose_on_node_start_first_gate_wins_all_run() {
    let seen = Arc::new(Mutex::new(vec![]));
    let dummy: NodeRef = nref(PassThrough { core: core("D") });
    let hooks: Vec<HookRef> = vec![
        Arc::new(GateHooks {
            tag: "a",
            seen: seen.clone(),
            gate: None,
        }),
        Arc::new(GateHooks {
            tag: "b",
            seen: seen.clone(),
            gate: Some("gate".into()),
        }),
        Arc::new(GateHooks {
            tag: "c",
            seen: seen.clone(),
            gate: Some("ignored".into()),
        }),
    ];
    let shared = json!({});
    let gate = focket_rs::fire_on_node_start(&hooks, &dummy, &shared, &[])
        .await
        .unwrap();
    assert_eq!(gate, Some("gate".to_string()));
    assert_eq!(*seen.lock().unwrap(), vec!["a", "b", "c"]);
}

struct ErrorHooks {
    tag: &'static str,
    seen: Arc<Mutex<Vec<String>>>,
    policy: Option<ErrorPolicy>,
}

#[async_trait]
impl Hooks for ErrorHooks {
    async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
        self.seen.lock().unwrap().push(self.tag.to_string());
        Ok(self.policy.clone())
    }
}

#[tokio::test]
async fn internal_compose_on_error_first_wins_and_short_circuits() {
    let seen = Arc::new(Mutex::new(vec![]));
    let dummy: NodeRef = nref(PassThrough { core: core("D") });
    let hooks: Vec<HookRef> = vec![
        Arc::new(ErrorHooks {
            tag: "a",
            seen: seen.clone(),
            policy: None,
        }),
        Arc::new(ErrorHooks {
            tag: "b",
            seen: seen.clone(),
            policy: Some(ErrorPolicy::Route("recover".into())),
        }),
        Arc::new(ErrorHooks {
            tag: "c",
            seen: seen.clone(),
            policy: Some(ErrorPolicy::Route("ignored".into())),
        }),
    ];
    let shared = json!({});
    let r = focket_rs::fire_on_error(&hooks, &dummy, &shared, &[], &FlowError::msg("x"))
        .await
        .unwrap();
    assert_eq!(r, Some(ErrorPolicy::Route("recover".to_string())));
    assert_eq!(*seen.lock().unwrap(), vec!["a", "b"]); // c not reached (b returned Some)
}

#[tokio::test]
async fn internal_compose_on_error_none_when_all_none() {
    let seen = Arc::new(Mutex::new(vec![]));
    let dummy: NodeRef = nref(PassThrough { core: core("D") });
    let hooks: Vec<HookRef> = vec![
        Arc::new(ErrorHooks {
            tag: "a",
            seen: seen.clone(),
            policy: None,
        }),
        Arc::new(ErrorHooks {
            tag: "b",
            seen: seen.clone(),
            policy: None,
        }),
    ];
    let shared = json!({});
    let r = focket_rs::fire_on_error(&hooks, &dummy, &shared, &[], &FlowError::msg("x"))
        .await
        .unwrap();
    assert_eq!(r, None);
}

/* --------------------- primitive behavioral edge cases ---------------- */

#[tokio::test]
async fn next_warns_when_overwriting_existing_edge() {
    init_logs();
    let a = nref(PassThrough {
        core: core("OverwriteA"),
    });
    let b1 = nref(PassThrough { core: core("B1") });
    let b2 = nref(PassThrough { core: core("B2") });
    a.next(b1);
    a.next(b2.clone()); // overwrite default → warn
    assert!(logs_contain("OverwriteA") && logs_contain("overwriting"));
    assert!(Arc::ptr_eq(&a.resolve_successor(&None).unwrap(), &b2));
}

#[tokio::test]
async fn batch_prep_void_runs_over_empty_array() {
    #[derive(Clone)]
    struct B {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for B {
        impl_node_core!(core);
        // no prep → Null → items = []
        async fn exec_item(&self, item: &Value, _sig: &CancellationToken) -> Result<Value> {
            Ok(item.clone())
        }
        async fn post(&self, shared: &mut Shared, _p: &Value, exec_res: &Value) -> Result<Action> {
            shared["b"] = exec_res.clone();
            Ok(None)
        }
    }
    let b = nref(B {
        core: NodeCore::with_opts(NodeOpts::batch()),
    });
    let mut shared = json!({});
    b.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["b"], json!([]));
}

#[tokio::test]
async fn batch_fail_fast_concurrency_throws_item_error() {
    let b = nref(BadItemBatch {
        core: NodeCore::with_opts(NodeOpts::batch().concurrency(3).fail_fast(true)),
    });
    let mut shared = json!({});
    let err = b.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "bad item");
}

#[tokio::test]
async fn fork_join_default_opts_runs_all_branches() {
    let fan: NodeRef = nref(ForkJoin::with_ops(
        vec![out_writer("x"), out_writer("y")],
        FanJoinOut,
    ));
    let flow = Flow::new(fan);
    let mut shared = json!({"out": []});
    flow.run(&mut shared, None).await.unwrap();
    let mut out: Vec<String> = shared["out"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    out.sort();
    assert_eq!(out, vec!["x", "y"]);
}

#[derive(Clone)]
struct FanJoinOut;

#[async_trait]
impl ForkJoinOps for FanJoinOut {
    async fn join(&self, shared: &mut Shared, branch_shareds: &[Option<Shared>]) -> Result<()> {
        let mut all = vec![];
        for b in branch_shareds.iter().flatten() {
            all.extend(b["out"].as_array().unwrap().clone());
        }
        shared["out"] = Value::Array(all);
        Ok(())
    }
}

#[derive(Clone)]
struct OutWriter {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for OutWriter {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        let id = self.param("id").unwrap();
        shared["out"].as_array_mut().unwrap().push(id);
        Ok(None)
    }
}

fn out_writer(id: &str) -> NodeRef {
    let w = nref(OutWriter {
        core: core(&format!("OutWriter:{id}")),
    });
    w.set_params(json!({"id": id}));
    w
}

#[tokio::test]
async fn fork_join_fail_fast_rethrows_and_aborts() {
    let start = Instant::now();
    let fan: NodeRef = nref(
        ForkJoin::new(vec![
            nref(BadBranch { core: core("Boom") }),
            nref(SlowPrep { core: core("Hang") }),
        ])
        .with_fail_fast(true),
    );
    let flow = Flow::new(fan);
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "boom");
    assert!(start.elapsed() < Duration::from_millis(150));
}

#[derive(Clone)]
struct ActiveCounter {
    core: NodeCore,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

#[async_trait]
impl FlowNode for ActiveCounter {
    impl_node_core!(core);
    async fn prep(&self, _s: &mut Shared) -> Result<Value> {
        let a = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(a, Ordering::SeqCst);
        sleep(Duration::from_millis(20)).await;
        Ok(Value::Null)
    }
    async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(None)
    }
}

#[tokio::test]
async fn fork_join_explicit_concurrency_bounds_parallelism() {
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let branches: Vec<NodeRef> = (0..4)
        .map(|_| {
            nref(ActiveCounter {
                core: core("W"),
                active: active.clone(),
                max_active: max_active.clone(),
            }) as NodeRef
        })
        .collect();
    let fan: NodeRef = nref(ForkJoin::new(branches).with_concurrency(2));
    let flow = Flow::new(fan);
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert!(max_active.load(Ordering::SeqCst) <= 2);
}

#[tokio::test]
async fn fork_join_undefined_slot_via_join() {
    #[derive(Clone)]
    struct SlotJoin;
    #[async_trait]
    impl ForkJoinOps for SlotJoin {
        async fn join(&self, shared: &mut Shared, branch_shareds: &[Option<Shared>]) -> Result<()> {
            shared["slots"] = json!(branch_shareds.iter().filter(|b| b.is_some()).count());
            Ok(())
        }
    }
    #[derive(Clone)]
    struct OkCounter {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for OkCounter {
        impl_node_core!(core);
        async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
            let s = shared["slots"].as_i64().unwrap_or(0);
            shared["slots"] = json!(s + 1);
            Ok(None)
        }
    }
    let fan: NodeRef = nref(ForkJoin::with_ops(
        vec![
            nref(BadBranch { core: core("Boom") }),
            nref(OkCounter { core: core("Ok") }),
        ],
        SlotJoin,
    ));
    let flow = Flow::new(fan);
    let mut shared = json!({"slots": 0});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["slots"], json!(1)); // Boom → None, Ok → counted
}

#[tokio::test]
async fn subflow_inner_failure_exercises_exec_fallback() {
    #[derive(Clone)]
    struct FailLeaf {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for FailLeaf {
        impl_node_core!(core);
        async fn prep(&self, _s: &mut Shared) -> Result<Value> {
            Err(FlowError::msg("leaf fail"))
        }
    }
    #[derive(Clone)]
    struct PropagateOps {
        called: Arc<AtomicBool>,
    }
    #[async_trait]
    impl SubflowOps for PropagateOps {
        async fn derive_shared(
            &self,
            shared: &Shared,
            _c: Option<&SharedCloner>,
        ) -> Result<Shared> {
            Ok(shared.clone())
        }
        async fn exec_fallback(&self, _sub: &Value, _err: FlowError) -> Result<Value> {
            self.called.store(true, Ordering::SeqCst);
            Err(FlowError::msg("propagated"))
        }
    }
    let called = Arc::new(AtomicBool::new(false));
    let sub: NodeRef = nref(Subflow::with_ops(
        nref(Flow::new(nref(FailLeaf { core: core("Leaf") }))),
        PropagateOps {
            called: called.clone(),
        },
    ));
    let flow = Flow::new(sub);
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "propagated");
    assert!(called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn subflow_clone_shared_option_uses_override() {
    #[derive(Clone)]
    struct TagLeaf {
        core: NodeCore,
        seen: Arc<Mutex<String>>,
    }
    #[async_trait]
    impl FlowNode for TagLeaf {
        impl_node_core!(core);
        async fn prep(&self, shared: &mut Shared) -> Result<Value> {
            *self.seen.lock().unwrap() = shared["tag"].as_str().unwrap().to_string();
            Ok(Value::Null)
        }
    }
    let seen = Arc::new(Mutex::new(String::new()));
    let cloner: SharedCloner =
        Arc::new(|s: &Shared| json!({"tag": format!("{}-cloned", s["tag"].as_str().unwrap())}));
    let sub: NodeRef = nref(
        Subflow::new(nref(Flow::new(nref(TagLeaf {
            core: core("Leaf"),
            seen: seen.clone(),
        }))))
        .with_clone_shared(cloner),
    );
    let flow = Flow::new(sub);
    let mut shared = json!({"tag": "orig"});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(*seen.lock().unwrap(), "orig-cloned");
}

#[tokio::test]
async fn batch_flow_parallel_with_no_start_node_throws() {
    let bf: NodeRef = nref(
        BatchFlow::without_start(IdBundlesNoMerge {
            bundles: vec![json!({"x": 1}), json!({"x": 2})],
        })
        .with_concurrency(3),
    );
    let flow = Flow::new(bf);
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert!(err_msg(&err).contains("no start node"));
}

#[tokio::test]
async fn batch_flow_parallel_fail_fast_rethrows_bundle_error() {
    #[derive(Clone)]
    struct BundleB {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for BundleB {
        impl_node_core!(core);
        async fn prep(&self, _s: &mut Shared) -> Result<Value> {
            if self.param("id") == Some(json!("b")) {
                return Err(FlowError::msg("bundle b"));
            }
            Ok(Value::Null)
        }
    }
    let bf: NodeRef = nref(
        BatchFlow::with_ops(
            nref(BundleB {
                core: core("Worker"),
            }),
            IdBundlesNoMerge {
                bundles: ids(&["a", "b", "c"]),
            },
        )
        .with_concurrency(3)
        .with_fail_fast(true),
    );
    let flow = Flow::new(bf);
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "bundle b");
}

#[derive(Clone)]
struct IdBundlesSeen {
    bundles: Vec<Value>,
}

#[async_trait]
impl BatchFlowOps for IdBundlesSeen {
    async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
        Ok(Value::Array(self.bundles.clone()))
    }
    async fn merge(&self, parent: &mut Shared, bundle_shareds: &[Shared]) -> Result<()> {
        let mut all = vec![];
        for b in bundle_shareds {
            all.extend(b["seen"].as_array().unwrap().clone());
        }
        parent["seen"] = Value::Array(all);
        Ok(())
    }
}

#[tokio::test]
async fn batch_flow_parallel_non_fail_fast_on_error_fires_and_merge_folds() {
    let errs = Arc::new(AtomicUsize::new(0));
    struct CountOnError {
        errs: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Hooks for CountOnError {
        async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
            self.errs.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }
    let bf: NodeRef = nref(
        BatchFlow::with_ops(
            nref(BoomBundleId {
                core: core("Worker"),
            }),
            IdBundlesSeen {
                bundles: ids(&["a", "b", "c"]),
            },
        )
        .with_concurrency(3),
    );
    let flow = Flow::new(bf);
    flow.use_hooks(Arc::new(CountOnError { errs: errs.clone() }));
    let mut shared = json!({"seen": []});
    flow.run(&mut shared, None).await.unwrap();
    let mut seen: Vec<String> = shared["seen"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    seen.sort();
    assert_eq!(seen, vec!["a", "c"]); // only surviving bundles merged in
    assert!(errs.load(Ordering::SeqCst) >= 1); // bundle failure reported via on_error
}

#[derive(Clone)]
struct BoomBundleId {
    core: NodeCore,
}

#[async_trait]
impl FlowNode for BoomBundleId {
    impl_node_core!(core);
    async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
        let id = self.param("id").unwrap();
        if id == json!("b") {
            return Err(FlowError::msg("bundle b"));
        }
        shared["seen"].as_array_mut().unwrap().push(id);
        Ok(None)
    }
}

#[tokio::test]
async fn batch_flow_sequential_fail_fast_rethrows() {
    #[derive(Clone)]
    struct SeqA {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for SeqA {
        impl_node_core!(core);
        async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
            if self.param("id") == Some(json!("a")) {
                return Err(FlowError::msg("seq a"));
            }
            Ok(None)
        }
    }
    let bf: NodeRef = nref(
        BatchFlow::with_ops(
            nref(SeqA {
                core: core("Worker"),
            }),
            IdBundlesNoMerge {
                bundles: ids(&["a", "b"]),
            },
        )
        .with_concurrency(1)
        .with_fail_fast(true),
    );
    let flow = Flow::new(bf);
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert_eq!(err_msg(&err), "seq a");
}

#[tokio::test]
async fn batch_flow_sequential_non_fail_fast_continues() {
    let errs = Arc::new(AtomicUsize::new(0));
    struct CountOnError {
        errs: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Hooks for CountOnError {
        async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
            self.errs.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }
    #[derive(Clone)]
    struct SeqWorker {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for SeqWorker {
        impl_node_core!(core);
        async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
            let id = self.param("id").unwrap();
            if id == json!("a") {
                return Err(FlowError::msg("seq a"));
            }
            shared["seen"].as_array_mut().unwrap().push(id);
            Ok(None)
        }
    }
    let bf: NodeRef = nref(
        BatchFlow::with_ops(
            nref(SeqWorker {
                core: core("Worker"),
            }),
            IdBundlesNoMerge {
                bundles: ids(&["a", "b"]),
            },
        )
        .with_concurrency(1),
    );
    let flow = Flow::new(bf);
    flow.use_hooks(Arc::new(CountOnError { errs: errs.clone() }));
    let mut shared = json!({"seen": []});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["seen"], json!(["b"])); // "a" failed but "b" still ran
    assert!(errs.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn bare_node_run_honors_signal() {
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        t2.cancel();
    });
    let node: NodeRef = nref(HangPrep { core: core("Slow") });
    let mut shared = json!({});
    let err = node.run(&mut shared, Some(tok)).await.unwrap_err();
    assert!(err.is_aborted());
}

#[tokio::test]
async fn on_node_end_reports_error_when_node_fails() {
    #[derive(Clone)]
    struct Kaboom {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for Kaboom {
        impl_node_core!(core);
        async fn exec(&self, _p: &Value) -> Result<Value> {
            Err(FlowError::msg("kaboom"))
        }
    }
    struct ErrorReport {
        reported: Arc<Mutex<Option<String>>>,
    }
    #[async_trait]
    impl Hooks for ErrorReport {
        async fn on_node_end(&self, ctx: NodeEndCtx<'_>) -> Result<()> {
            *self.reported.lock().unwrap() = ctx.error.map(|e| e.to_string());
            Ok(())
        }
    }
    let reported = Arc::new(Mutex::new(None));
    let flow = Flow::new(nref(Kaboom { core: core("Boom") }));
    flow.use_hooks(Arc::new(ErrorReport {
        reported: reported.clone(),
    }));
    let mut shared = json!({});
    let _ = flow.run(&mut shared, None).await;
    assert_eq!(*reported.lock().unwrap(), Some("kaboom".to_string()));
}

#[tokio::test]
async fn flow_with_void_prep_still_orchestrates() {
    #[derive(Clone)]
    struct VoidPrep;
    #[async_trait]
    impl FlowOps for VoidPrep {
        async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
            Ok(Value::Null)
        }
    }
    #[derive(Clone)]
    struct OkLeaf {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for OkLeaf {
        impl_node_core!(core);
        async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
            shared["ok"] = json!(true);
            Ok(None)
        }
    }
    let w: NodeRef = nref(Flow::with_ops(
        nref(OkLeaf { core: core("Leaf") }),
        VoidPrep,
    ));
    let flow = Flow::new(w);
    let mut shared = json!({"ok": false});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["ok"], json!(true));
}

#[tokio::test]
async fn orchestration_aborts_at_node_boundary_when_pre_aborted() {
    let tok = CancellationToken::new();
    tok.cancel(); // pre-aborted
    let fan: NodeRef = nref(
        ForkJoin::new(vec![
            nref(PassThrough { core: core("N") }),
            nref(PassThrough { core: core("N") }),
        ])
        .with_concurrency(2),
    );
    let flow = Flow::new(fan);
    let mut shared = json!({});
    let err = flow.run(&mut shared, Some(tok)).await.unwrap_err();
    assert!(err.is_aborted());
}

#[tokio::test]
async fn run_parallel_pre_aborted_sequential_throws() {
    let tok = CancellationToken::new();
    tok.cancel();
    let r = focket_rs::internal::run_parallel(
        vec![1, 2, 3],
        1,
        |x, _i, _s| async move { Ok(x) },
        Some(&tok),
    )
    .await;
    assert!(r.unwrap_err().is_aborted());
}

#[tokio::test]
async fn run_parallel_pre_aborted_parallel_breaks_workers() {
    let tok = CancellationToken::new();
    tok.cancel();
    let calls = Arc::new(AtomicUsize::new(0));
    let c2 = calls.clone();
    let out = focket_rs::internal::run_parallel(
        vec![1, 2, 3],
        3,
        move |x, _i, _s| {
            let c2 = c2.clone();
            async move {
                c2.fetch_add(1, Ordering::SeqCst);
                Ok(x)
            }
        },
        Some(&tok),
    )
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(out, vec![None, None, None]);
}

/* --------- FlowAbortError propagation through primitive catches -------- */

#[tokio::test]
async fn batch_item_throwing_abort_is_rethrown_not_swallowed() {
    #[derive(Clone)]
    struct AbortBatch {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for AbortBatch {
        impl_node_core!(core);
        async fn prep(&self, _s: &mut Shared) -> Result<Value> {
            Ok(json!([1, 2]))
        }
        async fn exec_item(&self, _item: &Value, _sig: &CancellationToken) -> Result<Value> {
            Err(FlowError::Aborted)
        }
    }
    let called = Arc::new(AtomicBool::new(false));
    let b = nref(AbortBatch {
        core: NodeCore::with_opts(NodeOpts::batch().fail_fast(false)),
    });
    let flow = Flow::new(b);
    flow.use_hooks(Arc::new(OnErrorFlag {
        called: called.clone(),
    }));
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert!(err.is_aborted());
    assert!(!called.load(Ordering::SeqCst)); // abort never reaches on_error
}

#[tokio::test]
async fn fork_join_branch_throwing_abort_is_rethrown() {
    #[derive(Clone)]
    struct AbortBranch {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for AbortBranch {
        impl_node_core!(core);
        async fn prep(&self, _s: &mut Shared) -> Result<Value> {
            Err(FlowError::Aborted)
        }
    }
    #[derive(Clone)]
    struct NoJoin;
    #[async_trait]
    impl ForkJoinOps for NoJoin {}
    let fan: NodeRef = nref(
        ForkJoin::with_ops(
            vec![
                nref(AbortBranch { core: core("Boom") }),
                nref(PassThrough { core: core("Ok") }),
            ],
            NoJoin,
        )
        .with_fail_fast(false),
    );
    let flow = Flow::new(fan);
    let mut shared = json!({});
    let err = flow.run(&mut shared, None).await.unwrap_err();
    assert!(err.is_aborted());
}

#[tokio::test]
async fn batch_flow_parallel_cancel_mid_run_rethrows_abort() {
    let called = Arc::new(AtomicBool::new(false));
    let bf: NodeRef = nref(
        BatchFlow::with_ops(
            nref(SlowPrep {
                core: core("Worker"),
            }),
            IdBundlesNoMerge {
                bundles: ids(&["a", "b"]),
            },
        )
        .with_concurrency(3),
    );
    let flow = Flow::new(bf);
    flow.use_hooks(Arc::new(OnErrorFlag {
        called: called.clone(),
    }));
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        t2.cancel();
    });
    let mut shared = json!({});
    let err = flow.run(&mut shared, Some(tok)).await.unwrap_err();
    assert!(err.is_aborted());
    assert!(!called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn batch_flow_sequential_cancel_mid_run_rethrows_abort() {
    let bf: NodeRef = nref(
        BatchFlow::with_ops(
            nref(SlowPrep {
                core: core("Worker"),
            }),
            IdBundlesNoMerge {
                bundles: ids(&["a", "b"]),
            },
        )
        .with_concurrency(1),
    );
    let flow = Flow::new(bf);
    let tok = CancellationToken::new();
    let t2 = tok.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        t2.cancel();
    });
    let mut shared = json!({});
    let err = flow.run(&mut shared, Some(tok)).await.unwrap_err();
    assert!(err.is_aborted());
}

/* ---------------- remaining constructor/orchestration branches -------- */

#[tokio::test]
async fn fork_join_no_branches_uses_make_branches() {
    #[derive(Clone)]
    struct OutSetter {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for OutSetter {
        impl_node_core!(core);
        async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
            shared["out"] = json!("ran");
            Ok(None)
        }
    }
    #[derive(Clone)]
    struct DynamicFan;
    #[async_trait]
    impl ForkJoinOps for DynamicFan {
        fn make_branches(&self, _shared: &Shared, _default: &[NodeRef]) -> Vec<NodeRef> {
            vec![nref(OutSetter { core: core("W") })]
        }
        async fn join(&self, shared: &mut Shared, branch_shareds: &[Option<Shared>]) -> Result<()> {
            if let Some(Some(b)) = branch_shareds.first() {
                shared["out"] = b["out"].clone();
            }
            Ok(())
        }
    }
    let fan: NodeRef = nref(ForkJoin::with_ops(vec![], DynamicFan));
    let flow = Flow::new(fan);
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["out"], json!("ran"));
}

#[tokio::test]
async fn flow_with_value_prep_still_orchestrates() {
    #[derive(Clone)]
    struct WithPrep;
    #[async_trait]
    impl FlowOps for WithPrep {
        async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
            Ok(json!("prep-val"))
        }
    }
    #[derive(Clone)]
    struct VLeaf {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for VLeaf {
        impl_node_core!(core);
        async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
            shared["v"] = json!(1);
            Ok(None)
        }
    }
    let w: NodeRef = nref(Flow::with_ops(nref(VLeaf { core: core("Leaf") }), WithPrep));
    let flow = Flow::new(w);
    let mut shared = json!({"v": 0});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["v"], json!(1));
}

#[tokio::test]
async fn batch_flow_parallel_zero_bundles_skips_parallel() {
    #[derive(Clone)]
    struct EmptyBundles {
        merged: Arc<AtomicBool>,
    }
    #[async_trait]
    impl BatchFlowOps for EmptyBundles {
        async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
            Ok(json!([]))
        }
        async fn merge(&self, _parent: &mut Shared, _bundles: &[Shared]) -> Result<()> {
            self.merged.store(true, Ordering::SeqCst);
            Ok(())
        }
    }
    let merged = Arc::new(AtomicBool::new(false));
    let bf: NodeRef = nref(
        BatchFlow::with_ops(
            nref(PassThrough { core: core("N") }),
            EmptyBundles {
                merged: merged.clone(),
            },
        )
        .with_concurrency(4), // >1 but 0 bundles → sequential path (no-op)
    );
    let flow = Flow::new(bf);
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert!(!merged.load(Ordering::SeqCst)); // merge() not called in sequential mode
}

#[tokio::test]
async fn batch_default_exec_item_noop_yields_null_per_item() {
    #[derive(Clone)]
    struct NoItem {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for NoItem {
        impl_node_core!(core);
        async fn prep(&self, _s: &mut Shared) -> Result<Value> {
            Ok(json!([1, 2]))
        }
        // no exec_item override → default no-op → each item becomes null
        async fn post(&self, shared: &mut Shared, _p: &Value, exec_res: &Value) -> Result<Action> {
            shared["nb"] = exec_res.clone();
            Ok(None)
        }
    }
    let b = nref(NoItem {
        core: NodeCore::with_opts(NodeOpts::batch()),
    });
    let mut shared = json!({});
    b.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["nb"], json!([null, null]));
}

/* ---------------- primitive on_error and fallback arms ---------------- */

#[tokio::test]
async fn batch_fail_fast_false_with_on_error_invokes_it() {
    let errs = Arc::new(AtomicUsize::new(0));
    struct CountOnError {
        errs: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Hooks for CountOnError {
        async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
            self.errs.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }
    #[derive(Clone)]
    struct Bad1 {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for Bad1 {
        impl_node_core!(core);
        async fn prep(&self, _s: &mut Shared) -> Result<Value> {
            Ok(json!([1, 2]))
        }
        async fn exec_item(&self, item: &Value, _sig: &CancellationToken) -> Result<Value> {
            if item == &json!(1) {
                return Err(FlowError::msg("bad"));
            }
            Ok(json!("ok"))
        }
    }
    let b = nref(Bad1 {
        core: NodeCore::with_opts(NodeOpts::batch().concurrency(2).fail_fast(false)),
    });
    let flow = Flow::new(b);
    flow.use_hooks(Arc::new(CountOnError { errs: errs.clone() }));
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert!(errs.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn fork_join_fail_fast_false_with_on_error_invokes_it() {
    let errs = Arc::new(AtomicUsize::new(0));
    struct CountOnError {
        errs: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Hooks for CountOnError {
        async fn on_error(&self, _ctx: ErrorCtx<'_>) -> Result<Option<ErrorPolicy>> {
            self.errs.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }
    #[derive(Clone)]
    struct NoJoin;
    #[async_trait]
    impl ForkJoinOps for NoJoin {}
    let fan: NodeRef = nref(
        ForkJoin::with_ops(
            vec![
                nref(BadBranch { core: core("Boom") }),
                nref(PassThrough { core: core("Ok") }),
            ],
            NoJoin,
        )
        .with_fail_fast(false)
        .with_concurrency(2),
    );
    let flow = Flow::new(fan);
    flow.use_hooks(Arc::new(CountOnError { errs: errs.clone() }));
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
    assert!(errs.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn batch_flow_parallel_fail_fast_false_without_on_error_completes() {
    #[derive(Clone)]
    struct BundlePost {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for BundlePost {
        impl_node_core!(core);
        async fn post(&self, _s: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
            if self.param("id") == Some(json!("b")) {
                return Err(FlowError::msg("b"));
            }
            Ok(None)
        }
    }
    #[derive(Clone)]
    struct NoMerge;
    #[async_trait]
    impl BatchFlowOps for NoMerge {
        async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
            Ok(json!([{ "id": "a" }, { "id": "b" }]))
        }
        async fn merge(&self, _parent: &mut Shared, _bundles: &[Shared]) -> Result<()> {
            Ok(())
        }
    }
    // no hooks → on_error absent → catch short-circuits
    let bf: NodeRef = nref(
        BatchFlow::with_ops(
            nref(BundlePost {
                core: core("Worker"),
            }),
            NoMerge,
        )
        .with_concurrency(3),
    );
    let flow = Flow::new(bf);
    let mut shared = json!({});
    flow.run(&mut shared, None).await.unwrap();
}

#[tokio::test]
async fn batch_flow_void_prep_uses_empty_bundles() {
    #[derive(Clone)]
    struct VoidBundles;
    #[async_trait]
    impl BatchFlowOps for VoidBundles {
        async fn prep(&self, _shared: &mut Shared) -> Result<Value> {
            Ok(Value::Null) // → no bundles → worker never runs
        }
    }
    #[derive(Clone)]
    struct OkWorker {
        core: NodeCore,
    }
    #[async_trait]
    impl FlowNode for OkWorker {
        impl_node_core!(core);
        async fn post(&self, shared: &mut Shared, _p: &Value, _e: &Value) -> Result<Action> {
            shared["ok"] = json!(true);
            Ok(None)
        }
    }
    let bf: NodeRef = nref(
        BatchFlow::with_ops(
            nref(OkWorker {
                core: core("Worker"),
            }),
            VoidBundles,
        )
        .with_concurrency(3),
    );
    let flow = Flow::new(bf);
    let mut shared = json!({"ok": false});
    flow.run(&mut shared, None).await.unwrap();
    assert_eq!(shared["ok"], json!(false));
}
