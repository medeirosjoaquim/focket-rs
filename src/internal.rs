//! Engine internals: cancellation/timeout helpers, bounded parallelism,
//! isolation, and the shared retry loop. Exposed (like TS's `_internal`) so
//! they can be unit-tested in isolation; not part of the stable public surface.

use futures::future::join_all;
use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::error::FlowError;
use crate::hooks::fire_on_retry;
use crate::types::*;

/// Create a token that is cancelled when `parent` is (linked), and can also be
/// cancelled directly (used by `fail_fast` to cancel in-flight siblings).
/// Dropping the child detaches it, so no cleanup is needed.
pub fn link_signal(parent: Option<&CancellationToken>) -> CancellationToken {
    match parent {
        Some(p) => p.child_token(),
        None => CancellationToken::new(),
    }
}

/// Resolve with `work`'s output, or fail with [`FlowError::Aborted`] if
/// `signal` is cancelled. The `work` future is dropped (cancelled) on abort.
pub async fn race_abort<T, F>(work: F, signal: &CancellationToken) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    if signal.is_cancelled() {
        return Err(FlowError::Aborted);
    }
    tokio::select! {
        biased; // a ready work result wins over a simultaneous cancel
        r = work => r,
        _ = signal.cancelled() => Err(FlowError::Aborted),
    }
}

/// Race `work` against a per-node timeout (`timeout_ms`) and/or cancellation.
/// The first to settle wins: timeout → [`FlowError::Timeout`]; cancel →
/// [`FlowError::Aborted`]; otherwise `work`'s output. The losing future is
/// dropped, so a timed-out node can never run a late `post()`.
pub async fn race_work<T, F>(work: F, timeout_ms: u64, signal: &CancellationToken) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    if signal.is_cancelled() {
        return Err(FlowError::Aborted);
    }
    if timeout_ms == 0 {
        tokio::select! {
            biased;
            r = work => r,
            _ = signal.cancelled() => Err(FlowError::Aborted),
        }
    } else {
        tokio::select! {
            biased;
            r = work => r,
            _ = sleep(Duration::from_millis(timeout_ms)) => Err(FlowError::timeout(timeout_ms)),
            _ = signal.cancelled() => Err(FlowError::Aborted),
        }
    }
}

/// Run an async mapper over `items` with bounded `concurrency`, preserving
/// order, with **cancellation**: each in-flight item races a linked child of
/// `parent`, and on any failure the linked token is cancelled so siblings stop
/// (the `fail_fast` contract made honest). `concurrency <= 1` runs
/// sequentially; `> 1` runs in parallel (p-limit style).
///
/// Returns one slot per item; a slot is `None` only if its worker never ran
/// (e.g. a pre-cancelled token in parallel mode). On sequential mode a failure
/// propagates immediately.
pub async fn run_parallel<T, R, F, Fut>(
    items: Vec<T>,
    concurrency: usize,
    f: F,
    parent: Option<&CancellationToken>,
) -> Result<Vec<Option<R>>>
where
    F: Fn(T, usize, CancellationToken) -> Fut + Send + Sync,
    Fut: Future<Output = Result<R>> + Send,
    T: Clone + Send + Sync,
    R: Send,
{
    let n = items.len();
    if n == 0 {
        return Ok(vec![]);
    }
    let linked = link_signal(parent);

    if concurrency <= 1 {
        let mut out: Vec<Option<R>> = Vec::with_capacity(n);
        for (i, item) in items.into_iter().enumerate() {
            if linked.is_cancelled() {
                return Err(FlowError::Aborted);
            }
            out.push(Some(race_abort(f(item, i, linked.clone()), &linked).await?));
        }
        return Ok(out);
    }

    let out: Vec<Mutex<Option<R>>> = (0..n).map(|_| Mutex::new(None)).collect();
    let cursor = AtomicUsize::new(0);
    let first_err: Mutex<Option<FlowError>> = Mutex::new(None);
    let worker_count = concurrency.min(n).max(1);

    let mut workers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        workers.push(async {
            loop {
                if linked.is_cancelled() {
                    break;
                }
                let i = cursor.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    break;
                }
                match race_abort(f(items[i].clone(), i, linked.clone()), &linked).await {
                    Ok(v) => {
                        *out[i].lock().unwrap() = Some(v);
                    }
                    Err(e) => {
                        {
                            let mut fe = first_err.lock().unwrap();
                            if fe.is_none() {
                                *fe = Some(e);
                            }
                        }
                        linked.cancel(); // cancel in-flight siblings (failFast) / honor cancel
                        break;
                    }
                }
            }
        });
    }
    join_all(workers).await;

    if let Some(e) = first_err.lock().unwrap().take() {
        return Err(e);
    }
    Ok(out.into_iter().map(|m| m.into_inner().unwrap()).collect())
}

/// Produce an isolated copy of `shared` for a parallel branch / bundle /
/// sub-flow. Default: deep clone (a `Value` clone is the exact analogue of
/// TS's `structuredClone`). Pass a [`SharedCloner`] for a custom strategy.
pub fn isolated(shared: &Shared, cloner: Option<&SharedCloner>) -> Shared {
    match cloner {
        Some(c) => c(shared),
        None => shared.clone(),
    }
}

/// The shared retry loop used by single-item and batch execution.
/// `max_retries` is the number of retries **after** the first attempt.
/// Cancellation ([`FlowError::Aborted`]) is **never** retried — it propagates
/// immediately so a cancel doesn't burn the retry budget. After the final
/// failure, `fallback` decides the outcome (default fallbacks rethrow).
#[allow(clippy::too_many_arguments)]
pub async fn retrying<T, F, Fut, FB, FutB>(
    node: &NodeRef,
    max_retries: u32,
    wait_ms: u64,
    mut run: F,
    fallback: FB,
    hooks: &[HookRef],
    shared: &Shared,
    path: &[String],
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
    FB: FnOnce(FlowError) -> FutB,
    FutB: Future<Output = Result<T>>,
{
    let mut last_error: Option<FlowError> = None;
    for attempt in 0..=max_retries {
        match run().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if e.is_aborted() {
                    return Err(e); // cancel: don't retry, propagate
                }
                let is_last = attempt == max_retries;
                last_error = Some(e);
                if is_last {
                    break;
                }
                fire_on_retry(
                    hooks,
                    node,
                    shared,
                    path,
                    last_error.as_ref().unwrap(),
                    attempt,
                    wait_ms,
                )
                .await?;
                if wait_ms > 0 {
                    sleep(Duration::from_millis(wait_ms)).await;
                }
            }
        }
    }
    fallback(last_error.unwrap()).await
}
