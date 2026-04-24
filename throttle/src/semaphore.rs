use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub struct Semaphore {
    flag: std::sync::Arc<AtomicBool>,
    sem: tokio::sync::Semaphore,
    // per-interval replenish count, read by `run_replenish_thread` on every
    // iteration. Making this dynamic lets the congestion-control layer
    // adjust the token rate while the replenish loop is running.
    replenish: AtomicUsize,
    // current intended concurrency cap — tracked separately from the inner
    // tokio semaphore so `set_max` can perform delta-based adjustments
    // (add or forget permits) rather than a reset-and-add that would drift
    // against held permits.
    limit: AtomicUsize,
    // Outstanding shrink shortfall. When `set_max` reduces the cap but the
    // excess permits are held by outstanding acquirers, `forget_permits`
    // only takes from the available pool; the remainder is recorded here
    // and consumed by the next N permit drops (see [`Permit::drop`]) so
    // the effective in-flight count eventually converges to the new cap.
    forget_debt: AtomicUsize,
}

/// RAII guard wrapping a tokio semaphore permit. On drop, if the semaphore
/// has outstanding `forget_debt` from a prior shrink, this permit is
/// forgotten (removed from the pool) rather than released; otherwise it
/// returns to the pool normally.
pub struct Permit<'a> {
    inner: Option<PermitInner<'a>>,
}

struct PermitInner<'a> {
    sem: &'a Semaphore,
    permit: tokio::sync::SemaphorePermit<'a>,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        // Consume one unit of forget_debt if any is outstanding. We use a
        // CAS loop so concurrent drops race cleanly — at most `debt` of
        // them will successfully decrement and forget their permit; the
        // rest return to the pool normally.
        let mut debt = inner.sem.forget_debt.load(Ordering::Acquire);
        while debt > 0 {
            match inner.sem.forget_debt.compare_exchange_weak(
                debt,
                debt - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    inner.permit.forget();
                    return;
                }
                Err(actual) => debt = actual,
            }
        }
        // debt == 0: let `permit` drop normally, returning to the pool.
        drop(inner.permit);
    }
}

impl Semaphore {
    pub fn new() -> Self {
        let flag = std::sync::Arc::new(AtomicBool::new(false));
        // initialize with zero permits so `set_max` can cleanly add the
        // first batch without having to first forget an arbitrary baseline.
        // Callers that go through the `flag`-guarded API never observe the
        // zero state: acquire returns None while flag is false, so nothing
        // blocks on the semaphore before setup.
        let sem = tokio::sync::Semaphore::const_new(0);
        Self {
            flag,
            sem,
            replenish: AtomicUsize::new(0),
            limit: AtomicUsize::new(0),
            forget_debt: AtomicUsize::new(0),
        }
    }

    pub fn setup(&self, value: usize) {
        // temporarily disable while reconfiguring so a concurrent acquire
        // cannot observe `flag == true` with an empty semaphore (the
        // permit-free window between `forget_permits` and `add_permits`).
        // A caller racing this reconfiguration will see the semaphore as
        // disabled and acquire a no-op permit — acceptable since setup is
        // normally a startup operation.
        self.flag.store(false, Ordering::Release);
        self.sem.forget_permits(self.sem.available_permits());
        self.forget_debt.store(0, Ordering::Release);
        self.limit.store(value, Ordering::Release);
        if value == 0 {
            return;
        }
        self.sem.add_permits(value);
        // flip to enabled only after permits are in place.
        self.flag.store(true, Ordering::Release);
    }

    /// Update the concurrency cap dynamically.
    ///
    /// Adjusts by delta from the current limit: if `value` is larger, new
    /// permits are added; if smaller, available permits are forgotten
    /// first and any shortfall (because permits are held by outstanding
    /// acquirers) is recorded as `forget_debt`. The next N permit drops
    /// will consume that debt — being forgotten rather than returned to
    /// the pool — so the effective in-flight count converges to `value`.
    ///
    /// **Threading:** assumes a single writer (the congestion-control
    /// adapter task). Concurrent callers can race the `limit.swap` and
    /// compute incorrect deltas against each other. Callers that need
    /// multi-writer access must wrap `set_max` in an external lock.
    ///
    /// **Limitation:** `set_max(0)` flips the cap off for *new* `acquire`
    /// calls but does not wake tasks already suspended inside
    /// `acquire().await`. They remain parked until a permit becomes
    /// available. Callers that require a cancellable disable should use a
    /// higher-level shutdown signal; the adaptive controller does not rely
    /// on zero transitions in practice (its minimum cwnd is configured
    /// `>= 1`).
    pub fn set_max(&self, value: usize) {
        let current = self.limit.swap(value, Ordering::AcqRel);
        if value == 0 {
            // disable: flip the flag before forgetting permits so new
            // acquires observe the disabled state and return None instead
            // of blocking on a now-empty semaphore.
            self.flag.store(false, Ordering::Release);
            if current > 0 {
                self.record_shrink(current);
            }
            return;
        }
        // enable or adjust: apply the permit delta before flipping the
        // flag to true, so a 0 → N transition never lets a concurrent
        // acquire see `flag == true` with zero permits.
        match value.cmp(&current) {
            std::cmp::Ordering::Greater => {
                self.sem.add_permits(value - current);
            }
            std::cmp::Ordering::Less => {
                self.record_shrink(current - value);
            }
            std::cmp::Ordering::Equal => {}
        }
        self.flag.store(true, Ordering::Release);
    }

    /// Apply a `delta`-permit shrink: forget what we can from the available
    /// pool, then accrue the remainder as `forget_debt` so outstanding
    /// permits are reclaimed on drop.
    fn record_shrink(&self, delta: usize) {
        let forgotten = self.sem.forget_permits(delta);
        let shortfall = delta.saturating_sub(forgotten);
        if shortfall > 0 {
            self.forget_debt.fetch_add(shortfall, Ordering::AcqRel);
        }
    }

    /// Disable this semaphore without adjusting the cap. Intended for
    /// rate-throttle semantics where "no limit" means `consume()` becomes
    /// a no-op rather than pausing token replenishment.
    pub fn disable(&self) {
        self.flag.store(false, Ordering::Release);
    }

    /// Re-enable this semaphore after [`disable`], so `consume` / `acquire`
    /// once again wait on the inner pool. Requires that the semaphore was
    /// previously configured (via [`setup`] or [`set_max`]) with a non-zero
    /// value — otherwise there are no permits for callers to wait on, and
    /// flipping the flag would strand them. Returns `true` if the flag was
    /// flipped on, `false` if there is no prior configuration to enable.
    pub fn enable(&self) -> bool {
        if self.limit.load(Ordering::Acquire) == 0 {
            return false;
        }
        self.flag.store(true, Ordering::Release);
        true
    }

    /// Return the currently-configured cap. Intended for metrics and tests
    /// that want to observe the most recent `set_max` / `setup` value
    /// without having to probe the inner semaphore.
    pub fn current_limit(&self) -> usize {
        self.limit.load(Ordering::Acquire)
    }

    /// Update the per-interval replenish count. Takes effect on the next
    /// iteration of `run_replenish_thread` without restarting the loop.
    pub fn set_replenish(&self, value: usize) {
        self.replenish.store(value, Ordering::Release);
    }

    pub async fn acquire(&self) -> Option<Permit<'_>> {
        if self.flag.load(Ordering::Acquire) {
            let permit = self.sem.acquire().await.unwrap();
            Some(Permit {
                inner: Some(PermitInner { sem: self, permit }),
            })
        } else {
            None
        }
    }

    pub async fn consume(&self) {
        if self.flag.load(Ordering::Acquire) {
            self.sem.acquire().await.unwrap().forget();
        }
    }

    pub async fn consume_many(&self, value: u32) {
        if self.flag.load(Ordering::Acquire) {
            self.sem.acquire_many(value).await.unwrap().forget();
        }
    }

    pub async fn run_replenish_thread(&self, replenish: usize, interval: std::time::Duration) {
        if !self.flag.load(Ordering::Acquire) {
            return;
        }
        self.replenish.store(replenish, Ordering::Release);
        loop {
            tokio::time::sleep(interval).await;
            let replenish = self.replenish.load(Ordering::Acquire);
            if replenish == 0 {
                continue;
            }
            let curr_permits = self.sem.available_permits();
            if curr_permits >= replenish {
                continue;
            }
            self.sem.add_permits(replenish - curr_permits);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Yield repeatedly so a just-woken task has a chance to actually run its
    /// loop body before the next assertion. A single `yield_now` is often
    /// not enough when the task has to progress through several `await`
    /// points between wake-up and the observable state change.
    async fn let_spawned_task_run() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn set_max_delta_grows_and_shrinks_available_permits() {
        let sem = Semaphore::new();
        sem.set_max(10);
        assert_eq!(sem.sem.available_permits(), 10);
        sem.set_max(15);
        assert_eq!(sem.sem.available_permits(), 15);
        sem.set_max(3);
        assert_eq!(sem.sem.available_permits(), 3);
    }

    #[tokio::test]
    async fn set_max_to_zero_disables_acquires() {
        let sem = Semaphore::new();
        sem.set_max(4);
        // active: acquire returns a permit
        assert!(sem.acquire().await.is_some());
        sem.set_max(0);
        // disabled: acquire returns None immediately, no blocking
        assert!(sem.acquire().await.is_none());
    }

    #[tokio::test]
    async fn set_max_shrink_converges_via_forget_debt() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.set_max(5);
        // hold 3 permits — leaves 2 available in the pool.
        let g1 = sem.acquire().await.unwrap();
        let g2 = sem.acquire().await.unwrap();
        let g3 = sem.acquire().await.unwrap();
        assert_eq!(sem.sem.available_permits(), 2);
        // shrink from 5 to 1: we need to remove 4 permits, but only 2 are
        // available. The other 2 are recorded as forget_debt and consumed
        // by the next two drops.
        sem.set_max(1);
        assert_eq!(sem.sem.available_permits(), 0);
        assert_eq!(sem.forget_debt.load(Ordering::Acquire), 2);
        drop(g1);
        assert_eq!(sem.forget_debt.load(Ordering::Acquire), 1);
        assert_eq!(sem.sem.available_permits(), 0);
        drop(g2);
        assert_eq!(sem.forget_debt.load(Ordering::Acquire), 0);
        assert_eq!(sem.sem.available_permits(), 0);
        // debt is now 0; the third drop returns its permit to the pool,
        // giving us steady-state of exactly 1 — the new cap.
        drop(g3);
        assert_eq!(sem.sem.available_permits(), 1);
    }

    #[tokio::test]
    async fn set_max_zero_while_held_revokes_permits_on_drop() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.set_max(3);
        let g1 = sem.acquire().await.unwrap();
        let g2 = sem.acquire().await.unwrap();
        let g3 = sem.acquire().await.unwrap();
        // no available permits; set_max(0) records full debt.
        sem.set_max(0);
        assert_eq!(sem.forget_debt.load(Ordering::Acquire), 3);
        drop(g1);
        drop(g2);
        drop(g3);
        // all three permits consumed by debt — none back in the pool.
        assert_eq!(sem.sem.available_permits(), 0);
        assert_eq!(sem.forget_debt.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn set_max_grow_during_pending_debt_settles_to_new_cap() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.set_max(5);
        let g1 = sem.acquire().await.unwrap();
        let g2 = sem.acquire().await.unwrap();
        let g3 = sem.acquire().await.unwrap();
        // shrink to 1 — leaves 2 units of debt pending.
        sem.set_max(1);
        assert_eq!(sem.forget_debt.load(Ordering::Acquire), 2);
        // grow back to 5 while debt still pending. The pool gains
        // (5 - 1) = 4 permits; debt stays the same and will still be
        // consumed by drops.
        sem.set_max(5);
        assert_eq!(sem.sem.available_permits(), 4);
        assert_eq!(sem.forget_debt.load(Ordering::Acquire), 2);
        // drops: first two consume debt; third returns to pool.
        drop(g1);
        drop(g2);
        drop(g3);
        // steady state: pool has 4 (from regrow) + 1 (from g3) = 5 = new cap.
        assert_eq!(sem.sem.available_permits(), 5);
    }

    #[tokio::test]
    async fn disable_flips_flag_without_clearing_pool() {
        let sem = Semaphore::new();
        sem.setup(3);
        assert_eq!(sem.sem.available_permits(), 3);
        sem.disable();
        // consume is now a no-op; pool is untouched.
        sem.consume().await;
        assert_eq!(sem.sem.available_permits(), 3);
    }

    #[tokio::test]
    async fn enable_after_disable_restores_gating() {
        let sem = Semaphore::new();
        sem.setup(2);
        sem.disable();
        // gate is open — consume drains nothing.
        sem.consume().await;
        sem.consume().await;
        assert_eq!(sem.sem.available_permits(), 2);
        // flip the flag back on: consume now actually drains tokens.
        assert!(sem.enable());
        sem.consume().await;
        sem.consume().await;
        assert_eq!(sem.sem.available_permits(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_set_max_and_permit_drops_converge() {
        // Stress test: many workers concurrently acquire+hold+drop permits
        // while the test thread issues a sequence of set_max calls that
        // shrink the cap below the current held count. The CAS loop in
        // Permit::drop must race cleanly so the final state matches the
        // last cap, not some drift from debt accounting bugs.
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.set_max(50);
        let workers = 50;
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let sem = sem.clone();
            handles.push(tokio::spawn(async move {
                // acquire, hold briefly, drop — repeat a few times.
                for _ in 0..10 {
                    if let Some(guard) = sem.acquire().await {
                        // tiny yield so set_max has a chance to interleave
                        // while we hold the permit.
                        tokio::task::yield_now().await;
                        drop(guard);
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }
        // meanwhile, shrink and grow the cap across the workers' lifetime.
        for target in [10, 40, 5, 30, 1, 20].iter().copied() {
            tokio::task::yield_now().await;
            sem.set_max(target);
        }
        // settle on a final cap and let workers finish.
        sem.set_max(15);
        for h in handles {
            h.await.expect("worker completes");
        }
        // After all workers complete and settle, the semaphore's
        // available_permits must equal the final cap: every permit either
        // returned to the pool or was consumed by forget_debt on drop.
        // No drift, no leak.
        assert_eq!(
            sem.sem.available_permits(),
            15,
            "expected final cap (15), got {} — forget_debt accounting drifted",
            sem.sem.available_permits(),
        );
        assert_eq!(
            sem.forget_debt.load(Ordering::Acquire),
            0,
            "debt must be fully consumed once all permits have returned",
        );
    }

    #[tokio::test]
    async fn enable_without_setup_is_noop() {
        // A semaphore that was never configured (setup/set_max not called)
        // has no permits; flipping the flag on would strand any caller
        // that arrived via acquire/consume. enable() refuses and reports
        // false so the caller can detect the "unconfigured" state.
        let sem = Semaphore::new();
        assert!(!sem.enable());
        // flag should still be false — acquire returns None immediately.
        assert!(sem.acquire().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn replenish_thread_tops_up_to_initial_value() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.setup(3);
        sem.consume().await;
        sem.consume().await;
        sem.consume().await;
        // bucket is empty; kick off the replenish loop
        let sem2 = sem.clone();
        let handle = tokio::spawn(async move {
            sem2.run_replenish_thread(3, std::time::Duration::from_millis(100))
                .await;
        });
        // let the spawned task run to the first `sleep` before advancing time
        let_spawned_task_run().await;
        tokio::time::advance(std::time::Duration::from_millis(150)).await;
        // and yield back so the wake-up runs the body that adds permits
        let_spawned_task_run().await;
        assert_eq!(sem.sem.available_permits(), 3);
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn set_replenish_takes_effect_on_next_iteration() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.setup(5);
        let sem2 = sem.clone();
        let handle = tokio::spawn(async move {
            sem2.run_replenish_thread(5, std::time::Duration::from_millis(100))
                .await;
        });
        // let the task reach its first sleep and initialize the replenish
        // atomic before we touch it from the test thread.
        let_spawned_task_run().await;
        // drain now — after spawn, so the upcoming refill has work to do.
        while sem.sem.available_permits() > 0 {
            sem.consume().await;
        }
        // first refill at the initial rate
        tokio::time::advance(std::time::Duration::from_millis(150)).await;
        let_spawned_task_run().await;
        assert_eq!(sem.sem.available_permits(), 5);
        // bump the rate; drain; next refill uses the new value
        sem.set_replenish(10);
        while sem.sem.available_permits() > 0 {
            sem.consume().await;
        }
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        let_spawned_task_run().await;
        assert_eq!(sem.sem.available_permits(), 10);
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn set_replenish_to_zero_pauses_refills() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.setup(4);
        let sem2 = sem.clone();
        let handle = tokio::spawn(async move {
            sem2.run_replenish_thread(4, std::time::Duration::from_millis(100))
                .await;
        });
        let_spawned_task_run().await;
        while sem.sem.available_permits() > 0 {
            sem.consume().await;
        }
        // first refill happens at the initial rate
        tokio::time::advance(std::time::Duration::from_millis(150)).await;
        let_spawned_task_run().await;
        assert_eq!(sem.sem.available_permits(), 4);
        // setting rate to zero keeps the loop alive but stops adding permits
        sem.set_replenish(0);
        while sem.sem.available_permits() > 0 {
            sem.consume().await;
        }
        tokio::time::advance(std::time::Duration::from_millis(300)).await;
        let_spawned_task_run().await;
        assert_eq!(sem.sem.available_permits(), 0);
        // restoring the rate resumes refills
        sem.set_replenish(4);
        tokio::time::advance(std::time::Duration::from_millis(150)).await;
        let_spawned_task_run().await;
        assert_eq!(sem.sem.available_permits(), 4);
        handle.abort();
    }
}
