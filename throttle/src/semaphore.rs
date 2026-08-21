use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub struct Semaphore {
    flag: AtomicBool,
    epoch: std::sync::RwLock<std::sync::Arc<Epoch>>,
    // per-interval replenish count, read by `run_replenish_thread` on every
    // iteration. Making this dynamic lets the congestion-control layer
    // adjust the token rate while the replenish loop is running.
    replenish: AtomicUsize,
}

struct Epoch {
    sem: std::sync::Arc<tokio::sync::Semaphore>,
    // current intended concurrency cap — tracked separately from the inner
    // tokio semaphore so `set_max` can perform delta-based adjustments
    // (add or forget permits) rather than a reset-and-add that would drift
    // against held permits.
    limit: AtomicUsize,
    // Outstanding shrink shortfall. When `set_max` reduces the cap but the
    // excess permits are held by outstanding acquirers, `forget_permits`
    // only takes from the available pool; the remainder is recorded here
    // and consumed by returning permits or by a newly granted raw permit before it can admit work,
    // so the effective in-flight count eventually converges to the new cap.
    forget_debt: AtomicUsize,
    // true while a shrink has removed available permits but may not yet have
    // published the corresponding shortfall debt.
    shrink_in_progress: AtomicBool,
    shrink_finished: tokio::sync::Notify,
}

impl Epoch {
    fn new(limit: usize) -> Self {
        Self {
            sem: std::sync::Arc::new(tokio::sync::Semaphore::new(limit)),
            limit: AtomicUsize::new(limit),
            forget_debt: AtomicUsize::new(0),
            shrink_in_progress: AtomicBool::new(false),
            shrink_finished: tokio::sync::Notify::new(),
        }
    }

    /// Apply a `delta`-permit shrink: forget what we can from the available
    /// pool, then accrue the remainder as `forget_debt` so outstanding
    /// permits are reclaimed on drop.
    fn record_shrink(&self, delta: usize) {
        self.record_shrink_inner(delta, || {}, || {});
    }

    fn record_shrink_inner(
        &self,
        delta: usize,
        after_forget: impl FnOnce(),
        after_clear: impl FnOnce(),
    ) {
        let already_shrinking = self.shrink_in_progress.swap(true, Ordering::AcqRel);
        debug_assert!(
            !already_shrinking,
            "overlapping shrink operations bypassed epoch configuration serialization"
        );
        let forgotten = self.sem.forget_permits(delta);
        after_forget();
        let shortfall = delta.saturating_sub(forgotten);
        if shortfall > 0 {
            self.forget_debt.fetch_add(shortfall, Ordering::AcqRel);
        }
        self.shrink_in_progress.store(false, Ordering::Release);
        after_clear();
        self.shrink_finished.notify_waiters();
    }

    async fn wait_for_shrink_to_finish(&self) {
        while self.shrink_in_progress.load(Ordering::Acquire) {
            // construct the future before rechecking the gate so notify_waiters cannot be lost
            // between observing an active shrink and awaiting its completion.
            let finished = self.shrink_finished.notified();
            if self.shrink_in_progress.load(Ordering::Acquire) {
                finished.await;
            }
        }
    }

    /// Pay one unit of outstanding shrink debt, if present.
    ///
    /// This is shared by wrapped permit drops and newly granted raw Tokio permits. The latter is
    /// necessary because cancelling a waiter after Tokio assigns it a permit returns that permit
    /// directly to the inner semaphore, bypassing [`Permit::drop`]. A subsequent acquire must
    /// retire that capacity instead of admitting work above the shrunken cap.
    fn pay_one_forget_debt(&self) -> bool {
        let mut debt = self.forget_debt.load(Ordering::Acquire);
        while debt > 0 {
            match self.forget_debt.compare_exchange_weak(
                debt,
                debt - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => debt = actual,
            }
        }
        false
    }

    /// Apply growth first to pending shrink debt and return only the capacity still to add.
    fn cancel_forget_debt(&self, growth: usize) -> usize {
        let mut debt = self.forget_debt.load(Ordering::Acquire);
        loop {
            let cancelled = debt.min(growth);
            if cancelled == 0 {
                return growth;
            }
            match self.forget_debt.compare_exchange_weak(
                debt,
                debt - cancelled,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return growth - cancelled,
                Err(actual) => debt = actual,
            }
        }
    }
}

/// RAII guard wrapping a tokio semaphore permit. On drop, if its epoch has outstanding shrink
/// debt, the permit is forgotten rather than released into that epoch's pool.
pub struct Permit {
    epoch: std::sync::Arc<Epoch>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        if self.epoch.pay_one_forget_debt() {
            permit.forget();
        } else {
            // no debt: return the permit to this epoch normally.
            drop(permit);
        }
    }
}

impl Semaphore {
    pub fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            epoch: std::sync::RwLock::new(std::sync::Arc::new(Epoch::new(0))),
            replenish: AtomicUsize::new(0),
        }
    }

    fn current_epoch(&self) -> std::sync::Arc<Epoch> {
        self.epoch
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn enabled_epoch(&self) -> Option<std::sync::Arc<Epoch>> {
        let epoch = self
            .epoch
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.flag.load(Ordering::Acquire).then(|| epoch.clone())
    }

    /// Establish a fresh startup/test configuration.
    ///
    /// Outstanding permits and queued waiters remain attached to the retired epoch. Closing it
    /// wakes queued waiters so they can observe the fresh configuration.
    pub fn setup(&self, value: usize) {
        let next = std::sync::Arc::new(Epoch::new(value));
        let mut current = self
            .epoch
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let retired = std::mem::replace(&mut *current, next);
        self.flag.store(value > 0, Ordering::Release);
        retired.sem.close();
    }

    /// Update the concurrency cap dynamically.
    ///
    /// Adjusts by delta from the current limit: if `value` is larger, new
    /// permits are added; if smaller, available permits are forgotten
    /// first and any shortfall (because permits are held by outstanding
    /// acquirers) is recorded as `forget_debt`. Returning permits consume that debt instead of
    /// re-entering the pool; a raw permit returned by a cancelled Tokio waiter is likewise retired
    /// by the next acquire before it can admit work. Growth first cancels pending debt, then adds
    /// only its remaining delta. Together these rules keep the effective in-flight count converging
    /// to `value` without overshooting a later growth target.
    pub fn set_max(&self, value: usize) {
        self.set_max_inner(value, || {});
    }

    fn set_max_inner(&self, value: usize, after_epoch_lock: impl FnOnce()) {
        if value == 0 {
            let next = std::sync::Arc::new(Epoch::new(0));
            let mut current = self
                .epoch
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            after_epoch_lock();
            let retired = std::mem::replace(&mut *current, next);
            self.flag.store(false, Ordering::Release);
            retired.sem.close();
            return;
        }
        let epoch = self
            .epoch
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        after_epoch_lock();
        let current = epoch.limit.swap(value, Ordering::AcqRel);
        // enable or adjust: apply the permit delta before flipping the
        // flag to true, so a 0 → N transition never lets a concurrent
        // acquire see `flag == true` with zero permits.
        match value.cmp(&current) {
            std::cmp::Ordering::Greater => {
                let added = epoch.cancel_forget_debt(value - current);
                if added > 0 {
                    epoch.sem.add_permits(added);
                }
            }
            std::cmp::Ordering::Less => {
                epoch.record_shrink(current - value);
            }
            std::cmp::Ordering::Equal => {}
        }
        self.flag.store(true, Ordering::Release);
    }

    /// Disable this semaphore without adjusting the cap. Intended for
    /// rate-throttle semantics where "no limit" means future `consume()`
    /// calls become no-ops rather than pausing token replenishment. A caller
    /// already parked in the current epoch can still receive a replenished
    /// token.
    pub fn disable(&self) {
        self.flag.store(false, Ordering::Release);
    }

    /// Re-enable this semaphore after [`disable`], so `consume` / `acquire`
    /// once again wait on the inner pool. Requires that the current epoch has
    /// a nonzero configured limit — otherwise there are no permits for callers
    /// to wait on, and flipping the flag would strand them. Returns `true` if
    /// the flag was flipped on, `false` if the current limit is zero.
    pub fn enable(&self) -> bool {
        let epoch = self
            .epoch
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if epoch.limit.load(Ordering::Acquire) == 0 {
            return false;
        }
        self.flag.store(true, Ordering::Release);
        true
    }

    /// Return the currently-configured cap. Intended for metrics and tests
    /// that want to observe the most recent `set_max` / `setup` value
    /// without having to probe the inner semaphore.
    pub fn current_limit(&self) -> usize {
        self.current_epoch().limit.load(Ordering::Acquire)
    }

    /// Update the per-interval replenish count. Takes effect on the next
    /// iteration of `run_replenish_thread` without restarting the loop.
    pub fn set_replenish(&self, value: usize) {
        self.replenish.store(value, Ordering::Release);
    }

    pub async fn acquire(&self) -> Option<Permit> {
        loop {
            let epoch = self.enabled_epoch()?;
            match epoch.sem.clone().acquire_owned().await {
                Ok(permit) => {
                    epoch.wait_for_shrink_to_finish().await;
                    if epoch.pay_one_forget_debt() {
                        permit.forget();
                        continue;
                    }
                    return Some(Permit {
                        epoch,
                        permit: Some(permit),
                    });
                }
                Err(_) => continue,
            }
        }
    }

    pub async fn consume(&self) {
        loop {
            let Some(epoch) = self.enabled_epoch() else {
                return;
            };
            match epoch.sem.clone().acquire_owned().await {
                Ok(permit) => {
                    permit.forget();
                    return;
                }
                Err(_) => continue,
            }
        }
    }

    pub async fn consume_many(&self, value: u32) {
        loop {
            let Some(epoch) = self.enabled_epoch() else {
                return;
            };
            match epoch.sem.clone().acquire_many_owned(value).await {
                Ok(permit) => {
                    permit.forget();
                    return;
                }
                Err(_) => continue,
            }
        }
    }

    pub async fn run_replenish_thread(&self, replenish: usize, interval: std::time::Duration) {
        // No early-return on `flag == false`: the auto-meta bootstrap path
        // spawns this thread and then immediately calls `disable()` so the
        // adapter can enable rate capping later via
        // [`crate::enable_ops_throttle`]. If the thread exited on `!flag`
        // here it would race that sequence and die before it ever looped.
        // With `replenish == 0`, each iteration is a no-op, so running
        // the loop while disabled is cheap. A nonzero replenish can still
        // wake callers that parked before disable; otherwise it leaves
        // permits available for a later enable.
        self.replenish.store(replenish, Ordering::Release);
        loop {
            tokio::time::sleep(interval).await;
            let replenish = self.replenish.load(Ordering::Acquire);
            if replenish == 0 {
                continue;
            }
            let epoch = self.current_epoch();
            let curr_permits = epoch.sem.available_permits();
            if curr_permits >= replenish {
                continue;
            }
            epoch.sem.add_permits(replenish - curr_permits);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    /// Yield repeatedly so a just-woken task has a chance to actually run its
    /// loop body before the next assertion. A single `yield_now` is often
    /// not enough when the task has to progress through several `await`
    /// points between wake-up and the observable state change.
    async fn let_spawned_task_run() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    fn available_permits(sem: &Semaphore) -> usize {
        sem.current_epoch().sem.available_permits()
    }

    fn forget_debt(sem: &Semaphore) -> usize {
        sem.current_epoch().forget_debt.load(Ordering::Acquire)
    }

    fn poll_once<F: Future>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        future.poll(&mut context)
    }

    #[tokio::test]
    async fn set_max_delta_grows_and_shrinks_available_permits() {
        let sem = Semaphore::new();
        sem.set_max(10);
        assert_eq!(available_permits(&sem), 10);
        sem.set_max(15);
        assert_eq!(available_permits(&sem), 15);
        sem.set_max(3);
        assert_eq!(available_permits(&sem), 3);
    }

    #[test]
    fn set_max_holds_exclusive_epoch_configuration_access() {
        let sem = Semaphore::new();
        sem.setup(2);
        sem.set_max_inner(1, || {
            assert!(
                matches!(
                    sem.epoch.try_read(),
                    Err(std::sync::TryLockError::WouldBlock)
                ),
                "a concurrent set_max caller could enter the same epoch update"
            );
        });
        assert_eq!(sem.current_limit(), 1);
    }

    #[tokio::test]
    async fn setup_does_not_accept_a_permit_from_an_older_configuration() {
        let sem = Semaphore::new();
        sem.setup(1);
        let old = sem.acquire().await.unwrap();
        sem.setup(1);
        let current = sem.acquire().await.unwrap();
        drop(old);
        assert_eq!(
            available_permits(&sem),
            0,
            "an old permit inflated the newly configured capacity"
        );
        drop(current);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), sem.acquire())
                .await
                .is_ok(),
            "the current epoch's permit did not return normally"
        );
    }

    #[tokio::test]
    async fn retired_permit_does_not_consume_current_epoch_shrink_debt() {
        let sem = Semaphore::new();
        sem.setup(2);
        let old = sem.acquire().await.unwrap();
        sem.setup(2);
        let current_one = sem.acquire().await.unwrap();
        let current_two = sem.acquire().await.unwrap();
        sem.set_max(1);
        assert_eq!(forget_debt(&sem), 1);
        drop(old);
        assert_eq!(
            forget_debt(&sem),
            1,
            "a retired permit consumed shrink debt from the current epoch"
        );
        drop(current_one);
        assert_eq!(forget_debt(&sem), 0);
        drop(current_two);
        let only = sem.acquire().await.unwrap();
        assert_eq!(
            available_permits(&sem),
            0,
            "the shrunk current epoch admitted more than one permit"
        );
        drop(only);
    }

    #[tokio::test]
    async fn waiter_crossing_setup_does_not_shrink_fresh_pool() {
        let sem = Semaphore::new();
        sem.setup(1);
        let held = sem.acquire().await.unwrap();
        let mut waiter = std::pin::pin!(sem.acquire());
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        assert!(waiter.as_mut().poll(&mut context).is_pending());
        sem.setup(1);
        drop(held);
        let crossing = tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
            .await
            .expect("registered waiter wakes after setup")
            .expect("fresh setup remains enabled");
        drop(crossing);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), sem.acquire())
                .await
                .expect("crossing waiter returns its fresh permit")
                .is_some()
        );
    }

    #[tokio::test]
    async fn setup_zero_wakes_parked_waiter_to_none() {
        let sem = Semaphore::new();
        sem.setup(1);
        let held = sem.acquire().await.unwrap();
        let mut waiter = std::pin::pin!(sem.acquire());
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        assert!(waiter.as_mut().poll(&mut context).is_pending());
        sem.setup(0);
        drop(held);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
                .await
                .expect("setup wakes registered waiter")
                .is_none()
        );
    }

    #[tokio::test]
    async fn set_max_zero_wakes_parked_waiter_to_none() {
        let sem = Semaphore::new();
        sem.set_max(1);
        let held = sem.acquire().await.unwrap();
        let mut waiter = std::pin::pin!(sem.acquire());
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        assert!(waiter.as_mut().poll(&mut context).is_pending());
        sem.set_max(0);
        drop(held);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
                .await
                .expect("zero cap wakes registered waiter")
                .is_none()
        );
    }

    #[tokio::test]
    async fn consume_many_drains_exactly_n_permits() {
        let sem = Semaphore::new();
        sem.set_max(10);
        sem.consume_many(3).await;
        assert_eq!(available_permits(&sem), 7);
        sem.consume_many(0).await;
        assert_eq!(available_permits(&sem), 7, "consuming zero is a no-op");
    }

    #[tokio::test]
    async fn consume_many_is_a_noop_while_disabled() {
        let sem = Semaphore::new();
        sem.set_max(10);
        sem.disable();
        sem.consume_many(5).await;
        assert_eq!(
            available_permits(&sem),
            10,
            "a disabled semaphore must not drain permits"
        );
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
        assert_eq!(available_permits(&sem), 2);
        // shrink from 5 to 1: we need to remove 4 permits, but only 2 are
        // available. The other 2 are recorded as forget_debt and consumed
        // by the next two drops.
        sem.set_max(1);
        assert_eq!(available_permits(&sem), 0);
        assert_eq!(forget_debt(&sem), 2);
        drop(g1);
        assert_eq!(forget_debt(&sem), 1);
        assert_eq!(available_permits(&sem), 0);
        drop(g2);
        assert_eq!(forget_debt(&sem), 0);
        assert_eq!(available_permits(&sem), 0);
        // debt is now 0; the third drop returns its permit to the pool,
        // giving us steady-state of exactly 1 — the new cap.
        drop(g3);
        assert_eq!(available_permits(&sem), 1);
    }

    #[tokio::test]
    async fn set_max_zero_while_held_revokes_permits_on_drop() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.set_max(3);
        let retired = sem.current_epoch();
        let g1 = sem.acquire().await.unwrap();
        let g2 = sem.acquire().await.unwrap();
        let g3 = sem.acquire().await.unwrap();
        sem.set_max(0);
        assert!(retired.sem.is_closed());
        drop(g1);
        drop(g2);
        drop(g3);
        assert_eq!(available_permits(&sem), 0);
        assert_eq!(forget_debt(&sem), 0);
    }

    #[tokio::test]
    async fn set_max_growth_cancels_pending_shrink_debt() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.set_max(5);
        let g1 = sem.acquire().await.unwrap();
        let g2 = sem.acquire().await.unwrap();
        let g3 = sem.acquire().await.unwrap();
        // shrink to 1 — leaves 2 units of debt pending.
        sem.set_max(1);
        assert_eq!(forget_debt(&sem), 2);
        // grow back to 5 while debt is pending. Two units of growth cancel
        // the two units of debt; only the remaining two become immediately
        // available, so the three held permits plus two new permits equal 5.
        sem.set_max(5);
        assert_eq!(available_permits(&sem), 2);
        assert_eq!(forget_debt(&sem), 0);
        let g4 = sem.acquire().await.unwrap();
        let g5 = sem.acquire().await.unwrap();
        let mut sixth = Box::pin(sem.acquire());
        let sixth_was_pending = poll_once(sixth.as_mut()).is_pending();
        drop(g1);
        drop(g2);
        drop(g3);
        drop(g4);
        drop(g5);
        drop(sixth);
        assert_eq!(available_permits(&sem), 5);
        assert!(
            sixth_was_pending,
            "growth exposed more operations than the new cap while old permits were held"
        );
    }

    #[tokio::test]
    async fn cancelled_woken_waiter_cannot_bypass_pending_shrink() {
        let sem = Semaphore::new();
        sem.set_max(2);
        let g1 = sem.acquire().await.unwrap();
        let g2 = sem.acquire().await.unwrap();
        let mut assigned_waiter = Box::pin(sem.acquire());
        assert!(poll_once(assigned_waiter.as_mut()).is_pending());

        // tokio assigns g1's returned raw permit to this queued waiter. Shrinking before that
        // waiter is polled records one unit of debt because neither permit is in the available
        // pool. Cancelling the waiter then returns its assigned permit directly to Tokio, bypassing
        // our Permit::drop boundary.
        drop(g1);
        sem.set_max(1);
        assert_eq!(forget_debt(&sem), 1);
        drop(assigned_waiter);

        let mut fresh = Box::pin(sem.acquire());
        let (fresh_was_pending, fresh_guard) = match poll_once(fresh.as_mut()) {
            std::task::Poll::Pending => (true, None),
            std::task::Poll::Ready(guard) => (false, guard),
        };
        drop(g2);
        let fresh_guard = match fresh_guard {
            Some(guard) => guard,
            None => fresh.await.expect("nonzero cap remains enabled"),
        };
        drop(fresh_guard);
        assert_eq!(available_permits(&sem), 1);
        assert!(
            fresh_was_pending,
            "a cancelled Tokio waiter returned a permit that bypassed pending shrink debt"
        );
    }

    #[tokio::test]
    async fn acquire_during_shrink_waits_for_debt_publication() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.set_max(2);
        let returned_during_shrink = sem.acquire().await.unwrap();
        let held = sem.acquire().await.unwrap();
        let after_forget = std::sync::Arc::new(std::sync::Barrier::new(2));
        let publish_debt = std::sync::Arc::new(std::sync::Barrier::new(2));
        let epoch = sem.current_epoch();
        let shrink_after_forget = after_forget.clone();
        let shrink_publish_debt = publish_debt.clone();
        let shrink = std::thread::spawn(move || {
            // mirror set_max's limit update before entering the shrink operation.
            epoch.limit.store(1, Ordering::Release);
            epoch.record_shrink_inner(
                1,
                || {
                    shrink_after_forget.wait();
                    shrink_publish_debt.wait();
                },
                || {},
            );
        });
        after_forget.wait();
        drop(returned_during_shrink);
        let mut fresh = Box::pin(sem.acquire());
        let (fresh_was_pending, fresh_guard) = match poll_once(fresh.as_mut()) {
            std::task::Poll::Pending => (true, None),
            std::task::Poll::Ready(guard) => (false, guard),
        };
        publish_debt.wait();
        shrink.join().expect("shrink thread completes");
        assert!(
            fresh_was_pending,
            "a raw permit returned during shrink admitted replacement work before debt publication"
        );
        drop(fresh_guard);
        assert!(
            poll_once(fresh.as_mut()).is_pending(),
            "the replacement acquire did not retire shrink debt before admitting work"
        );
        assert_eq!(forget_debt(&sem), 0);
        assert_eq!(available_permits(&sem), 0);
        drop(held);
        let fresh_guard = tokio::time::timeout(std::time::Duration::from_secs(1), fresh)
            .await
            .expect("the replacement acquire wakes after the allowed operation exits")
            .expect("the nonzero cap remains enabled");
        drop(fresh_guard);
        assert_eq!(available_permits(&sem), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shrink_completion_wakes_acquire_after_debt_publication() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.set_max(2);
        let returned_during_shrink = sem.acquire().await.unwrap();
        let held = sem.acquire().await.unwrap();
        let after_forget = std::sync::Arc::new(std::sync::Barrier::new(2));
        let publish_debt = std::sync::Arc::new(std::sync::Barrier::new(2));
        let after_clear = std::sync::Arc::new(std::sync::Barrier::new(2));
        let allow_notify = std::sync::Arc::new(std::sync::Barrier::new(2));
        let waiter_polls = std::sync::Arc::new(AtomicUsize::new(0));
        let epoch = sem.current_epoch();
        let shrink_after_forget = after_forget.clone();
        let shrink_publish_debt = publish_debt.clone();
        let shrink_after_clear = after_clear.clone();
        let shrink_allow_notify = allow_notify.clone();
        let shrink_waiter_polls = waiter_polls.clone();
        let shrink_gate = epoch.clone();
        let shrink = std::thread::spawn(move || {
            epoch.limit.store(1, Ordering::Release);
            epoch.record_shrink_inner(
                1,
                || {
                    shrink_after_forget.wait();
                    shrink_publish_debt.wait();
                },
                || {
                    // if notification is ever moved before the gate-clear store, make the woken
                    // waiter deterministically repoll while the gate is still raised. The normal
                    // order observes false here and never spins.
                    while shrink_gate.shrink_in_progress.load(Ordering::Acquire)
                        && shrink_waiter_polls.load(Ordering::Acquire) < 2
                    {
                        std::thread::yield_now();
                    }
                    shrink_after_clear.wait();
                    shrink_allow_notify.wait();
                },
            );
        });

        after_forget.wait();
        drop(returned_during_shrink);
        let (first_poll_tx, first_poll_rx) = tokio::sync::oneshot::channel();
        let (second_poll_tx, mut second_poll_rx) = tokio::sync::oneshot::channel();
        let waiter_sem = sem.clone();
        let waiter_poll_count = waiter_polls.clone();
        let waiter = tokio::spawn(async move {
            let mut acquire = Box::pin(waiter_sem.acquire());
            let mut first_poll_tx = Some(first_poll_tx);
            let mut second_poll_tx = Some(second_poll_tx);
            std::future::poll_fn(|cx| {
                let result = acquire.as_mut().poll(cx);
                waiter_poll_count.fetch_add(1, Ordering::AcqRel);
                if let Some(first_poll_tx) = first_poll_tx.take() {
                    let _ = first_poll_tx.send(());
                } else if let Some(second_poll_tx) = second_poll_tx.take() {
                    let _ = second_poll_tx.send(());
                }
                result
            })
            .await
        });
        first_poll_rx
            .await
            .expect("the replacement acquire did not park behind the shrink gate");

        publish_debt.wait();
        after_clear.wait();
        let progressed_before_announcement = second_poll_rx.try_recv().is_ok();
        allow_notify.wait();
        shrink.join().expect("shrink thread completes");
        if progressed_before_announcement {
            waiter.abort();
            match waiter.await {
                Err(error) if error.is_cancelled() => {}
                _ => panic!("the replacement acquire did not stop after abort"),
            }
            drop(held);
            panic!("the replacement acquire progressed before shrink completion was announced");
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), second_poll_rx)
            .await
            .expect("the shrink completion notification did not wake the replacement acquire")
            .expect("the replacement acquire ended before reporting its wake-up");
        assert_eq!(forget_debt(&sem), 0);

        drop(held);
        let guard = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("the replacement acquire did not finish after the allowed operation exited")
            .expect("the replacement acquire task panicked")
            .expect("the nonzero cap remains enabled");
        drop(guard);
        assert_eq!(available_permits(&sem), 1);
    }

    #[tokio::test]
    async fn disable_flips_flag_without_clearing_pool() {
        let sem = Semaphore::new();
        sem.setup(3);
        assert_eq!(available_permits(&sem), 3);
        sem.disable();
        // consume is now a no-op; pool is untouched.
        sem.consume().await;
        assert_eq!(available_permits(&sem), 3);
    }

    #[tokio::test]
    async fn enable_after_disable_restores_gating() {
        let sem = Semaphore::new();
        sem.setup(2);
        sem.disable();
        // gate is open — consume drains nothing.
        sem.consume().await;
        sem.consume().await;
        assert_eq!(available_permits(&sem), 2);
        // flip the flag back on: consume now actually drains tokens.
        assert!(sem.enable());
        sem.consume().await;
        sem.consume().await;
        assert_eq!(available_permits(&sem), 0);
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
            available_permits(&sem),
            15,
            "expected final cap (15), got {} — forget_debt accounting drifted",
            available_permits(&sem),
        );
        assert_eq!(
            forget_debt(&sem),
            0,
            "debt must be fully consumed once all permits have returned",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn replenish_thread_survives_disable_spawn_enable_cycle() {
        // Regression for the auto-meta bootstrap path: setup + disable +
        // spawn replenish thread + later enable + set_replenish. The
        // thread was previously exiting immediately on `!flag` (losing
        // its replenish loop) which defeated the whole point of
        // bootstrapping the ops-throttle for a later rate decision.
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.setup(1);
        sem.disable();
        // spawn the thread AFTER disable — the exact order the auto-meta
        // bootstrap uses in production.
        let sem2 = sem.clone();
        let handle = tokio::spawn(async move {
            sem2.run_replenish_thread(0, std::time::Duration::from_millis(100))
                .await;
        });
        let_spawned_task_run().await;
        // enable + set a rate — the thread must still be alive to
        // respond.
        assert!(sem.enable());
        sem.set_replenish(5);
        // drain anything that was in the pool to force a refill
        while available_permits(&sem) > 0 {
            sem.consume().await;
        }
        tokio::time::advance(std::time::Duration::from_millis(150)).await;
        let_spawned_task_run().await;
        assert_eq!(
            available_permits(&sem),
            5,
            "thread did not refill after the disable-then-enable cycle",
        );
        handle.abort();
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
        assert_eq!(available_permits(&sem), 3);
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
        while available_permits(&sem) > 0 {
            sem.consume().await;
        }
        // first refill at the initial rate
        tokio::time::advance(std::time::Duration::from_millis(150)).await;
        let_spawned_task_run().await;
        assert_eq!(available_permits(&sem), 5);
        // bump the rate; drain; next refill uses the new value
        sem.set_replenish(10);
        while available_permits(&sem) > 0 {
            sem.consume().await;
        }
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        let_spawned_task_run().await;
        assert_eq!(available_permits(&sem), 10);
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
        while available_permits(&sem) > 0 {
            sem.consume().await;
        }
        // first refill happens at the initial rate
        tokio::time::advance(std::time::Duration::from_millis(150)).await;
        let_spawned_task_run().await;
        assert_eq!(available_permits(&sem), 4);
        // setting rate to zero keeps the loop alive but stops adding permits
        sem.set_replenish(0);
        while available_permits(&sem) > 0 {
            sem.consume().await;
        }
        tokio::time::advance(std::time::Duration::from_millis(300)).await;
        let_spawned_task_run().await;
        assert_eq!(available_permits(&sem), 0);
        // restoring the rate resumes refills
        sem.set_replenish(4);
        tokio::time::advance(std::time::Duration::from_millis(150)).await;
        let_spawned_task_run().await;
        assert_eq!(available_permits(&sem), 4);
        handle.abort();
    }
}
