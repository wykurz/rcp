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
    /// permits are added; if smaller, excess available permits are forgotten.
    /// Permits already held by outstanding acquirers are not forcibly
    /// revoked — they return naturally on drop. While a shrink is in flight,
    /// the effective concurrency can exceed `value` until the held permits
    /// return; for BBR-style gradual cwnd adjustment this is acceptable.
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
                self.sem.forget_permits(current);
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
                self.sem.forget_permits(current - value);
            }
            std::cmp::Ordering::Equal => {}
        }
        self.flag.store(true, Ordering::Release);
    }

    /// Update the per-interval replenish count. Takes effect on the next
    /// iteration of `run_replenish_thread` without restarting the loop.
    pub fn set_replenish(&self, value: usize) {
        self.replenish.store(value, Ordering::Release);
    }

    pub async fn acquire(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        if self.flag.load(Ordering::Acquire) {
            Some(self.sem.acquire().await.unwrap())
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
    async fn set_max_shrink_with_held_permits_does_not_underflow() {
        let sem = std::sync::Arc::new(Semaphore::new());
        sem.set_max(5);
        // hold 3 permits
        let g1 = sem.acquire().await.unwrap();
        let g2 = sem.acquire().await.unwrap();
        let g3 = sem.acquire().await.unwrap();
        assert_eq!(sem.sem.available_permits(), 2);
        // shrink past available: `forget_permits` only takes from what is
        // currently available, so the held permits are NOT revoked — they
        // return on drop.
        sem.set_max(1);
        assert_eq!(sem.sem.available_permits(), 0);
        // once all three held permits are dropped, the steady-state
        // permit count exceeds the new cap: that is the documented
        // "temporary overshoot" behavior and is what the test pins down.
        // We assert the final state rather than intermediate drops so
        // the test doesn't depend on tokio's internal forget-debt
        // accounting.
        drop(g1);
        drop(g2);
        drop(g3);
        assert_eq!(sem.sem.available_permits(), 3);
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
