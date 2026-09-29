//! Task-scoped cancellation accounting for spawned asynchronous descendants.

use std::sync::Arc;

#[derive(Clone)]
struct TaskTracker {
    state: Arc<TaskTrackerState>,
}

struct TaskTrackerState {
    active: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
}

struct TrackedTask(TaskTracker);

pin_project_lite::pin_project! {
    /// One child future paired with the task-scope guard that accounts for it.
    ///
    /// Field order is load-bearing: cancelling even an unpolled Tokio task must destroy the child
    /// future and all of its captures before [`TrackedTask`] decrements the scope's active count.
    struct TrackedFuture<F> {
        #[pin]
        future: F,
        _tracked: TrackedTask,
    }
}

impl<F> std::future::Future for TrackedFuture<F>
where
    F: std::future::Future,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.project().future.poll(cx)
    }
}

impl TaskTracker {
    fn new() -> Self {
        Self {
            state: Arc::new(TaskTrackerState {
                active: std::sync::atomic::AtomicUsize::new(0),
                idle: tokio::sync::Notify::new(),
            }),
        }
    }

    fn enter(&self) -> TrackedTask {
        self.state
            .active
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        TrackedTask(self.clone())
    }

    async fn wait_idle(&self) {
        loop {
            let idle = self.state.idle.notified();
            if self.state.active.load(std::sync::atomic::Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }
}

impl Drop for TrackedTask {
    fn drop(&mut self) {
        if self
            .0
            .state
            .active
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel)
            == 1
        {
            self.0.state.idle.notify_one();
        }
    }
}

tokio::task_local! {
    static TASK_TRACKER: TaskTracker;
}

/// Runs an operation in a task scope and waits for cancelled descendants to finish dropping.
pub async fn scope_tasks<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    if TASK_TRACKER.try_with(Clone::clone).is_ok() {
        return future.await;
    }
    let tracker = TaskTracker::new();
    let output = TASK_TRACKER.scope(tracker.clone(), future).await;
    tracker.wait_idle().await;
    output
}

/// Spawns work tracked by the current task scope.
pub fn spawn_tracked<T, F>(join_set: &mut tokio::task::JoinSet<T>, future: F)
where
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
{
    let tracker = TASK_TRACKER
        .try_with(Clone::clone)
        .expect("tracked work must run inside a task scope");
    let tracked = tracker.enter();
    join_set.spawn(TASK_TRACKER.scope(
        tracker,
        TrackedFuture {
            future,
            _tracked: tracked,
        },
    ));
}
