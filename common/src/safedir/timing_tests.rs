use super::*;
use anyhow::Context as _;
use tracing::instrument::WithSubscriber as _;
use tracing_subscriber::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Started(&'static str),
    Completed(&'static str, bool),
}

#[derive(Clone, Default)]
struct Events(Arc<std::sync::Mutex<Vec<Event>>>);

impl<S> tracing_subscriber::Layer<S> for Events
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        _: &tracing::Id,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.0
            .lock()
            .unwrap()
            .push(Event::Started(attributes.metadata().name()));
    }
    fn on_record(
        &self,
        id: &tracing::Id,
        values: &tracing::span::Record<'_>,
        context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        #[derive(Default)]
        struct Completion(Option<bool>);
        impl tracing::field::Visit for Completion {
            fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                if field.name() == "timing_finished" {
                    self.0 = Some(value);
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        let mut completion = Completion::default();
        values.record(&mut completion);
        if let Some(finished) = completion.0 {
            self.0.lock().unwrap().push(Event::Completed(
                context.span(id).unwrap().metadata().name(),
                finished,
            ));
        }
    }
}

struct Capture {
    dispatch: tracing::Dispatch,
    guard: crate::timing::TimingGuard,
    file: tempfile::NamedTempFile,
    events: Events,
}

impl Capture {
    fn new(detail: bool) -> anyhow::Result<Self> {
        let file = tempfile::NamedTempFile::new()?;
        let (layer, guard) =
            crate::timing::TimingLayer::new("metadata-test".into(), Some(file.reopen()?), None);
        let events = Events::default();
        let subscriber = tracing_subscriber::registry().with(
            layer
                .and_then(events.clone())
                .with_filter(crate::timing::timing_filter(detail)),
        );
        Ok(Self {
            dispatch: tracing::Dispatch::new(subscriber),
            guard,
            file,
            events,
        })
    }
    fn events(&self) -> Vec<Event> {
        self.events.0.lock().unwrap().clone()
    }
    fn report(mut self) -> anyhow::Result<serde_json::Value> {
        self.guard.finish()?;
        Ok(serde_json::from_reader(self.file.reopen()?)?)
    }
}

fn runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()?)
}

fn assert_sample(report: &serde_json::Value, name: &str, finished: u64, interrupted: u64) {
    let scope = report["scopes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|scope| scope["name"] == name)
        .unwrap_or_else(|| panic!("missing timing scope {name}: {report}"));
    assert_eq!(scope["count"], finished + interrupted, "{name}");
    assert_eq!(scope["finished"], finished, "{name}");
    assert_eq!(scope["interrupted"], interrupted, "{name}");
}

#[test]
fn metadata_timings_distinguish_side_operation_and_phase() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let capture = Capture::new(true)?;
        let directory = tempfile::tempdir()?;
        let source_path = directory.path().to_owned();
        run_metadata_probed_blocking(
            congestion::Side::Source,
            congestion::MetadataOp::Stat,
            move || std::fs::metadata(source_path),
        )
        .with_subscriber(capture.dispatch.clone())
        .await?;
        let destination_path = directory.path().join("created");
        run_metadata_probed_blocking(
            congestion::Side::Destination,
            congestion::MetadataOp::OpenCreate,
            move || std::fs::File::create(destination_path),
        )
        .with_subscriber(capture.dispatch.clone())
        .await?;
        assert!(directory.path().join("created").is_file());
        let report = capture.report()?;
        for name in [
            "source.metadata.stat.wait_rate",
            "source.metadata.stat.wait_admission",
            "source.metadata.stat.wait_worker",
            "source.metadata.stat.execute",
            "destination.metadata.open-create.wait_rate",
            "destination.metadata.open-create.wait_admission",
            "destination.metadata.open-create.wait_worker",
            "destination.metadata.open-create.execute",
        ] {
            assert_sample(&report, name, 1, 0);
        }
        assert_eq!(report["scopes"].as_array().unwrap().len(), 8);
        Ok(())
    })
}

#[test]
fn metadata_timings_require_detail_enablement() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let capture = Capture::new(false)?;
        let file = tempfile::tempfile()?;
        run_metadata_probed_blocking(
            congestion::Side::Source,
            congestion::MetadataOp::Stat,
            move || file.metadata(),
        )
        .with_subscriber(capture.dispatch.clone())
        .await?;
        assert!(capture.report()?["scopes"].as_array().unwrap().is_empty());
        Ok(())
    })
}

#[test]
fn metadata_admission_wait_ends_before_worker_submission() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let limits = crate::testutils::AdmissionLimit::new().await;
        let resource = throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
        limits.set_max_ops_in_flight(resource, 1);
        let occupied = throttle::ops_in_flight_permit(resource).await;
        let capture = Capture::new(true)?;
        let file = tempfile::tempfile()?;
        let mut work = Box::pin(
            run_metadata_probed_blocking_no_rate(
                congestion::Side::Source,
                congestion::MetadataOp::Stat,
                move || file.metadata(),
            )
            .with_subscriber(capture.dispatch.clone()),
        );
        assert!(futures::poll!(work.as_mut()).is_pending());
        let waiting = capture.events();
        drop(occupied);
        work.await?;
        assert_eq!(
            waiting,
            vec![Event::Started("source.metadata.stat.wait_admission")],
            "blocked admission must not count as blocking-pool queueing"
        );
        let report = capture.report()?;
        assert_sample(&report, "source.metadata.stat.wait_admission", 1, 0);
        assert_sample(&report, "source.metadata.stat.wait_worker", 1, 0);
        assert_sample(&report, "source.metadata.stat.execute", 1, 0);
        assert_eq!(report["scopes"].as_array().unwrap().len(), 3);
        Ok(())
    })
}

#[test]
fn cancelled_queued_metadata_has_no_execution_sample() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let limits = crate::testutils::AdmissionLimit::new().await;
        limits.set_files_in_flight(1);
        let samples = Arc::new(congestion::testing::CollectingSink::new());
        congestion::install_sample_sink(samples.clone());
        let guard = throttle::open_file_permit().await;
        let admission = guard.admission();
        let capture = Capture::new(true)?;
        let file = tempfile::tempfile()?;
        let identity = crate::testutils::FdIdentityProbe::capture(file.as_raw_fd())?;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
        });
        started_rx.await?;
        let (work_started_tx, work_started_rx) = tokio::sync::oneshot::channel();
        let mut work = Box::pin(
            with_fd_admission(admission, async move {
                run_metadata_probed_blocking(
                    congestion::Side::Source,
                    congestion::MetadataOp::Stat,
                    move || {
                        let _ = work_started_tx.send(());
                        file.metadata()
                    },
                )
                .await
            })
            .with_subscriber(capture.dispatch.clone()),
        );
        assert!(futures::poll!(work.as_mut()).is_pending());
        let queued = capture.events();
        drop(guard);
        drop(work);
        let closed_before_release = identity.original_is_closed();
        let mut next_permit = Box::pin(throttle::open_file_permit());
        let capacity_returned = futures::poll!(next_permit.as_mut()).is_ready();
        drop(release_tx);
        blocker.await?;
        assert!(work_started_rx.await.is_err(), "cancelled work ran");
        assert!(closed_before_release?, "queued fd survived cancellation");
        assert!(capacity_returned, "queued cancellation retained admission");
        assert_eq!(samples.metadata_count(), 0);
        assert!(queued.contains(&Event::Started("source.metadata.stat.wait_worker")));
        assert!(!queued.contains(&Event::Started("source.metadata.stat.execute")));
        let report = capture.report()?;
        assert_sample(&report, "source.metadata.stat.wait_worker", 0, 1);
        assert!(
            report["scopes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|scope| scope["name"] != "source.metadata.stat.execute")
        );
        Ok(())
    })
}

#[test]
fn cancelled_running_metadata_times_actual_worker_lifetime() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let limits = crate::testutils::AdmissionLimit::new().await;
        limits.set_files_in_flight(1);
        let resource = throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
        limits.set_max_ops_in_flight(resource, 1);
        let samples = Arc::new(congestion::testing::CollectingSink::new());
        congestion::install_sample_sink(samples.clone());
        let guard = throttle::open_file_permit().await;
        let admission = guard.admission();
        let file = tempfile::tempfile()?;
        let identity = crate::testutils::FdIdentityProbe::capture(file.as_raw_fd())?;
        let capture = Capture::new(true)?;
        let (blocker_started_tx, blocker_started_rx) = tokio::sync::oneshot::channel();
        let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = blocker_started_tx.send(());
            let _ = release_blocker_rx.recv();
        });
        blocker_started_rx.await?;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let mut work = Box::pin(
            with_fd_admission(admission, async move {
                let _guard = guard;
                run_metadata_probed_blocking_no_rate(
                    congestion::Side::Source,
                    congestion::MetadataOp::Stat,
                    move || {
                        let _ = started_tx.send(());
                        let _ = release_rx.recv();
                        file.metadata()
                    },
                )
                .await
            })
            .with_subscriber(capture.dispatch.clone()),
        );
        assert!(futures::poll!(work.as_mut()).is_pending());
        // the congestion sample must already be running while the work is queued
        let queued_at = std::time::Instant::now();
        drop(release_blocker_tx);
        blocker.await?;
        started_rx.await.context("metadata worker did not start")?;
        drop(work);
        let cancelled_at = std::time::Instant::now();
        let after_cancel = capture.events();
        let fd_still_open = !identity.original_is_closed()?;
        let mut next_file = Box::pin(throttle::open_file_permit());
        let file_retained = futures::poll!(next_file.as_mut()).is_pending();
        let mut next_op = Box::pin(throttle::ops_in_flight_permit(resource));
        let operation_retained = futures::poll!(next_op.as_mut()).is_pending();
        drop(release_tx);
        // this barrier runs after the detached job on the sole blocking worker
        tokio::task::spawn_blocking(|| ()).await?;
        assert!(fd_still_open && file_retained && operation_retained);
        assert!(identity.original_is_closed()?);
        assert!(futures::poll!(next_file.as_mut()).is_ready());
        assert!(futures::poll!(next_op.as_mut()).is_ready());
        let samples = samples.metadata_samples();
        assert_eq!(samples.len(), 1);
        assert!(samples[0].started_at <= queued_at);
        assert!(samples[0].completed_at >= cancelled_at);
        assert!(after_cancel.contains(&Event::Completed("source.metadata.stat.wait_worker", true)));
        assert!(after_cancel.contains(&Event::Started("source.metadata.stat.execute")));
        assert!(
            !after_cancel
                .iter()
                .any(|event| matches!(event, Event::Completed("source.metadata.stat.execute", _)))
        );
        let report = capture.report()?;
        assert_sample(&report, "source.metadata.stat.wait_worker", 1, 0);
        assert_sample(&report, "source.metadata.stat.execute", 1, 0);
        Ok(())
    })
}

#[test]
fn metadata_errors_finish_execution_but_panics_interrupt_it() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let _limits = crate::testutils::AdmissionLimit::new().await;
        let samples = Arc::new(congestion::testing::CollectingSink::new());
        congestion::install_sample_sink(samples.clone());
        let capture = Capture::new(true)?;
        let error = run_metadata_probed_blocking_no_rate(
            congestion::Side::Destination,
            congestion::MetadataOp::OpenCreate,
            || Err::<(), _>(std::io::Error::from_raw_os_error(libc::EACCES)),
        )
        .with_subscriber(capture.dispatch.clone())
        .await
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EACCES));
        let error = run_metadata_probed_blocking_no_rate(
            congestion::Side::Destination,
            congestion::MetadataOp::OpenCreate,
            || -> std::io::Result<()> { panic!("test metadata worker panic") },
        )
        .with_subscriber(capture.dispatch.clone())
        .await
        .unwrap_err();
        assert!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<tokio::task::JoinError>())
                .is_some_and(tokio::task::JoinError::is_panic)
        );
        assert_eq!(samples.metadata_count(), 0);
        let report = capture.report()?;
        assert_sample(
            &report,
            "destination.metadata.open-create.wait_admission",
            2,
            0,
        );
        assert_sample(
            &report,
            "destination.metadata.open-create.wait_worker",
            2,
            0,
        );
        assert_sample(&report, "destination.metadata.open-create.execute", 1, 1);
        Ok(())
    })
}

#[test]
fn async_metadata_reports_success_and_error_without_worker_queue() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let capture = Capture::new(true)?;
        let directory = tempfile::tempdir()?;
        let metadata = crate::walk::run_metadata_probed(
            congestion::Side::Source,
            congestion::MetadataOp::Stat,
            tokio::fs::metadata(directory.path()),
        )
        .with_subscriber(capture.dispatch.clone())
        .await?;
        assert!(metadata.is_dir());
        let error = crate::walk::run_metadata_probed(
            congestion::Side::Destination,
            congestion::MetadataOp::OpenCreate,
            tokio::fs::File::create(directory.path().join("missing/file")),
        )
        .with_subscriber(capture.dispatch.clone())
        .await
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        let report = capture.report()?;
        for operation in ["source.metadata.stat", "destination.metadata.open-create"] {
            for phase in ["wait_rate", "wait_admission", "execute"] {
                assert_sample(&report, &format!("{operation}.{phase}"), 1, 0);
            }
        }
        assert_eq!(report["scopes"].as_array().unwrap().len(), 6);
        Ok(())
    })
}

#[test]
fn async_metadata_cancelled_during_admission_never_executes() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let limits = crate::testutils::AdmissionLimit::new().await;
        let resource = throttle::Resource::meta(
            throttle::Side::Destination,
            throttle::MetadataOp::OpenCreate,
        );
        limits.set_max_ops_in_flight(resource, 1);
        let held = throttle::ops_in_flight_permit(resource).await;
        let directory = tempfile::tempdir()?;
        let destination = directory.path().join("never-created");
        let capture = Capture::new(true)?;
        let mut work = Box::pin(
            crate::walk::run_metadata_probed_no_rate(
                congestion::Side::Destination,
                congestion::MetadataOp::OpenCreate,
                tokio::fs::File::create(&destination),
            )
            .with_subscriber(capture.dispatch.clone()),
        );
        assert!(futures::poll!(work.as_mut()).is_pending());
        drop(work);
        drop(held);
        assert!(!destination.exists());
        let report = capture.report()?;
        assert_sample(
            &report,
            "destination.metadata.open-create.wait_admission",
            0,
            1,
        );
        assert_eq!(report["scopes"].as_array().unwrap().len(), 1);
        Ok(())
    })
}

#[test]
fn async_metadata_execution_spans_suspension_and_cancellation() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let limits = crate::testutils::AdmissionLimit::new().await;
        let resource = throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
        limits.set_max_ops_in_flight(resource, 1);
        let samples = Arc::new(congestion::testing::CollectingSink::new());
        congestion::install_sample_sink(samples.clone());
        let capture = Capture::new(true)?;
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let mut work = Box::pin(
            crate::walk::run_metadata_probed_no_rate(
                congestion::Side::Source,
                congestion::MetadataOp::Stat,
                async { released.await.map_err(std::io::Error::other) },
            )
            .with_subscriber(capture.dispatch.clone()),
        );
        assert!(futures::poll!(work.as_mut()).is_pending());
        let suspended = capture.events();
        let mut next = Box::pin(throttle::ops_in_flight_permit(resource));
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(work);
        assert!(futures::poll!(next.as_mut()).is_ready());
        assert_eq!(
            samples.metadata_count(),
            0,
            "cancelled operation must not publish a success probe"
        );
        assert!(release.send(()).is_err());
        assert!(suspended.contains(&Event::Started("source.metadata.stat.execute")));
        assert!(
            !suspended
                .iter()
                .any(|event| matches!(event, Event::Completed("source.metadata.stat.execute", _)))
        );
        let report = capture.report()?;
        assert_sample(&report, "source.metadata.stat.wait_admission", 1, 0);
        assert_sample(&report, "source.metadata.stat.execute", 0, 1);
        assert_eq!(report["scopes"].as_array().unwrap().len(), 2);
        Ok(())
    })
}

#[test]
fn async_metadata_panic_interrupts_execution_and_releases_admission() -> anyhow::Result<()> {
    use futures::FutureExt as _;
    runtime()?.block_on(async {
        let limits = crate::testutils::AdmissionLimit::new().await;
        let resource = throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
        limits.set_max_ops_in_flight(resource, 1);
        let capture = Capture::new(true)?;
        let work = crate::walk::run_metadata_probed_no_rate(
            congestion::Side::Source,
            congestion::MetadataOp::Stat,
            async {
                panic!("async metadata panic");
                #[allow(unreachable_code)]
                std::io::Result::Ok(())
            },
        )
        .with_subscriber(capture.dispatch.clone());
        assert!(
            std::panic::AssertUnwindSafe(work)
                .catch_unwind()
                .await
                .is_err()
        );
        assert!(
            throttle::ops_in_flight_permit(resource)
                .now_or_never()
                .is_some()
        );
        let report = capture.report()?;
        assert_sample(&report, "source.metadata.stat.wait_admission", 1, 0);
        assert_sample(&report, "source.metadata.stat.execute", 0, 1);
        assert_eq!(report["scopes"].as_array().unwrap().len(), 2);
        Ok(())
    })
}

#[test]
fn async_metadata_timings_require_detail_enablement() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let capture = Capture::new(false)?;
        let directory = tempfile::tempdir()?;
        let metadata = crate::walk::run_metadata_probed(
            congestion::Side::Source,
            congestion::MetadataOp::Stat,
            tokio::fs::metadata(directory.path()),
        )
        .with_subscriber(capture.dispatch.clone())
        .await?;
        assert!(metadata.is_dir());
        assert!(capture.report()?["scopes"].as_array().unwrap().is_empty());
        Ok(())
    })
}

#[test]
fn blocking_execution_finishes_after_probe_and_operation_release() -> anyhow::Result<()> {
    use futures::FutureExt as _;
    struct Completion {
        samples: Arc<congestion::testing::CollectingSink>,
        observed: Arc<std::sync::Mutex<Vec<(usize, bool)>>>,
        resource: throttle::Resource,
    }
    impl<S> tracing_subscriber::Layer<S> for Completion
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_record(
            &self,
            id: &tracing::Id,
            values: &tracing::span::Record<'_>,
            context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Finished(bool);
            impl tracing::field::Visit for Finished {
                fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                    if field.name() == "timing_finished" {
                        self.0 = value;
                    }
                }
                fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            }
            let mut finished = Finished(false);
            values.record(&mut finished);
            if finished.0
                && context.span(id).unwrap().metadata().name() == "source.metadata.stat.execute"
            {
                let released = throttle::ops_in_flight_permit(self.resource)
                    .now_or_never()
                    .is_some();
                self.observed
                    .lock()
                    .unwrap()
                    .push((self.samples.metadata_count(), released));
            }
        }
    }
    runtime()?.block_on(async {
        let limits = crate::testutils::AdmissionLimit::new().await;
        let resource = throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
        limits.set_max_ops_in_flight(resource, 1);
        let samples = Arc::new(congestion::testing::CollectingSink::new());
        congestion::install_sample_sink(samples.clone());
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(Completion {
            samples,
            observed: observed.clone(),
            resource,
        });
        let file = tempfile::tempfile()?;
        run_metadata_probed_blocking_no_rate(
            congestion::Side::Source,
            congestion::MetadataOp::Stat,
            move || file.metadata(),
        )
        .with_subscriber(subscriber)
        .await?;
        assert_eq!(
            *observed.lock().unwrap(),
            vec![(1, true)],
            "execution completion must follow the probe and operation permit cleanup"
        );
        Ok(())
    })
}

#[test]
fn async_metadata_no_rate_finishes_after_resuming_the_operation() -> anyhow::Result<()> {
    runtime()?.block_on(async {
        let capture = Capture::new(true)?;
        let directory = tempfile::tempdir()?;
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let mut work = Box::pin(
            crate::walk::run_metadata_probed_no_rate(
                congestion::Side::Source,
                congestion::MetadataOp::Stat,
                async {
                    released.await.map_err(std::io::Error::other)?;
                    tokio::fs::metadata(directory.path()).await
                },
            )
            .with_subscriber(capture.dispatch.clone()),
        );
        assert!(futures::poll!(work.as_mut()).is_pending());
        let suspended = capture.events();
        release.send(()).unwrap();
        assert!(work.await?.is_dir());
        assert_eq!(
            suspended,
            vec![
                Event::Started("source.metadata.stat.wait_admission"),
                Event::Completed("source.metadata.stat.wait_admission", true),
                Event::Started("source.metadata.stat.execute"),
            ]
        );
        let report = capture.report()?;
        assert_sample(&report, "source.metadata.stat.wait_admission", 1, 0);
        assert_sample(&report, "source.metadata.stat.execute", 1, 0);
        assert_eq!(report["scopes"].as_array().unwrap().len(), 2);
        Ok(())
    })
}

#[test]
fn moved_metadata_scopes_release_their_original_parent_under_another_subscriber()
-> anyhow::Result<()> {
    #[derive(Clone, Default)]
    struct Closed(Arc<std::sync::Mutex<Vec<&'static str>>>);
    impl<S> tracing_subscriber::Layer<S> for Closed
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_close(&self, id: tracing::Id, context: tracing_subscriber::layer::Context<'_, S>) {
            self.0
                .lock()
                .unwrap()
                .push(context.span(&id).unwrap().metadata().name());
        }
    }
    for finish in [false, true] {
        for (phase, name) in [
            (
                MetadataTimingPhase::Queue,
                "source.metadata.stat.wait_worker",
            ),
            (MetadataTimingPhase::Execute, "source.metadata.stat.execute"),
            (MetadataTimingPhase::Rate, "source.metadata.stat.wait_rate"),
            (
                MetadataTimingPhase::Admission,
                "source.metadata.stat.wait_admission",
            ),
        ] {
            let file = tempfile::NamedTempFile::new()?;
            let (layer, mut guard) = crate::timing::TimingLayer::new(
                "moved-metadata".into(),
                Some(file.reopen()?),
                None,
            );
            let original_closed = Closed::default();
            let other_closed = Closed::default();
            let original = tracing::Dispatch::new(
                tracing_subscriber::registry()
                    .with(layer.with_filter(crate::timing::timing_filter(true)))
                    .with(original_closed.clone()),
            );
            let other =
                tracing::Dispatch::new(tracing_subscriber::registry().with(other_closed.clone()));
            let scope = tracing::dispatcher::with_default(&original, || {
                let parent = tracing::info_span!("active_metadata_parent");
                let _entered = parent.enter();
                metadata_timing_scope(
                    congestion::Side::Source,
                    congestion::MetadataOp::Stat,
                    phase,
                )
            });
            let joined = std::thread::spawn(move || {
                tracing::dispatcher::with_default(&other, || {
                    if finish {
                        scope.finish();
                    } else {
                        drop(scope);
                    }
                });
            })
            .join();
            assert!(
                joined.is_ok(),
                "{name} crossed subscriber ownership while finish={finish}"
            );
            assert_eq!(
                original_closed
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|name| **name == "active_metadata_parent")
                    .count(),
                1
            );
            assert!(
                other_closed.0.lock().unwrap().is_empty(),
                "foreign registry received a span close"
            );
            guard.finish()?;
            let report: serde_json::Value = serde_json::from_reader(file.reopen()?)?;
            assert_sample(&report, name, u64::from(finish), u64::from(!finish));
            assert_eq!(report["scopes"].as_array().unwrap().len(), 1);
        }
    }
    Ok(())
}
