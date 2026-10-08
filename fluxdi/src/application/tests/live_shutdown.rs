use std::time::Duration;

use tokio::time::timeout;

use super::*;
use crate::Provider;
use crate::application::options::ShutdownOptions;
use crate::live::{LiveOutcome, LiveState};

const HANG: Duration = Duration::from_secs(5);

struct Endless;

/// Registers `Endless`, whose live producer never finishes; with
/// `wait_on_stop`, its `on_stop` waits for `Endless` and records the outcome.
struct EndlessModule {
    wait_on_stop: Option<Arc<Mutex<Option<Error>>>>,
}

impl Module for EndlessModule {
    fn providers(&self, injector: &Injector) {
        injector.provide::<Endless>(Provider::root_async(|_| async {
            std::future::pending::<()>().await;
            Shared::new(Endless)
        }));
    }

    fn on_stop(&self, injector: Shared<Injector>) -> ModuleLifecycleFuture {
        let Some(seen) = self.wait_on_stop.clone() else {
            return Box::pin(async { Ok(()) });
        };
        Box::pin(async move {
            let error = injector
                .resolve_live::<Endless>()
                .ready()
                .await
                .err()
                .expect("Endless became ready");
            *seen.lock().unwrap() = Some(error.clone());
            Err(error)
        })
    }
}

async fn started_app(wait_on_stop: Option<Arc<Mutex<Option<Error>>>>) -> Application {
    let mut app = Application::new(EndlessModule { wait_on_stop });
    app.bootstrap().await.unwrap();
    app
}

async fn assert_shutdown_cancels_the_producer<F>(shutdown: F)
where
    F: for<'a> FnOnce(
        &'a mut Application,
    )
        -> std::pin::Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>,
{
    let mut app = started_app(None).await;
    let live = app.injector().resolve_live::<Endless>();

    timeout(HANG, shutdown(&mut app))
        .await
        .expect("shutdown hung")
        .unwrap();
    let error = timeout(HANG, live.ready())
        .await
        .expect("the live producer outlived the application shutdown")
        .err()
        .expect("Endless became ready");
    assert_eq!(error.kind, ErrorKind::LiveProducerCancelled, "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cancels_live_producers() {
    assert_shutdown_cancels_the_producer(|app| Box::pin(app.shutdown())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_async_cancels_live_producers() {
    assert_shutdown_cancels_the_producer(|app| Box::pin(app.shutdown_async())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_cancels_live_producers() {
    assert_shutdown_cancels_the_producer(|app| Box::pin(app.stop())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_with_options_cancels_live_producers() {
    assert_shutdown_cancels_the_producer(|app| {
        Box::pin(app.shutdown_with_options(ShutdownOptions::default()))
    })
    .await;
}

#[cfg(feature = "lifecycle")]
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_with_a_timeout_cancels_live_producers() {
    assert_shutdown_cancels_the_producer(|app| {
        Box::pin(app.shutdown_with_options(ShutdownOptions::default().with_timeout(HANG)))
    })
    .await;
}

async fn assert_on_stop_sees_the_cancellation<F>(shutdown: F)
where
    F: for<'a> FnOnce(
        &'a mut Application,
    )
        -> std::pin::Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>,
{
    let seen = Arc::new(Mutex::new(None));
    let mut app = started_app(Some(seen.clone())).await;
    app.injector().resolve_live::<Endless>();

    let error = timeout(HANG, shutdown(&mut app))
        .await
        .expect("on_stop waited for ever on a live producer")
        .unwrap_err();
    assert!(error.message.contains("cancelled"), "{error}");
    let seen = seen.lock().unwrap().clone().expect("on_stop did not run");
    assert_eq!(seen.kind, ErrorKind::LiveProducerCancelled, "{seen}");
}

#[tokio::test(flavor = "multi_thread")]
async fn on_stop_waiting_on_a_live_producer_sees_its_cancellation() {
    assert_on_stop_sees_the_cancellation(|app| Box::pin(app.shutdown())).await;
}

#[cfg(feature = "lifecycle")]
#[tokio::test(flavor = "multi_thread")]
async fn on_stop_waiting_on_a_live_producer_sees_its_cancellation_within_a_timeout() {
    assert_on_stop_sees_the_cancellation(|app| {
        Box::pin(app.shutdown_with_options(ShutdownOptions::default().with_timeout(HANG * 2)))
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_shutdown_leaves_live_state_as_it_is() {
    let mut app = started_app(None).await;
    let live = app.injector().resolve_live::<Endless>();
    timeout(HANG, app.shutdown()).await.unwrap().unwrap();
    timeout(HANG, live.ready())
        .await
        .unwrap()
        .err()
        .expect("Endless became ready");
    let ended = |app: &Application| {
        let report = app.injector().live_report();
        assert_eq!(report.len(), 1);
        assert!(
            matches!(&report[0].outcome, LiveOutcome::Failed(error) if error.kind == ErrorKind::LiveProducerCancelled)
        );
        report[0].elapsed
    };
    let elapsed = ended(&app);

    timeout(HANG, app.shutdown()).await.unwrap().unwrap();
    app.injector().shutdown_live();

    assert_eq!(ended(&app), elapsed);
    let state = live.state();
    assert_eq!(state.generation.get(), 1);
    assert!(
        matches!(&state.value, LiveState::Failed(error) if error.kind == ErrorKind::LiveProducerCancelled)
    );
}

/// Starts `Endless` live in `on_start`, then fails its own start.
struct FailsAfterLiveStart {
    seen: Arc<Mutex<Option<Error>>>,
}

impl Module for FailsAfterLiveStart {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        vec![Box::new(EndlessModule {
            wait_on_stop: Some(self.seen.clone()),
        })]
    }

    fn on_start(&self, injector: Shared<Injector>) -> ModuleLifecycleFuture {
        Box::pin(async move {
            injector.resolve_live::<Endless>();
            Err(Error::module_lifecycle_failed(
                "FailsAfterLiveStart",
                "on_start",
                "intentional test failure",
            ))
        })
    }
}

async fn assert_rollback_on_stop_sees_the_cancellation(parallel_start: bool) {
    let seen = Arc::new(Mutex::new(None));
    let mut app = Application::new(FailsAfterLiveStart { seen: seen.clone() });

    timeout(
        HANG,
        app.bootstrap_with_options(
            crate::application::options::BootstrapOptions::default()
                .with_parallel_start(parallel_start),
        ),
    )
    .await
    .expect("a rollback on_stop waited for ever on a live producer")
    .err()
    .expect("the bootstrap succeeded");
    let seen = seen.lock().unwrap().clone().expect("on_stop did not run");
    assert_eq!(seen.kind, ErrorKind::LiveProducerCancelled, "{seen}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_on_stop_waiting_on_a_live_producer_sees_its_cancellation() {
    assert_rollback_on_stop_sees_the_cancellation(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parallel_rollback_on_stop_waiting_on_a_live_producer_sees_its_cancellation() {
    assert_rollback_on_stop_sees_the_cancellation(true).await;
}

/// Starts `Endless` live in `configure`, then fails its own configure.
struct FailsConfigureAfterLiveStart {
    seen: Arc<Mutex<Option<Error>>>,
}

impl Module for FailsConfigureAfterLiveStart {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        vec![Box::new(EndlessModule {
            wait_on_stop: Some(self.seen.clone()),
        })]
    }

    fn configure(&self, injector: &Injector) -> Result<(), Error> {
        injector.resolve_live::<Endless>();
        Err(Error::module_lifecycle_failed(
            "FailsConfigureAfterLiveStart",
            "configure",
            "intentional test failure",
        ))
    }
}

async fn assert_configure_failure_cancels_the_producer(app: &Application) {
    let error = timeout(HANG, app.injector().resolve_live::<Endless>().ready())
        .await
        .expect("the live producer outlived the failed bootstrap")
        .err()
        .expect("Endless became ready");
    assert_eq!(error.kind, ErrorKind::LiveProducerCancelled, "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_configure_failure_rolls_back_the_started_modules_and_live_production() {
    let seen = Arc::new(Mutex::new(None));
    let mut app = Application::new(FailsConfigureAfterLiveStart { seen: seen.clone() });

    let error = timeout(HANG, app.bootstrap())
        .await
        .expect("a rollback on_stop waited for ever on a live producer")
        .unwrap_err();
    assert!(error.message.contains("phase=configure"), "{error}");
    let seen = seen.lock().unwrap().clone().expect("on_stop did not run");
    assert_eq!(seen.kind, ErrorKind::LiveProducerCancelled, "{seen}");
    assert_configure_failure_cancels_the_producer(&app).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parallel_configure_failure_ends_live_production() {
    let seen = Arc::new(Mutex::new(None));
    let mut app = Application::new(FailsConfigureAfterLiveStart { seen: seen.clone() });

    let error = timeout(
        HANG,
        app.bootstrap_with_options(
            crate::application::options::BootstrapOptions::default().with_parallel_start(true),
        ),
    )
    .await
    .expect("the bootstrap hung")
    .unwrap_err();
    assert!(error.message.contains("phase=configure"), "{error}");
    assert!(
        seen.lock().unwrap().is_none(),
        "a module that never started was stopped"
    );
    assert_configure_failure_cancels_the_producer(&app).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sync_configure_failure_ends_live_production() {
    let seen = Arc::new(Mutex::new(None));
    let mut app = Application::new(FailsConfigureAfterLiveStart { seen });

    let error = app.bootstrap_sync().unwrap_err();
    assert!(error.message.contains("phase=configure"), "{error}");
    assert_configure_failure_cancels_the_producer(&app).await;
}
