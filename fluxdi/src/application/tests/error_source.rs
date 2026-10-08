use super::*;
use crate::application::options::BootstrapOptions;

#[derive(Debug)]
struct StoreUnavailable;

impl std::fmt::Display for StoreUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "store unavailable")
    }
}

impl std::error::Error for StoreUnavailable {}

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Configure,
    OnStart,
    OnStop,
}

fn typed_failure() -> Error {
    Error::factory_failed("SessionStore", Box::new(StoreUnavailable))
}

struct TypedFailureModule {
    fails_in: Phase,
    imports_failing: bool,
}

impl TypedFailureModule {
    fn fails_in(phase: Phase) -> Self {
        Self {
            fails_in: phase,
            imports_failing: false,
        }
    }

    fn result(&self, phase: Phase) -> Result<(), Error> {
        if self.fails_in == phase {
            Err(typed_failure())
        } else {
            Ok(())
        }
    }
}

impl Module for TypedFailureModule {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        if self.imports_failing {
            vec![Box::new(TypedFailureModule::fails_in(self.fails_in))]
        } else {
            vec![]
        }
    }

    fn configure(&self, _: &Injector) -> Result<(), Error> {
        self.result(Phase::Configure)
    }

    fn on_start(&self, _: Shared<Injector>) -> ModuleLifecycleFuture {
        let result = self.result(Phase::OnStart);
        Box::pin(async move { result })
    }

    fn on_stop(&self, _: Shared<Injector>) -> ModuleLifecycleFuture {
        let result = self.result(Phase::OnStop);
        Box::pin(async move { result })
    }
}

fn assert_keeps_typed_source(err: &Error, phase: &str) {
    assert_eq!(
        err.kind,
        ErrorKind::ModuleLifecycleFailed,
        "{}",
        err.message
    );
    assert!(
        err.message.contains(&format!("phase={phase}")),
        "phase missing: {}",
        err.message
    );
    let chain: Vec<&(dyn std::error::Error + 'static)> =
        std::iter::successors(Some(err as &(dyn std::error::Error + 'static)), |e| {
            e.source()
        })
        .collect();
    let nested = chain
        .iter()
        .skip(1)
        .find_map(|e| e.downcast_ref::<Error>())
        .unwrap_or_else(|| panic!("no nested fluxdi Error in the source chain of {err:?}"));
    assert_eq!(nested.kind, ErrorKind::FactoryFailed);
    assert!(
        chain
            .iter()
            .any(|e| e.downcast_ref::<StoreUnavailable>().is_some()),
        "typed error lost from the source chain of {err:?}"
    );
}

#[test]
fn sync_configure_failure_keeps_the_typed_source() {
    let mut app = Application::new(TypedFailureModule::fails_in(Phase::Configure));
    let err = app.bootstrap_sync().unwrap_err();
    assert_keeps_typed_source(&err, "configure");
}

#[test]
fn sequential_configure_failure_keeps_the_typed_source() {
    let mut app = Application::new(TypedFailureModule::fails_in(Phase::Configure));
    let err = block_on(app.bootstrap()).unwrap_err();
    assert_keeps_typed_source(&err, "configure");
}

#[test]
fn parallel_configure_failure_keeps_the_typed_source() {
    let mut app = Application::new(TypedFailureModule::fails_in(Phase::Configure));
    let opts = BootstrapOptions::default().with_parallel_start(true);
    let err = block_on(app.bootstrap_with_options(opts)).unwrap_err();
    assert_keeps_typed_source(&err, "configure");
}

#[test]
fn sequential_on_start_failure_keeps_the_typed_source() {
    let mut app = Application::new(TypedFailureModule::fails_in(Phase::OnStart));
    let err = block_on(app.bootstrap()).unwrap_err();
    assert_keeps_typed_source(&err, "on_start");
}

#[test]
fn parallel_on_start_failure_keeps_the_typed_source() {
    let mut app = Application::new(TypedFailureModule::fails_in(Phase::OnStart));
    let opts = BootstrapOptions::default().with_parallel_start(true);
    let err = block_on(app.bootstrap_with_options(opts)).unwrap_err();
    assert_keeps_typed_source(&err, "on_start");
}

#[test]
fn aggregated_parallel_on_start_failures_keep_the_first_typed_source() {
    let mut app = Application::new(TypedFailureModule {
        fails_in: Phase::OnStart,
        imports_failing: true,
    });
    let opts = BootstrapOptions::default().with_parallel_start(true);
    let err = block_on(app.bootstrap_with_options(opts)).unwrap_err();
    assert!(
        err.message.contains("2 module(s) reported errors"),
        "{}",
        err.message
    );
    let first = std::error::Error::source(&err)
        .expect("the aggregate keeps the first failure as its source")
        .downcast_ref::<Error>()
        .expect("the aggregate's source is a fluxdi Error");
    assert_keeps_typed_source(first, "on_start");
}

#[test]
fn on_stop_failure_keeps_the_typed_source() {
    let mut app = Application::new(TypedFailureModule::fails_in(Phase::OnStop));
    block_on(app.bootstrap()).unwrap();
    let err = block_on(app.shutdown()).unwrap_err();
    assert_keeps_typed_source(&err, "on_stop");
}

#[test]
fn aggregated_on_stop_failures_keep_the_first_typed_source() {
    let mut app = Application::new(TypedFailureModule {
        fails_in: Phase::OnStop,
        imports_failing: true,
    });
    block_on(app.bootstrap()).unwrap();
    let err = block_on(app.shutdown()).unwrap_err();
    assert!(
        err.message.contains("2 module(s) reported errors"),
        "{}",
        err.message
    );
    let first = std::error::Error::source(&err)
        .expect("the aggregate keeps the first failure as its source")
        .downcast_ref::<Error>()
        .expect("the aggregate's source is a fluxdi Error");
    assert_keeps_typed_source(first, "on_stop");
}

#[cfg(feature = "lifecycle")]
#[tokio::test]
async fn on_stop_failure_under_a_shutdown_deadline_keeps_the_typed_source() {
    let mut app = Application::new(TypedFailureModule::fails_in(Phase::OnStop));
    app.bootstrap().await.unwrap();
    let opts = crate::application::options::ShutdownOptions::default()
        .with_timeout(std::time::Duration::from_secs(5));
    let err = app.shutdown_with_options(opts).await.unwrap_err();
    assert_keeps_typed_source(&err, "on_stop");
}

/// The on_start of `ResolvesFailingStore` resolves a store whose factory
/// fails, as a session spine resolved inside on_start does.
#[cfg(feature = "async-factory")]
struct ResolvesFailingStore;

#[cfg(feature = "async-factory")]
struct SessionStore;

#[cfg(feature = "async-factory")]
impl Module for ResolvesFailingStore {
    fn providers(&self, injector: &Injector) {
        injector.provide::<SessionStore>(crate::Provider::root_try_async(|_| async {
            Err::<Shared<SessionStore>, _>(StoreUnavailable)
        }));
    }

    fn on_start(&self, injector: Shared<Injector>) -> ModuleLifecycleFuture {
        Box::pin(async move {
            injector.try_resolve_async::<SessionStore>().await?;
            Ok(())
        })
    }
}

#[cfg(feature = "async-factory")]
#[test]
fn on_start_resolve_failure_keeps_the_factory_error_and_its_kind() {
    let mut app = Application::new(ResolvesFailingStore);
    let err = block_on(app.bootstrap()).unwrap_err();
    assert_keeps_typed_source(&err, "on_start");
}

/// Fails `on_start` after its import started; the import's `on_stop` fails
/// during the rollback.
struct FailsAfterImportStarted;

impl Module for FailsAfterImportStarted {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        vec![Box::new(TypedFailureModule::fails_in(Phase::OnStop))]
    }

    fn on_start(&self, _: Shared<Injector>) -> ModuleLifecycleFuture {
        Box::pin(async { Err(typed_failure()) })
    }
}

#[test]
fn rollback_on_stop_failures_are_reported_after_the_start_failure() {
    for parallel in [false, true] {
        let mut app = Application::new(FailsAfterImportStarted);
        let opts = BootstrapOptions::default().with_parallel_start(parallel);
        let err = block_on(app.bootstrap_with_options(opts)).unwrap_err();
        assert!(
            err.message.contains("2 module(s) reported errors")
                && err.message.contains("phase=on_start")
                && err.message.contains("phase=on_stop"),
            "parallel={parallel}: {}",
            err.message
        );
        let first = std::error::Error::source(&err)
            .expect("the aggregate keeps the start failure as its source")
            .downcast_ref::<Error>()
            .expect("the aggregate's source is a fluxdi Error");
        assert_keeps_typed_source(first, "on_start");
    }
}

/// Fails `configure` after its import started; the import's `on_stop` fails
/// during the rollback.
struct FailsConfigureAfterImportStarted;

impl Module for FailsConfigureAfterImportStarted {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        vec![Box::new(TypedFailureModule::fails_in(Phase::OnStop))]
    }

    fn configure(&self, _: &Injector) -> Result<(), Error> {
        Err(typed_failure())
    }
}

#[test]
fn rollback_on_stop_failures_are_reported_after_the_configure_failure() {
    let mut app = Application::new(FailsConfigureAfterImportStarted);
    let err = block_on(app.bootstrap()).unwrap_err();
    assert!(
        err.message.contains("2 module(s) reported errors")
            && err.message.contains("phase=configure")
            && err.message.contains("phase=on_stop"),
        "{}",
        err.message
    );
    let first = std::error::Error::source(&err)
        .expect("the aggregate keeps the configure failure as its source")
        .downcast_ref::<Error>()
        .expect("the aggregate's source is a fluxdi Error");
    assert_keeps_typed_source(first, "configure");
}

#[test]
fn a_parallel_configure_failure_stops_no_module_because_none_started() {
    let mut app = Application::new(FailsConfigureAfterImportStarted);
    let opts = BootstrapOptions::default().with_parallel_start(true);
    let err = block_on(app.bootstrap_with_options(opts)).unwrap_err();
    assert_keeps_typed_source(&err, "configure");
    assert!(!err.message.contains("phase=on_stop"), "{}", err.message);
}
