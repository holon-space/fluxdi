use super::*;
use crate::{ErrorKind, Provider, Shared};

use futures::executor::block_on;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[cfg(feature = "thread-safe")]
fn fails_on_first_run(
    runs: Arc<AtomicUsize>,
) -> impl Fn(
    Injector,
) -> std::pin::Pin<Box<dyn Future<Output = Result<Shared<String>, BoxError>> + Send>>
+ Send
+ Sync
+ 'static {
    move |_| {
        let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            if run == 1 {
                return Err("database unreachable".into());
            }
            Ok(Shared::new(format!("run {run}")))
        })
    }
}

#[cfg(not(feature = "thread-safe"))]
fn fails_on_first_run(
    runs: Arc<AtomicUsize>,
) -> impl Fn(Injector) -> std::pin::Pin<Box<dyn Future<Output = Result<Shared<String>, BoxError>>>>
+ 'static {
    move |_| {
        let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move {
            if run == 1 {
                return Err("database unreachable".into());
            }
            Ok(Shared::new(format!("run {run}")))
        })
    }
}

fn assert_factory_failed(err: Error) {
    assert_eq!(err.kind, ErrorKind::FactoryFailed);
    let source = std::error::Error::source(&err).expect("FactoryFailed keeps its source");
    assert_eq!(source.to_string(), "database unreachable");
    assert!(
        err.message.contains("database unreachable"),
        "source message lost: {}",
        err.message
    );
    assert!(
        err.message.contains(std::any::type_name::<String>()),
        "type name missing: {}",
        err.message
    );
}

fn assert_failure_is_not_cached(injector: &Injector, runs: &AtomicUsize, cached: bool) {
    assert_factory_failed(block_on(injector.try_resolve_async::<String>()).unwrap_err());
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    let second = block_on(injector.try_resolve_async::<String>()).unwrap();
    assert_eq!(second.as_str(), "run 2");
    assert_eq!(runs.load(Ordering::SeqCst), 2);

    let third = block_on(injector.try_resolve_async::<String>()).unwrap();
    assert_eq!(Shared::ptr_eq(&second, &third), cached);
    assert_eq!(runs.load(Ordering::SeqCst), if cached { 2 } else { 3 });
}

#[test]
fn singleton_try_async_failure_is_returned_and_not_cached() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<String>(Provider::singleton_try_async(fails_on_first_run(
        runs.clone(),
    )));

    assert_failure_is_not_cached(&injector, &runs, true);
}

#[test]
fn root_try_async_failure_is_returned_and_not_cached() {
    let runs = Arc::new(AtomicUsize::new(0));
    let root = Injector::root();
    root.provide::<String>(Provider::root_try_async(fails_on_first_run(runs.clone())));
    let child = Injector::child(Shared::new(root.clone()));

    assert_failure_is_not_cached(&child, &runs, true);
    let from_root = block_on(root.try_resolve_async::<String>()).unwrap();
    assert_eq!(from_root.as_str(), "run 2");
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[test]
fn scoped_try_async_failure_is_returned_and_not_cached() {
    let runs = Arc::new(AtomicUsize::new(0));
    let root = Injector::root();
    root.provide::<String>(Provider::scoped_try_async(fails_on_first_run(runs.clone())));
    let scope = root.create_scope();

    assert_failure_is_not_cached(&scope, &runs, true);
}

#[test]
fn transient_try_async_failure_is_returned() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<String>(Provider::transient_try_async(fails_on_first_run(
        runs.clone(),
    )));

    assert_failure_is_not_cached(&injector, &runs, false);
}

#[test]
fn named_try_async_failure_is_returned_and_not_cached() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide_named::<String>(
        "primary",
        Provider::singleton_try_async(fails_on_first_run(runs.clone())),
    );

    assert_factory_failed(
        block_on(injector.try_resolve_named_async::<String>("primary")).unwrap_err(),
    );
    let second = block_on(injector.try_resolve_named_async::<String>("primary")).unwrap();
    let third = block_on(injector.try_resolve_named_async::<String>("primary")).unwrap();
    assert_eq!(second.as_str(), "run 2");
    assert!(Shared::ptr_eq(&second, &third));
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[test]
fn set_try_async_failure_is_returned_and_not_cached() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide_into_set::<String>(Provider::singleton_async(|_| async {
        Shared::new("healthy".to_string())
    }));
    injector.provide_into_set::<String>(Provider::singleton_try_async(fails_on_first_run(
        runs.clone(),
    )));

    assert_factory_failed(block_on(injector.try_resolve_all_async::<String>()).unwrap_err());
    let second = block_on(injector.try_resolve_all_async::<String>()).unwrap();
    let third = block_on(injector.try_resolve_all_async::<String>()).unwrap();
    let values: Vec<&str> = second.iter().map(|v| v.as_str()).collect();
    assert_eq!(values, ["healthy", "run 2"]);
    assert!(Shared::ptr_eq(&second[1], &third[1]));
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "eager-resolution")]
#[test]
fn eager_resolution_reports_try_async_failure() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<String>(Provider::singleton_try_async(fails_on_first_run(
        runs.clone(),
    )));

    let err = block_on(injector.resolve_all_eager()).unwrap_err();
    assert_eq!(err.kind, ErrorKind::EagerResolutionFailed);
    assert!(
        err.message.contains("database unreachable"),
        "source message lost: {}",
        err.message
    );

    block_on(injector.resolve_all_eager()).unwrap();
    assert_eq!(
        block_on(injector.try_resolve_async::<String>())
            .unwrap()
            .as_str(),
        "run 2"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[test]
fn nested_fluxdi_error_kind_is_recoverable_from_the_source() {
    let injector = Injector::root();
    injector.provide::<String>(Provider::singleton_try_async(|inj: Injector| async move {
        let missing = inj.try_resolve_async::<u32>().await?;
        Ok::<_, Error>(Shared::new(missing.to_string()))
    }));

    let err = block_on(injector.try_resolve_async::<String>()).unwrap_err();
    assert_eq!(err.kind, ErrorKind::FactoryFailed);
    let source = std::error::Error::source(&err)
        .expect("FactoryFailed keeps its source")
        .downcast_ref::<Error>()
        .expect("source is the nested fluxdi Error");
    assert_eq!(source.kind, ErrorKind::ServiceNotProvided);
}

#[test]
fn nested_circular_dependency_is_recoverable_from_the_source() {
    let injector = Injector::root();
    injector.provide::<String>(Provider::singleton_try_async(|inj: Injector| async move {
        let itself = inj.try_resolve_async::<String>().await?;
        Ok::<_, Error>(itself)
    }));

    let err = block_on(injector.try_resolve_async::<String>()).unwrap_err();
    assert_eq!(err.kind, ErrorKind::FactoryFailed);
    let source = std::error::Error::source(&err)
        .expect("FactoryFailed keeps its source")
        .downcast_ref::<Error>()
        .expect("source is the nested fluxdi Error");
    assert_eq!(source.kind, ErrorKind::CircularDependency);
}

#[test]
fn anyhow_error_chain_survives() {
    let injector = Injector::root();
    injector.provide::<String>(Provider::singleton_try_async(|_| async {
        Err::<Shared<String>, _>(anyhow::anyhow!("disk full").context("open database"))
    }));

    let err = block_on(injector.try_resolve_async::<String>()).unwrap_err();
    assert_eq!(err.kind, ErrorKind::FactoryFailed);
    assert!(err.message.contains("open database"), "{}", err.message);
    let chain: Vec<String> = std::iter::successors(std::error::Error::source(&err), |e| e.source())
        .map(|e| e.to_string())
        .collect();
    assert_eq!(chain, ["open database", "disk full"]);
}
