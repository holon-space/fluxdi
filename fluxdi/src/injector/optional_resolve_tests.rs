use super::*;
use crate::{ErrorKind, Provider, Shared};

use futures::executor::block_on;

fn failing() -> Provider<String> {
    Provider::singleton_try_async(|_| async {
        Err::<Shared<String>, _>(std::io::Error::other("database unreachable"))
    })
}

fn async_only() -> Provider<String> {
    Provider::singleton_async(|_| async { Shared::new("async".to_string()) })
}

fn assert_kind<T>(result: Result<Option<T>, Error>, kind: ErrorKind) {
    match result {
        Err(err) => assert_eq!(err.kind, kind, "{}", err.message),
        Ok(value) => panic!(
            "expected a {kind:?} error, got Ok({})",
            if value.is_some() { "Some" } else { "None" }
        ),
    }
}

#[test]
fn try_optional_resolve_async_is_none_only_without_a_binding() {
    let injector = Injector::root();
    assert!(
        block_on(injector.try_optional_resolve_async::<String>())
            .unwrap()
            .is_none()
    );
    assert!(
        block_on(injector.try_optional_resolve_named_async::<String>("primary"))
            .unwrap()
            .is_none()
    );
    assert!(
        block_on(injector.try_optional_resolve_all_async::<String>())
            .unwrap()
            .is_none()
    );
    assert!(injector.try_optional_resolve::<String>().unwrap().is_none());
    assert!(
        injector
            .try_optional_resolve_named::<String>("primary")
            .unwrap()
            .is_none()
    );
    assert!(
        injector
            .try_optional_resolve_all::<String>()
            .unwrap()
            .is_none()
    );
}

#[test]
fn try_optional_resolve_async_returns_a_bound_value() {
    let injector = Injector::root();
    injector.provide::<String>(async_only());
    let value = block_on(injector.try_optional_resolve_async::<String>())
        .unwrap()
        .expect("bound");
    assert_eq!(value.as_str(), "async");
}

#[test]
fn try_optional_resolve_async_returns_a_factory_failure() {
    let injector = Injector::root();
    injector.provide::<String>(failing());
    assert_kind(
        block_on(injector.try_optional_resolve_async::<String>()),
        ErrorKind::FactoryFailed,
    );
}

#[test]
fn try_optional_resolve_named_async_returns_a_factory_failure() {
    let injector = Injector::root();
    injector.provide_named::<String>("primary", failing());
    assert_kind(
        block_on(injector.try_optional_resolve_named_async::<String>("primary")),
        ErrorKind::FactoryFailed,
    );
}

#[test]
fn try_optional_resolve_all_async_returns_a_factory_failure() {
    let injector = Injector::root();
    injector.provide_into_set::<String>(failing());
    assert_kind(
        block_on(injector.try_optional_resolve_all_async::<String>()),
        ErrorKind::FactoryFailed,
    );
}

#[test]
fn try_optional_resolve_returns_a_sync_resolve_of_an_async_binding() {
    let injector = Injector::root();
    injector.provide::<String>(async_only());
    injector.provide_named::<String>("primary", async_only());
    injector.provide_into_set::<String>(async_only());
    assert_kind(
        injector.try_optional_resolve::<String>(),
        ErrorKind::AsyncFactoryRequiresAsyncResolve,
    );
    assert_kind(
        injector.try_optional_resolve_named::<String>("primary"),
        ErrorKind::AsyncFactoryRequiresAsyncResolve,
    );
    assert_kind(
        injector.try_optional_resolve_all::<String>(),
        ErrorKind::AsyncFactoryRequiresAsyncResolve,
    );
}

/// Pins the deprecated swallow until `optional_resolve*` is removed.
#[test]
#[allow(deprecated)]
fn deprecated_optional_resolve_turns_a_failure_into_none() {
    let injector = Injector::root();
    injector.provide::<String>(failing());
    injector.provide_named::<String>("primary", failing());
    injector.provide_into_set::<String>(failing());
    assert!(block_on(injector.optional_resolve_async::<String>()).is_none());
    assert!(block_on(injector.optional_resolve_named_async::<String>("primary")).is_none());
    assert!(block_on(injector.optional_resolve_all_async::<String>()).is_none());
    assert!(injector.optional_resolve::<String>().is_none());
    assert!(
        injector
            .optional_resolve_named::<String>("primary")
            .is_none()
    );
    assert!(injector.optional_resolve_all::<String>().is_none());
}
