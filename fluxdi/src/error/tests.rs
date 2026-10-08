use super::*;

#[test]
fn service_not_provided_error() {
    let err = Error::service_not_provided("MyType");
    assert!(err.kind == ErrorKind::ServiceNotProvided);
    assert!(err.message.contains("MyType"));
    assert!(err.message.contains("Module::configure"));
}

#[test]
fn service_not_provided_for_override_error() {
    let err = Error::service_not_provided_for_override("MyType");
    assert!(err.kind == ErrorKind::ServiceNotProvided);
    assert!(err.message.contains("override"));
    assert!(err.message.contains("provide"));
}

#[test]
fn service_not_provided_named_error() {
    let err = Error::service_not_provided_named("MyType", "primary");
    assert!(err.kind == ErrorKind::ServiceNotProvided);
    assert!(err.message.contains("MyType"));
    assert!(err.message.contains("primary"));
    assert!(err.message.contains("provide_named"));
}

#[test]
fn type_mismatch_error() {
    let err = Error::type_mismatch("OtherType");
    assert!(err.kind == ErrorKind::TypeMismatch);
    assert!(err.message.contains("OtherType"));
}

#[test]
fn provider_already_registered_error() {
    let err = Error::provider_already_registered("Foo", "transient");
    assert!(err.kind == ErrorKind::ProviderAlreadyRegistered);
    assert!(err.message.contains("Foo"));
    assert!(err.message.contains("transient"));
    assert!(err.message.contains("override_provider"));
}

#[test]
fn provider_already_registered_named_error() {
    let err = Error::provider_already_registered_named("Foo", "primary", "root");
    assert!(err.kind == ErrorKind::ProviderAlreadyRegistered);
    assert!(err.message.contains("Foo"));
    assert!(err.message.contains("primary"));
    assert!(err.message.contains("root"));
}

#[test]
fn circular_dependency_error() {
    let chain = ["A", "B", "A"];
    let err = Error::circular_dependency(&chain);
    assert!(err.kind == ErrorKind::CircularDependency);
    assert!(err.message.contains("A -> B -> A"));
    assert!(err.message.contains("Break the cycle"));
}

#[test]
fn async_factory_requires_async_resolve_error() {
    let err = Error::async_factory_requires_async_resolve("AsyncType");
    assert!(err.kind == ErrorKind::AsyncFactoryRequiresAsyncResolve);
    assert!(err.message.contains("AsyncType"));
    assert!(err.message.contains("try_resolve_async"));
}

#[test]
fn resource_limit_exceeded_error() {
    let err = Error::resource_limit_exceeded("DbPool", "max_concurrent_creations=1");
    assert!(err.kind == ErrorKind::ResourceLimitExceeded);
    assert!(err.message.contains("DbPool"));
    assert!(err.message.contains("max_concurrent_creations=1"));
}

#[test]
fn module_lifecycle_failed_error() {
    let err = Error::module_lifecycle_failed("WebModule", "on_start", "bind failed");
    assert!(err.kind == ErrorKind::ModuleLifecycleFailed);
    assert!(err.message.contains("WebModule"));
    assert!(err.message.contains("on_start"));
    assert!(err.message.contains("bind failed"));
}

#[test]
fn module_lifecycle_failed_with_source_keeps_the_message_and_the_source() {
    let err = Error::module_lifecycle_failed_with_source(
        "WebModule",
        "on_start",
        std::io::Error::new(std::io::ErrorKind::AddrInUse, "bind failed"),
    );
    assert_eq!(
        err.message,
        Error::module_lifecycle_failed("WebModule", "on_start", "bind failed").message
    );
    let source = std::error::Error::source(&err)
        .expect("source kept")
        .downcast_ref::<std::io::Error>()
        .expect("source is the io::Error");
    assert_eq!(source.kind(), std::io::ErrorKind::AddrInUse);
}

#[test]
fn graph_validation_failed_error() {
    let err = Error::graph_validation_failed("missing Foo");
    assert!(err.kind == ErrorKind::GraphValidationFailed);
    assert!(err.message.contains("missing Foo"));
    assert!(err.message.contains("dependency_graph"));
}

#[test]
fn display_trait() {
    let err = Error::service_not_provided("X");
    let s = format!("{}", err);
    #[cfg(feature = "debug")]
    assert!(s.contains("ServiceNotProvided"));
    assert!(s.contains("X"));
}

#[test]
fn error_kind_equality() {
    let err1 = Error::type_mismatch("A");
    let err2 = Error::type_mismatch("B");
    assert!(err1.kind == err2.kind);
    assert_ne!(err1.message, err2.message);
}

#[test]
fn shutdown_aggregate_error() {
    let e1 = Error::module_lifecycle_failed("A", "on_stop", "detail1");
    let e2 = Error::module_lifecycle_failed("B", "on_stop", "detail2");
    let err = Error::shutdown_aggregate(vec![e1, e2]);
    assert_eq!(err.kind, ErrorKind::ModuleLifecycleFailed);
    assert!(err.message.contains("2 module(s) reported errors"));
    assert!(err.message.contains("detail1"));
    assert!(err.message.contains("detail2"));
}

#[test]
fn shutdown_aggregate_single_error_returns_unchanged() {
    let e = Error::module_lifecycle_failed("A", "on_stop", "detail");
    let err = Error::shutdown_aggregate(vec![e]);
    assert_eq!(err.kind, ErrorKind::ModuleLifecycleFailed);
    assert!(err.message.contains("A"));
    assert!(err.message.contains("detail"));
}

#[test]
fn bootstrap_aggregate_error() {
    let e1 = Error::module_lifecycle_failed("A", "on_start", "detail1");
    let e2 = Error::module_lifecycle_failed("B", "on_start", "detail2");
    let err = Error::bootstrap_aggregate(vec![e1, e2]);
    assert_eq!(err.kind, ErrorKind::ModuleLifecycleFailed);
    assert!(err.message.contains("2 module(s) reported errors"));
    assert!(err.message.contains("detail1"));
    assert!(err.message.contains("detail2"));
}

#[test]
fn bootstrap_aggregate_single_error_returns_unchanged() {
    let e = Error::module_lifecycle_failed("A", "on_start", "detail");
    let err = Error::bootstrap_aggregate(vec![e]);
    assert_eq!(err.kind, ErrorKind::ModuleLifecycleFailed);
    assert!(err.message.contains("A"));
    assert!(err.message.contains("detail"));
}

#[test]
fn factory_panicked_keeps_the_panic_as_a_typed_source() {
    let err = Error::factory_panicked("SessionStore", "store poisoned");
    assert_eq!(err.kind, ErrorKind::FactoryFailed);
    assert!(
        err.message.contains("panicked: store poisoned"),
        "{}",
        err.message
    );
    let panic = std::error::Error::source(&err)
        .expect("the panic is the error's source")
        .downcast_ref::<FactoryPanic>()
        .expect("the source is a FactoryPanic");
    assert_eq!(panic.message, "store poisoned");
}
