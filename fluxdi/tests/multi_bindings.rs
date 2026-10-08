use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "async-factory")]
use fluxdi::ErrorKind;
use fluxdi::{Injector, Provider, Shared};

trait PipelineStep: Send + Sync {
    fn id(&self) -> &'static str;
}

struct StepA;
impl PipelineStep for StepA {
    fn id(&self) -> &'static str {
        "a"
    }
}

struct StepB;
impl PipelineStep for StepB {
    fn id(&self) -> &'static str {
        "b"
    }
}

#[test]
fn set_resolve_preserves_registration_order() {
    let injector = Injector::root();

    injector.provide_into_set::<dyn PipelineStep>(Provider::singleton(|_| {
        Shared::new(StepA) as Shared<dyn PipelineStep>
    }));
    injector.provide_into_set::<dyn PipelineStep>(Provider::singleton(|_| {
        Shared::new(StepB) as Shared<dyn PipelineStep>
    }));

    let steps = injector.try_resolve_all::<dyn PipelineStep>().unwrap();
    let ids: Vec<&'static str> = steps.iter().map(|step| step.id()).collect();

    assert_eq!(ids, vec!["a", "b"]);
}

#[test]
fn parent_set_bindings_are_resolved_before_child_bindings() {
    let parent = Injector::root();
    parent.provide_into_set::<dyn PipelineStep>(Provider::singleton(|_| {
        Shared::new(StepA) as Shared<dyn PipelineStep>
    }));

    let child = Injector::child(Shared::new(parent.clone()));
    child.provide_into_set::<dyn PipelineStep>(Provider::singleton(|_| {
        Shared::new(StepB) as Shared<dyn PipelineStep>
    }));

    let steps = child.try_resolve_all::<dyn PipelineStep>().unwrap();
    let ids: Vec<&'static str> = steps.iter().map(|step| step.id()).collect();

    assert_eq!(ids, vec!["a", "b"]);
}

#[derive(Debug)]
struct Marker {
    source: &'static str,
    serial: usize,
}

#[test]
fn set_resolution_honors_scope_caching_rules() {
    static ROOT_CREATED: AtomicUsize = AtomicUsize::new(0);
    static MODULE_CREATED: AtomicUsize = AtomicUsize::new(0);
    static TRANSIENT_CREATED: AtomicUsize = AtomicUsize::new(0);

    ROOT_CREATED.store(0, Ordering::SeqCst);
    MODULE_CREATED.store(0, Ordering::SeqCst);
    TRANSIENT_CREATED.store(0, Ordering::SeqCst);

    let injector = Injector::root();
    injector.provide_into_set::<Marker>(Provider::root(|_| {
        Shared::new(Marker {
            source: "root",
            serial: ROOT_CREATED.fetch_add(1, Ordering::SeqCst),
        })
    }));
    injector.provide_into_set::<Marker>(Provider::singleton(|_| {
        Shared::new(Marker {
            source: "module",
            serial: MODULE_CREATED.fetch_add(1, Ordering::SeqCst),
        })
    }));
    injector.provide_into_set::<Marker>(Provider::transient(|_| {
        Shared::new(Marker {
            source: "transient",
            serial: TRANSIENT_CREATED.fetch_add(1, Ordering::SeqCst),
        })
    }));

    let first = injector.try_resolve_all::<Marker>().unwrap();
    let second = injector.try_resolve_all::<Marker>().unwrap();

    assert_eq!(first.len(), 3);
    assert_eq!(second.len(), 3);
    assert_eq!(first[0].source, "root");
    assert_eq!(first[1].source, "module");
    assert_eq!(first[2].source, "transient");

    assert!(Shared::ptr_eq(&first[0], &second[0]));
    assert!(Shared::ptr_eq(&first[1], &second[1]));
    assert!(!Shared::ptr_eq(&first[2], &second[2]));

    assert_eq!(first[0].serial, 0);
    assert_eq!(first[1].serial, 0);
    assert_eq!(first[2].serial, 0);
    assert_eq!(second[2].serial, 1);

    assert_eq!(ROOT_CREATED.load(Ordering::SeqCst), 1);
    assert_eq!(MODULE_CREATED.load(Ordering::SeqCst), 1);
    assert_eq!(TRANSIENT_CREATED.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "async-factory")]
#[test]
fn set_with_async_provider_requires_async_resolve_api() {
    use futures::executor::block_on;

    let injector = Injector::root();
    injector.provide_into_set::<String>(Provider::singleton(|_| Shared::new("sync".to_string())));
    injector.provide_into_set::<String>(Provider::singleton_async(|_| async {
        Shared::new("async".to_string())
    }));

    let sync_err = match injector.try_resolve_all::<String>() {
        Ok(_) => panic!("expected sync set resolve to fail for async provider"),
        Err(err) => err,
    };
    assert_eq!(sync_err.kind, ErrorKind::AsyncFactoryRequiresAsyncResolve);

    let values = block_on(injector.try_resolve_all_async::<String>()).unwrap();
    let values: Vec<&str> = values.iter().map(|value| value.as_str()).collect();
    assert_eq!(values, vec!["sync", "async"]);
}
