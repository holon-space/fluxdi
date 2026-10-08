use super::*;
use crate::{ErrorKind, FactoryPanic, Provider, Shared};

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Store(usize);
struct Session;

fn assert_panicked(err: &Error, type_name: &str, panic: &str) {
    assert_eq!(err.kind, ErrorKind::FactoryFailed, "{}", err.message);
    assert!(
        err.message.contains(type_name) && err.message.contains("panicked"),
        "{}",
        err.message
    );
    let source = std::error::Error::source(err)
        .and_then(|e| e.downcast_ref::<FactoryPanic>())
        .unwrap_or_else(|| panic!("no FactoryPanic source on {err:?}"));
    assert_eq!(source.message, panic);
}

/// Counts its runs and panics on the first one.
fn panics_on_first_run(runs: &Arc<AtomicUsize>) -> usize {
    let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
    if run == 1 {
        panic!("store corrupt");
    }
    run
}

#[test]
fn a_panicking_sync_singleton_is_a_failure_of_that_binding_and_is_not_cached() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<Store>(Provider::singleton({
        let runs = runs.clone();
        move |_: &Injector| Shared::new(Store(panics_on_first_run(&runs)))
    }));

    let err = injector
        .try_resolve::<Store>()
        .err()
        .expect("a panic resolved");
    assert_panicked(&err, std::any::type_name::<Store>(), "store corrupt");

    assert_eq!(injector.try_resolve::<Store>().unwrap().0, 2);
    assert_eq!(injector.try_resolve::<Store>().unwrap().0, 2);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[test]
fn a_panicking_transient_is_a_failure_of_that_resolve() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<Store>(Provider::transient({
        let runs = runs.clone();
        move |_: &Injector| Shared::new(Store(panics_on_first_run(&runs)))
    }));

    let err = injector
        .try_resolve::<Store>()
        .err()
        .expect("a panic resolved");
    assert_panicked(&err, std::any::type_name::<Store>(), "store corrupt");
    assert_eq!(injector.try_resolve::<Store>().unwrap().0, 2);
}

#[test]
fn a_panicking_set_member_is_a_failure_of_the_set_resolve() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide_into_set::<Store>(Provider::singleton({
        let runs = runs.clone();
        move |_: &Injector| Shared::new(Store(panics_on_first_run(&runs)))
    }));

    let err = injector
        .try_resolve_all::<Store>()
        .err()
        .expect("a panic resolved");
    assert_panicked(&err, std::any::type_name::<Store>(), "store corrupt");
    assert_eq!(injector.try_resolve_all::<Store>().unwrap()[0].0, 2);
}

#[test]
fn a_panic_in_a_nested_factory_stays_inside_the_nested_resolve() {
    let seen = Arc::new(std::sync::Mutex::new(None));
    let injector = Injector::root();
    injector.provide::<Store>(Provider::singleton(|_: &Injector| -> Shared<Store> {
        panic!("store corrupt")
    }));
    injector.provide::<Session>(Provider::singleton({
        let seen = seen.clone();
        move |inj: &Injector| {
            *seen.lock().unwrap() = inj.try_resolve::<Store>().err();
            Shared::new(Session)
        }
    }));

    injector.try_resolve::<Session>().unwrap();
    let inner = seen.lock().unwrap().take().expect("Store resolved");
    assert_panicked(&inner, std::any::type_name::<Store>(), "store corrupt");
}

#[test]
fn a_factory_that_unwraps_a_nested_panic_fails_with_its_own_type() {
    let injector = Injector::root();
    injector.provide::<Store>(Provider::singleton(|_: &Injector| -> Shared<Store> {
        panic!("store corrupt")
    }));
    injector.provide::<Session>(Provider::singleton(|inj: &Injector| {
        inj.try_resolve::<Store>().unwrap();
        Shared::new(Session)
    }));

    let err = injector
        .try_resolve::<Session>()
        .err()
        .expect("a panic resolved");
    assert_eq!(err.kind, ErrorKind::FactoryFailed, "{}", err.message);
    assert!(
        err.message.contains(std::any::type_name::<Session>())
            && err.message.contains("store corrupt"),
        "{}",
        err.message
    );
}

#[cfg(feature = "async-factory")]
mod async_factories {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn a_panicking_async_singleton_is_a_failure_of_that_binding_and_is_not_cached() {
        let runs = Arc::new(AtomicUsize::new(0));
        let injector = Injector::root();
        injector.provide::<Store>(Provider::singleton_async({
            let runs = runs.clone();
            move |_: Injector| {
                let runs = runs.clone();
                async move { Shared::new(Store(panics_on_first_run(&runs))) }
            }
        }));

        let err = block_on(injector.try_resolve_async::<Store>())
            .err()
            .expect("a panic resolved");
        assert_panicked(&err, std::any::type_name::<Store>(), "store corrupt");

        assert_eq!(
            block_on(injector.try_resolve_async::<Store>()).unwrap().0,
            2
        );
        assert_eq!(
            block_on(injector.try_resolve_async::<Store>()).unwrap().0,
            2
        );
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn an_async_factory_that_panics_before_returning_its_future_is_a_failure() {
        let injector = Injector::root();
        injector.provide::<Store>(Provider::singleton_async(
            |_: Injector| -> std::future::Ready<Shared<Store>> { panic!("store corrupt") },
        ));

        let err = block_on(injector.try_resolve_async::<Store>())
            .err()
            .expect("a panic resolved");
        assert_panicked(&err, std::any::type_name::<Store>(), "store corrupt");
    }

    #[test]
    fn a_panic_in_a_nested_async_factory_is_the_typed_source_of_the_outer_failure() {
        let injector = Injector::root();
        injector.provide::<Store>(Provider::singleton_async(|_: Injector| async {
            panic!("store corrupt");
            #[allow(unreachable_code)]
            Shared::new(Store(0))
        }));
        injector.provide::<Session>(Provider::root_try_async(|inj: Injector| async move {
            inj.try_resolve_async::<Store>().await?;
            Ok::<_, Error>(Shared::new(Session))
        }));

        let err = block_on(injector.try_resolve_async::<Session>())
            .err()
            .expect("a panic resolved");
        assert_eq!(err.kind, ErrorKind::FactoryFailed, "{}", err.message);
        let inner = std::iter::successors(Some(&err as &(dyn std::error::Error + 'static)), |e| {
            e.source()
        })
        .skip(1)
        .find_map(|e| e.downcast_ref::<Error>())
        .expect("no nested fluxdi Error");
        assert_panicked(inner, std::any::type_name::<Store>(), "store corrupt");
    }
}
