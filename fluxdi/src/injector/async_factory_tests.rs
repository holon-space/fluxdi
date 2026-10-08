use super::*;
use crate::{ErrorKind, Provider, Shared};

use futures::executor::block_on;
#[cfg(feature = "thread-safe")]
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};
#[cfg(feature = "thread-safe")]
use std::thread;
#[cfg(feature = "thread-safe")]
use std::time::Duration;

#[test]
fn sync_resolve_rejects_async_provider() {
    let injector = Injector::root();
    injector.provide::<String>(Provider::transient_async(|_| async {
        Shared::new("hello".to_string())
    }));

    let err = injector.try_resolve::<String>().unwrap_err();
    assert_eq!(err.kind, ErrorKind::AsyncFactoryRequiresAsyncResolve);
}

#[test]
fn async_resolve_handles_async_provider() {
    let injector = Injector::root();
    injector.provide::<String>(Provider::transient_async(|_| async {
        Shared::new("async".to_string())
    }));

    let value = block_on(injector.try_resolve_async::<String>()).unwrap();
    assert_eq!(value.as_str(), "async");
}

#[test]
fn async_resolve_handles_sync_provider() {
    let injector = Injector::root();
    injector.provide::<u32>(Provider::transient(|_| Shared::new(42u32)));

    let value = block_on(injector.try_resolve_async::<u32>()).unwrap();
    assert_eq!(*value, 42);
}

#[test]
fn async_root_provider_is_cached() {
    let injector = Injector::root();
    injector.provide::<String>(Provider::root_async(|_| async {
        Shared::new("cached".to_string())
    }));

    let first = block_on(injector.try_resolve_async::<String>()).unwrap();
    let second = block_on(injector.try_resolve_async::<String>()).unwrap();
    assert!(Shared::ptr_eq(&first, &second));
}

#[test]
fn async_transient_provider_is_not_cached() {
    let injector = Injector::root();
    injector.provide::<String>(Provider::transient_async(|_| async {
        Shared::new("new".to_string())
    }));

    let first = block_on(injector.try_resolve_async::<String>()).unwrap();
    let second = block_on(injector.try_resolve_async::<String>()).unwrap();
    assert!(!Shared::ptr_eq(&first, &second));
}

#[test]
fn optional_resolve_async_returns_none_for_missing_service() {
    let injector = Injector::root();
    let value = block_on(injector.optional_resolve_async::<String>());
    assert!(value.is_none());
}

#[cfg(feature = "thread-safe")]
#[test]
fn concurrent_async_transient_resolve_returns_unique_values() {
    let injector = Arc::new(Injector::root());
    let counter = Arc::new(AtomicUsize::new(0));

    injector.provide::<usize>(Provider::transient_async({
        let counter = Arc::clone(&counter);
        move |_| {
            let counter = Arc::clone(&counter);
            async move { Shared::new(counter.fetch_add(1, Ordering::SeqCst)) }
        }
    }));

    let workers = 8;
    let barrier = Arc::new(Barrier::new(workers));
    let handles: Vec<_> = (0..workers)
        .map(|_| {
            let injector = Arc::clone(&injector);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                *block_on(injector.try_resolve_async::<usize>()).unwrap()
            })
        })
        .collect();

    let mut values: Vec<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    values.sort_unstable();
    values.dedup();
    assert_eq!(values.len(), workers);
}

#[cfg(feature = "thread-safe")]
#[test]
fn concurrent_async_root_resolve_completes_and_caches() {
    let injector = Arc::new(Injector::root());
    let creations = Arc::new(AtomicUsize::new(0));

    injector.provide::<usize>(Provider::root_async({
        let creations = Arc::clone(&creations);
        move |_| {
            let creations = Arc::clone(&creations);
            async move {
                creations.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(5));
                Shared::new(7usize)
            }
        }
    }));

    let workers = 8;
    let barrier = Arc::new(Barrier::new(workers));
    let handles: Vec<_> = (0..workers)
        .map(|_| {
            let injector = Arc::clone(&injector);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                *block_on(injector.try_resolve_async::<usize>()).unwrap()
            })
        })
        .collect();

    for value in handles.into_iter().map(|h| h.join().unwrap()) {
        assert_eq!(value, 7);
    }

    let created = creations.load(Ordering::SeqCst);
    assert!(created >= 1);
    assert!(created <= workers);

    let first = block_on(injector.try_resolve_async::<usize>()).unwrap();
    let second = block_on(injector.try_resolve_async::<usize>()).unwrap();
    assert!(Shared::ptr_eq(&first, &second));
}

#[test]
fn decorators_wrap_an_async_provider_in_order() {
    let injector = Injector::root();
    injector.provide::<String>(
        Provider::transient_async(|_| async { Shared::new("base".to_string()) })
            .with_decorator(|inner| Shared::new(format!("[{inner}]")))
            .with_decorator(|inner| Shared::new(format!("({inner})"))),
    );

    let value = block_on(injector.try_resolve_async::<String>()).unwrap();
    assert_eq!(value.as_str(), "([base])");
}

#[test]
fn a_decorator_wraps_a_fallible_async_set_member() {
    let injector = Injector::root();
    injector.provide_into_set::<String>(
        Provider::singleton_try_async(|_| async {
            Ok::<_, std::io::Error>(Shared::new("member".to_string()))
        })
        .with_decorator(|inner| Shared::new(format!("<{inner}>"))),
    );

    let values = block_on(injector.try_resolve_all_async::<String>()).unwrap();
    let values: Vec<&str> = values.iter().map(|value| value.as_str()).collect();
    assert_eq!(values, vec!["<member>"]);
}
