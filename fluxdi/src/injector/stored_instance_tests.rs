use super::*;

/// Puts an `Instance<u32>` where the cache keeps the `Instance<String>`.
fn cache_a_mismatched_instance(injector: &Injector, name: Option<&str>) {
    let stored = Shared::new(Instance::new(Shared::new(7u32)));
    match name {
        None => {
            let key = TypeId::of::<String>();
            #[cfg(not(feature = "thread-safe"))]
            injector.inner.instances.borrow_mut().insert(key, stored);
            #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
            injector
                .inner
                .instances
                .write()
                .unwrap()
                .insert(key, stored);
            #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
            injector.inner.instances.insert(key, stored);
        }
        Some(name) => {
            let key = NamedTypeKey::of::<String>(name);
            #[cfg(not(feature = "thread-safe"))]
            injector
                .inner
                .named_instances
                .borrow_mut()
                .insert(key, stored);
            #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
            injector
                .inner
                .named_instances
                .write()
                .unwrap()
                .insert(key, stored);
            #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
            injector.inner.named_instances.insert(key, stored);
        }
    }
}

#[test]
#[should_panic(expected = "is not an Instance<alloc::string::String>")]
fn a_cached_instance_of_another_type_panics() {
    let injector = Injector::root();
    cache_a_mismatched_instance(&injector, None);
    injector.get_instance::<String>();
}

#[test]
#[should_panic(expected = "is not an Instance<alloc::string::String>")]
fn a_cached_named_instance_of_another_type_panics() {
    let injector = Injector::root();
    cache_a_mismatched_instance(&injector, Some("primary"));
    injector.get_instance_named::<String>("primary");
}

#[test]
fn an_uncached_instance_stays_absent() {
    let injector = Injector::root();
    assert!(injector.get_instance::<String>().is_none());
    assert!(injector.get_instance_named::<String>("primary").is_none());
}
