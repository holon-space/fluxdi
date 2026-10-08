use super::*;

impl Injector {
    pub fn try_resolve<T>(&self) -> Result<Shared<T>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();
        #[cfg(feature = "tracing")]
        let _span = info_span!(SPAN_RESOLVE, type_name = type_name).entered();

        #[cfg(feature = "metrics")]
        self.inner.metrics.record_resolve_attempt();
        #[cfg(feature = "metrics")]
        let resolve_started = std::time::Instant::now();

        let result = (|| {
            // Fast path: cache hit skips ResolveGuard (no recursion, no circular dep possible)
            if let Some(instance) = self.get_instance::<T>() {
                #[cfg(feature = "tracing")]
                trace!(
                    type_name = type_name,
                    op = "resolve_sync",
                    cache = "hit",
                    "Resolve cache hit"
                );
                #[cfg(feature = "metrics")]
                self.inner.metrics.record_resolve_cache_hit();
                return Ok(instance.value());
            }

            let _guard = ResolveGuard::push(type_id)?;

            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                op = "resolve_sync",
                cache = "miss",
                stage = "create_instance",
                "Resolve cache miss, creating instance"
            );
            #[cfg(feature = "metrics")]
            self.inner.metrics.record_resolve_cache_miss();

            let provider = self.resolve_provider::<T>()?;

            let instance = self.resolve_instance::<T>()?;

            if provider.scope == Scope::Transient {
                #[cfg(feature = "tracing")]
                trace!(
                    type_name = type_name,
                    op = "resolve_sync",
                    scope = %provider.scope,
                    cached = false,
                    "Resolved transient service"
                );
                return Ok(instance.value());
            }

            if let Some(target) = self.cache_target_for_scope(provider.scope) {
                target.store_instance::<T>(instance.clone());
            }

            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                op = "resolve_sync",
                scope = %provider.scope,
                cached = true,
                "Resolved and cached service"
            );

            Ok(instance.value())
        })();

        #[cfg(feature = "metrics")]
        match &result {
            Ok(_) => self
                .inner
                .metrics
                .record_resolve_success(resolve_started.elapsed()),
            Err(_) => self
                .inner
                .metrics
                .record_resolve_failure(resolve_started.elapsed()),
        }

        result
    }

    pub fn try_resolve_all<T>(&self) -> Result<Vec<Shared<T>>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();
        #[cfg(feature = "tracing")]
        let _span = info_span!(SPAN_RESOLVE, type_name = type_name, op = "resolve_all").entered();

        #[cfg(feature = "metrics")]
        self.inner.metrics.record_resolve_attempt();
        #[cfg(feature = "metrics")]
        let resolve_started = std::time::Instant::now();

        let result = (|| {
            let _guard = ResolveGuard::push(type_id)?;

            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                op = "resolve_all_sync",
                stage = "start",
                "Resolving service set"
            );

            let providers = self.resolve_set_providers::<T>()?;
            let mut values = Vec::with_capacity(providers.len());

            #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
            for (index, provider) in providers.into_iter().enumerate() {
                let cache_target = self.cache_target_for_scope(provider.scope);

                if let Some(target) = &cache_target {
                    if let Some(instance) = target.get_set_instance::<T>(&provider) {
                        #[cfg(feature = "tracing")]
                        trace!(
                            type_name = type_name,
                            op = "resolve_all_sync",
                            index = index,
                            scope = %provider.scope,
                            cache = "hit",
                            "Resolved set item from cache"
                        );
                        #[cfg(feature = "metrics")]
                        self.inner.metrics.record_resolve_cache_hit();
                        values.push(instance.value());
                        continue;
                    }

                    #[cfg(feature = "tracing")]
                    trace!(
                        type_name = type_name,
                        op = "resolve_all_sync",
                        index = index,
                        scope = %provider.scope,
                        cache = "miss",
                        "Set item cache miss, creating instance"
                    );
                    #[cfg(feature = "metrics")]
                    self.inner.metrics.record_resolve_cache_miss();
                }

                let instance = self.resolve_instance_from_provider::<T>(&provider)?;

                if let Some(target) = cache_target {
                    target.store_set_instance::<T>(&provider, instance.clone());
                }

                values.push(instance.value());
            }

            Ok(values)
        })();

        #[cfg(feature = "metrics")]
        match &result {
            Ok(_) => self
                .inner
                .metrics
                .record_resolve_success(resolve_started.elapsed()),
            Err(_) => self
                .inner
                .metrics
                .record_resolve_failure(resolve_started.elapsed()),
        }

        result
    }

    pub fn try_resolve_named<T>(&self, name: &str) -> Result<Shared<T>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();
        #[cfg(feature = "tracing")]
        let _span = info_span!(SPAN_RESOLVE, type_name = type_name, name = %name).entered();

        #[cfg(feature = "metrics")]
        self.inner.metrics.record_resolve_attempt();
        #[cfg(feature = "metrics")]
        let resolve_started = std::time::Instant::now();

        let result = (|| {
            let _guard = ResolveGuard::push(type_id)?;

            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                name = %name,
                op = "resolve_named_sync",
                stage = "start",
                "Resolving named service"
            );

            if let Some(instance) = self.get_instance_named::<T>(name) {
                #[cfg(feature = "tracing")]
                trace!(
                    type_name = type_name,
                    name = %name,
                    op = "resolve_named_sync",
                    cache = "hit",
                    "Named resolve cache hit"
                );
                #[cfg(feature = "metrics")]
                self.inner.metrics.record_resolve_cache_hit();
                return Ok(instance.value());
            }

            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                name = %name,
                op = "resolve_named_sync",
                cache = "miss",
                stage = "create_instance",
                "Named resolve cache miss, creating instance"
            );
            #[cfg(feature = "metrics")]
            self.inner.metrics.record_resolve_cache_miss();

            let provider = self.resolve_provider_named::<T>(name)?;
            let instance = self.resolve_instance_named::<T>(name)?;

            if provider.scope == Scope::Transient {
                #[cfg(feature = "tracing")]
                trace!(
                    type_name = type_name,
                    name = %name,
                    op = "resolve_named_sync",
                    scope = %provider.scope,
                    cached = false,
                    "Resolved named transient service"
                );
                return Ok(instance.value());
            }

            if let Some(target) = self.cache_target_for_scope(provider.scope) {
                target.store_instance_named::<T>(name, instance.clone());
            }

            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                name = %name,
                op = "resolve_named_sync",
                scope = %provider.scope,
                cached = true,
                "Resolved and cached named service"
            );

            Ok(instance.value())
        })();

        #[cfg(feature = "metrics")]
        match &result {
            Ok(_) => self
                .inner
                .metrics
                .record_resolve_success(resolve_started.elapsed()),
            Err(_) => self
                .inner
                .metrics
                .record_resolve_failure(resolve_started.elapsed()),
        }

        result
    }

    pub fn resolve<T>(&self) -> Shared<T>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_resolve::<T>().unwrap()
    }

    pub fn resolve_all<T>(&self) -> Vec<Shared<T>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_resolve_all::<T>().unwrap()
    }

    pub fn resolve_named<T>(&self, name: &str) -> Shared<T>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_resolve_named::<T>(name).unwrap()
    }

    /// Resolves `T`, or returns `None` when the resolve fails for any
    /// reason, including a failed factory. [`try_optional_resolve`](Self::try_optional_resolve)
    /// returns `None` only when no provider of `T` is registered.
    #[deprecated(note = "swallows factory failures; use try_optional_resolve*")]
    pub fn optional_resolve<T>(&self) -> Option<Shared<T>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_resolve::<T>().ok()
    }

    /// Resolves `T`; `Ok(None)` only when no provider of `T` is registered.
    /// Every other failure, such as a failed factory, is returned.
    pub fn try_optional_resolve<T>(&self) -> Result<Option<Shared<T>>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        if self.get_provider::<T>().is_none() {
            return Ok(None);
        }
        self.try_resolve::<T>().map(Some)
    }

    /// Resolves every member of the `T` set, or returns `None` when the resolve fails for any
    /// reason, including a failed factory. [`try_optional_resolve_all`](Self::try_optional_resolve_all)
    /// returns `None` only when no set provider of `T` is registered.
    #[deprecated(note = "swallows factory failures; use try_optional_resolve*")]
    pub fn optional_resolve_all<T>(&self) -> Option<Vec<Shared<T>>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_resolve_all::<T>().ok()
    }

    /// Resolves every member of the `T` set; `Ok(None)` only when no set provider of `T` is registered.
    /// Every other failure, such as a failed factory, is returned.
    pub fn try_optional_resolve_all<T>(&self) -> Result<Option<Vec<Shared<T>>>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let mut providers = Vec::new();
        self.collect_set_providers::<T>(&mut providers)?;
        if providers.is_empty() {
            return Ok(None);
        }
        self.try_resolve_all::<T>().map(Some)
    }

    /// Resolves `T` named `name`, or returns `None` when the resolve fails for any
    /// reason, including a failed factory. [`try_optional_resolve_named`](Self::try_optional_resolve_named)
    /// returns `None` only when no provider of `T` is registered under `name`.
    #[deprecated(note = "swallows factory failures; use try_optional_resolve*")]
    pub fn optional_resolve_named<T>(&self, name: &str) -> Option<Shared<T>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_resolve_named::<T>(name).ok()
    }

    /// Resolves `T` named `name`; `Ok(None)` only when no provider of `T` is registered under `name`.
    /// Every other failure, such as a failed factory, is returned.
    pub fn try_optional_resolve_named<T>(&self, name: &str) -> Result<Option<Shared<T>>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        if self.get_provider_named::<T>(name).is_none() {
            return Ok(None);
        }
        self.try_resolve_named::<T>(name).map(Some)
    }
}
