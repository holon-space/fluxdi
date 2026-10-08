use super::*;

impl Injector {
    pub fn try_provide<T>(&self, provider: Provider<T>) -> Result<(), Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let scope = provider.scope;

        #[cfg(feature = "metrics")]
        self.inner.metrics.record_provide_attempt();

        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "tracing")]
        let _span = info_span!(SPAN_PROVIDE, type_name = type_name, scope = %scope).entered();

        let result = match scope {
            Scope::Root => {
                let root = self.root_injector();
                root.store_provider::<T>(provider)
            }

            Scope::Module | Scope::Scoped | Scope::Transient => self.store_provider::<T>(provider),
        };

        #[cfg(feature = "tracing")]
        match &result {
            Ok(_) => debug!(
                type_name = type_name,
                scope = %scope,
                op = "provide",
                "Provider registered"
            ),
            Err(error) => debug!(
                type_name = type_name,
                scope = %scope,
                op = "provide",
                error = %error,
                "Provider registration failed"
            ),
        }

        #[cfg(feature = "metrics")]
        match &result {
            Ok(_) => self.inner.metrics.record_provide_success(),
            Err(_) => self.inner.metrics.record_provide_failure(),
        }

        result
    }

    pub fn provide<T>(&self, provider: Provider<T>) -> &Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_provide::<T>(provider).unwrap();
        self
    }

    pub fn try_provide_into_set<T>(&self, provider: Provider<T>) -> Result<(), Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let scope = provider.scope;

        #[cfg(feature = "metrics")]
        self.inner.metrics.record_provide_attempt();

        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "tracing")]
        let _span =
            info_span!(SPAN_PROVIDE, type_name = type_name, scope = %scope, op = "provide_set")
                .entered();

        let store = match scope {
            Scope::Root => self.root_injector(),
            Scope::Module | Scope::Scoped | Scope::Transient => self.clone(),
        };
        #[cfg(feature = "live")]
        let result = store.store_set_provider_live::<T>(provider);
        #[cfg(not(feature = "live"))]
        let result = store.store_set_provider::<T>(provider).map(drop);

        #[cfg(feature = "tracing")]
        match &result {
            Ok(_) => debug!(
                type_name = type_name,
                scope = %scope,
                op = "provide_set",
                "Provider appended into set"
            ),
            Err(error) => debug!(
                type_name = type_name,
                scope = %scope,
                op = "provide_set",
                error = %error,
                "Set provider registration failed"
            ),
        }

        #[cfg(feature = "metrics")]
        match &result {
            Ok(_) => self.inner.metrics.record_provide_success(),
            Err(_) => self.inner.metrics.record_provide_failure(),
        }

        result
    }

    pub fn provide_into_set<T>(&self, provider: Provider<T>) -> &Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_provide_into_set::<T>(provider).unwrap();
        self
    }

    pub fn try_provide_named<T>(
        &self,
        name: impl Into<String>,
        provider: Provider<T>,
    ) -> Result<(), Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let scope = provider.scope;
        let name = name.into();

        #[cfg(feature = "metrics")]
        self.inner.metrics.record_provide_attempt();

        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "tracing")]
        let _span =
            info_span!(SPAN_PROVIDE, type_name = type_name, scope = %scope, name = %name).entered();

        let result = match scope {
            Scope::Root => {
                let root = self.root_injector();
                root.store_named_provider::<T>(name.as_str(), provider)
            }

            Scope::Module | Scope::Scoped | Scope::Transient => {
                self.store_named_provider::<T>(name.as_str(), provider)
            }
        };

        #[cfg(feature = "tracing")]
        match &result {
            Ok(_) => debug!(
                type_name = type_name,
                scope = %scope,
                name = %name,
                op = "provide_named",
                "Named provider registered"
            ),
            Err(error) => debug!(
                type_name = type_name,
                scope = %scope,
                name = %name,
                op = "provide_named",
                error = %error,
                "Named provider registration failed"
            ),
        }

        #[cfg(feature = "metrics")]
        match &result {
            Ok(_) => self.inner.metrics.record_provide_success(),
            Err(_) => self.inner.metrics.record_provide_failure(),
        }

        result
    }

    pub fn provide_named<T>(&self, name: impl Into<String>, provider: Provider<T>) -> &Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_provide_named::<T>(name, provider).unwrap();
        self
    }

    pub fn try_override_provider<T>(&self, provider: Provider<T>) -> Result<(), Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let scope = provider.scope;

        #[cfg(feature = "metrics")]
        self.inner.metrics.record_provide_attempt();

        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "tracing")]
        let _span =
            info_span!(SPAN_PROVIDE, type_name = type_name, scope = %scope, op = "override")
                .entered();

        let result = match scope {
            Scope::Root => {
                let root = self.root_injector();
                root.replace_provider::<T>(provider)
            }

            Scope::Module | Scope::Scoped | Scope::Transient => {
                self.replace_provider::<T>(provider)
            }
        };

        #[cfg(feature = "tracing")]
        match &result {
            Ok(_) => debug!(
                type_name = type_name,
                scope = %scope,
                op = "override_provider",
                "Provider overridden"
            ),
            Err(error) => debug!(
                type_name = type_name,
                scope = %scope,
                op = "override_provider",
                error = %error,
                "Provider override failed"
            ),
        }

        #[cfg(feature = "metrics")]
        match &result {
            Ok(_) => self.inner.metrics.record_provide_success(),
            Err(_) => self.inner.metrics.record_provide_failure(),
        }

        result
    }

    pub fn override_provider<T>(&self, provider: Provider<T>) -> &Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_override_provider::<T>(provider).unwrap();
        self
    }
}
