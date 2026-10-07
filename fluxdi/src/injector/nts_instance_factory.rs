use super::*;

impl Injector {
    pub(crate) fn resolve_instance<T>(&self) -> Result<Shared<Instance<T>>, Error>
    where
        T: ?Sized + 'static,
    {
        #[cfg(feature = "metrics")]
        self.inner.metrics.record_factory_execution();

        let type_name = std::any::type_name::<T>();
        #[cfg(feature = "tracing")]
        let _span = info_span!(SPAN_FACTORY_EXECUTE, type_name = type_name).entered();

        let provider_ref = self.resolve_provider::<T>()?;
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_sync",
            scope = %provider_ref.scope,
            limit_policy = ?provider_ref.limits.policy,
            limit_max = ?provider_ref.limits.max_concurrent_creations,
            "Acquiring factory creation permit"
        );
        let permit = provider_ref.acquire_creation_permit(type_name)?;
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_sync",
            stage = "permit_acquired",
            "Factory permit acquired"
        );

        #[cfg(feature = "async-factory")]
        if provider_ref.async_factory.is_some() {
            drop(permit);
            #[cfg(feature = "tracing")]
            debug!(
                type_name = type_name,
                op = "factory_execute_sync",
                "Sync resolve attempted on async provider"
            );
            return Err(Error::async_factory_requires_async_resolve(type_name));
        }

        let instance = Shared::new((provider_ref.factory)(self));
        drop(permit);
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_sync",
            scope = %provider_ref.scope,
            "Factory execution completed"
        );
        Ok(instance)
    }

    pub(crate) fn resolve_instance_from_provider<T>(
        &self,
        provider_ref: &Shared<Provider<T>>,
    ) -> Result<Shared<Instance<T>>, Error>
    where
        T: ?Sized + 'static,
    {
        #[cfg(feature = "metrics")]
        self.inner.metrics.record_factory_execution();

        let type_name = std::any::type_name::<T>();
        #[cfg(feature = "tracing")]
        let _span = info_span!(
            SPAN_FACTORY_EXECUTE,
            type_name = type_name,
            op = "resolve_set"
        )
        .entered();

        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_sync_set",
            scope = %provider_ref.scope,
            limit_policy = ?provider_ref.limits.policy,
            limit_max = ?provider_ref.limits.max_concurrent_creations,
            "Acquiring factory creation permit for set binding"
        );
        let permit = provider_ref.acquire_creation_permit(type_name)?;
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_sync_set",
            stage = "permit_acquired",
            "Factory permit acquired for set binding"
        );

        #[cfg(feature = "async-factory")]
        if provider_ref.async_factory.is_some() {
            drop(permit);
            #[cfg(feature = "tracing")]
            debug!(
                type_name = type_name,
                op = "factory_execute_sync_set",
                "Sync resolve attempted on async provider in set"
            );
            return Err(Error::async_factory_requires_async_resolve(type_name));
        }

        let instance = Shared::new((provider_ref.factory)(self));
        drop(permit);
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_sync_set",
            scope = %provider_ref.scope,
            "Factory execution completed for set binding"
        );
        Ok(instance)
    }

    pub(crate) fn resolve_instance_named<T>(&self, name: &str) -> Result<Shared<Instance<T>>, Error>
    where
        T: ?Sized + 'static,
    {
        #[cfg(feature = "metrics")]
        self.inner.metrics.record_factory_execution();

        let type_name = std::any::type_name::<T>();
        #[cfg(feature = "tracing")]
        let _span = info_span!(SPAN_FACTORY_EXECUTE, type_name = type_name, name = %name).entered();

        let provider_ref = self.resolve_provider_named::<T>(name)?;
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            name = %name,
            op = "factory_execute_sync_named",
            scope = %provider_ref.scope,
            limit_policy = ?provider_ref.limits.policy,
            limit_max = ?provider_ref.limits.max_concurrent_creations,
            "Acquiring factory creation permit for named binding"
        );
        let permit = provider_ref.acquire_creation_permit(type_name)?;
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            name = %name,
            op = "factory_execute_sync_named",
            stage = "permit_acquired",
            "Factory permit acquired for named binding"
        );

        #[cfg(feature = "async-factory")]
        if provider_ref.async_factory.is_some() {
            drop(permit);
            #[cfg(feature = "tracing")]
            debug!(
                type_name = type_name,
                name = %name,
                op = "factory_execute_sync_named",
                "Sync resolve attempted on async named provider"
            );
            return Err(Error::async_factory_requires_async_resolve(type_name));
        }

        let instance = Shared::new((provider_ref.factory)(self));
        drop(permit);
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            name = %name,
            op = "factory_execute_sync_named",
            scope = %provider_ref.scope,
            "Factory execution completed for named binding"
        );
        Ok(instance)
    }

    #[cfg(feature = "async-factory")]
    pub(crate) async fn resolve_instance_async<T>(&self) -> Result<Shared<Instance<T>>, Error>
    where
        T: ?Sized + 'static,
    {
        #[cfg(feature = "metrics")]
        self.inner.metrics.record_factory_execution();

        #[cfg(feature = "tracing")]
        let _span =
            info_span!(SPAN_FACTORY_EXECUTE, type_name = std::any::type_name::<T>()).entered();

        let type_name = std::any::type_name::<T>();
        let provider_ref = self.resolve_provider::<T>()?;
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_async",
            scope = %provider_ref.scope,
            limit_policy = ?provider_ref.limits.policy,
            limit_max = ?provider_ref.limits.max_concurrent_creations,
            "Acquiring async factory creation permit"
        );
        let permit = provider_ref
            .acquire_creation_permit_async(type_name)
            .await?;
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_async",
            stage = "permit_acquired",
            "Async factory permit acquired"
        );

        if let Some(async_factory) = &provider_ref.async_factory {
            let instance = Shared::new((async_factory)(self.clone()).await?);
            drop(permit);
            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                op = "factory_execute_async",
                scope = %provider_ref.scope,
                mode = "async_factory",
                "Async factory execution completed"
            );
            return Ok(instance);
        }

        let instance = Shared::new((provider_ref.factory)(self));
        drop(permit);
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_async",
            scope = %provider_ref.scope,
            mode = "sync_factory",
            "Sync fallback factory execution completed in async resolve"
        );
        Ok(instance)
    }

    #[cfg(feature = "async-factory")]
    pub(crate) async fn resolve_instance_named_async<T>(
        &self,
        name: &str,
    ) -> Result<Shared<Instance<T>>, Error>
    where
        T: ?Sized + 'static,
    {
        #[cfg(feature = "metrics")]
        self.inner.metrics.record_factory_execution();

        #[cfg(feature = "tracing")]
        let _span =
            info_span!(SPAN_FACTORY_EXECUTE, type_name = std::any::type_name::<T>(), name = %name)
                .entered();

        let type_name = std::any::type_name::<T>();
        let provider_ref = self.resolve_provider_named::<T>(name)?;
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            name = %name,
            op = "factory_execute_async_named",
            scope = %provider_ref.scope,
            limit_policy = ?provider_ref.limits.policy,
            limit_max = ?provider_ref.limits.max_concurrent_creations,
            "Acquiring async factory creation permit for named binding"
        );
        let permit = provider_ref
            .acquire_creation_permit_async(type_name)
            .await?;
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            name = %name,
            op = "factory_execute_async_named",
            stage = "permit_acquired",
            "Async factory permit acquired for named binding"
        );

        if let Some(async_factory) = &provider_ref.async_factory {
            let instance = Shared::new((async_factory)(self.clone()).await?);
            drop(permit);
            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                name = %name,
                op = "factory_execute_async_named",
                scope = %provider_ref.scope,
                mode = "async_factory",
                "Async named factory execution completed"
            );
            return Ok(instance);
        }

        let instance = Shared::new((provider_ref.factory)(self));
        drop(permit);
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            name = %name,
            op = "factory_execute_async_named",
            scope = %provider_ref.scope,
            mode = "sync_factory",
            "Sync fallback named factory execution completed in async resolve"
        );
        Ok(instance)
    }

    #[cfg(feature = "async-factory")]
    pub(crate) async fn resolve_instance_from_provider_async<T>(
        &self,
        provider_ref: &Shared<Provider<T>>,
    ) -> Result<Shared<Instance<T>>, Error>
    where
        T: ?Sized + 'static,
    {
        #[cfg(feature = "metrics")]
        self.inner.metrics.record_factory_execution();

        #[cfg(feature = "tracing")]
        let _span = info_span!(
            SPAN_FACTORY_EXECUTE,
            type_name = std::any::type_name::<T>(),
            op = "resolve_set"
        )
        .entered();

        let type_name = std::any::type_name::<T>();
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_async_set",
            scope = %provider_ref.scope,
            limit_policy = ?provider_ref.limits.policy,
            limit_max = ?provider_ref.limits.max_concurrent_creations,
            "Acquiring async factory creation permit for set binding"
        );
        let permit = provider_ref
            .acquire_creation_permit_async(type_name)
            .await?;
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_async_set",
            stage = "permit_acquired",
            "Async factory permit acquired for set binding"
        );

        if let Some(async_factory) = &provider_ref.async_factory {
            let instance = Shared::new((async_factory)(self.clone()).await?);
            drop(permit);
            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                op = "factory_execute_async_set",
                scope = %provider_ref.scope,
                mode = "async_factory",
                "Async set factory execution completed"
            );
            return Ok(instance);
        }

        let instance = Shared::new((provider_ref.factory)(self));
        drop(permit);
        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "factory_execute_async_set",
            scope = %provider_ref.scope,
            mode = "sync_factory",
            "Sync fallback set factory execution completed in async resolve"
        );
        Ok(instance)
    }
}
