use super::*;

impl Injector {
    pub(crate) fn store_instance<T>(&self, instance: Shared<Instance<T>>)
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "lock-free")]
        {
            self.inner.instances.insert(type_id, instance);
        }

        #[cfg(not(feature = "lock-free"))]
        {
            self.inner
                .instances
                .write()
                .unwrap()
                .insert(type_id, instance);
        }

        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "instance_store",
            "Stored resolved instance in cache"
        );
    }

    pub(crate) fn store_set_instance<T>(
        &self,
        provider_ref: &Shared<Provider<T>>,
        instance: Shared<Instance<T>>,
    ) where
        T: ?Sized + Send + Sync + 'static,
    {
        let key = SetProviderKey::of::<T>(provider_ref);
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "lock-free")]
        {
            self.inner.set_instances.insert(key, instance);
        }

        #[cfg(not(feature = "lock-free"))]
        {
            self.inner
                .set_instances
                .write()
                .unwrap()
                .insert(key, instance);
        }

        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "instance_store_set",
            provider_ptr = key.provider_ptr,
            "Stored set-binding instance in cache"
        );
    }

    pub(crate) fn store_instance_named<T>(&self, name: &str, instance: Shared<Instance<T>>)
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let key = NamedTypeKey::of::<T>(name);
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "lock-free")]
        {
            self.inner.named_instances.insert(key, instance);
        }

        #[cfg(not(feature = "lock-free"))]
        {
            self.inner
                .named_instances
                .write()
                .unwrap()
                .insert(key, instance);
        }

        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            name = %name,
            op = "instance_store_named",
            "Stored named instance in cache"
        );
    }

    pub(crate) fn store_provider<T>(&self, provider: Provider<T>) -> Result<(), Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        let type_name = std::any::type_name::<T>();
        let scope = provider.scope;
        let scope_text = scope.to_string();
        let graph_meta = ProviderGraphMeta::of::<T>(scope, provider.dependency_hints.clone());

        #[cfg(feature = "tracing")]
        debug!(
            type_name = type_name,
            scope = %scope,
            op = "provider_store",
            "Storing provider definition"
        );

        #[cfg(feature = "lock-free")]
        {
            match self.inner.providers.entry(type_id) {
                DashEntry::Occupied(_) => {
                    #[cfg(feature = "tracing")]
                    debug!(
                        type_name = type_name,
                        scope = %scope,
                        op = "provider_store",
                        "Provider registration rejected: duplicate binding"
                    );
                    return Err(Error::provider_already_registered(
                        type_name,
                        scope_text.as_str(),
                    ));
                }
                DashEntry::Vacant(entry) => {
                    entry.insert(Shared::new(provider));
                }
            }
            self.inner.graph_providers.insert(type_id, graph_meta);
        }

        #[cfg(not(feature = "lock-free"))]
        {
            let mut providers = self.inner.providers.write().unwrap();
            if providers.contains_key(&type_id) {
                #[cfg(feature = "tracing")]
                debug!(
                    type_name = type_name,
                    scope = %scope,
                    op = "provider_store",
                    "Provider registration rejected: duplicate binding"
                );
                return Err(Error::provider_already_registered(
                    type_name,
                    scope_text.as_str(),
                ));
            }
            providers.insert(type_id, Shared::new(provider));
            drop(providers);
            self.inner
                .graph_providers
                .write()
                .unwrap()
                .insert(type_id, graph_meta);
        }

        #[cfg(feature = "eager-resolution")]
        {
            let node_id = format!("single::{}", type_name);
            let resolver: EagerResolverFn = Shared::new(
                move |inj: Injector| -> std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<(), Error>> + Send>,
                > {
                    Box::pin(async move { inj.try_resolve_async::<T>().await.map(|_| ()) })
                },
            );
            #[cfg(not(feature = "lock-free"))]
            self.inner
                .eager_resolvers
                .write()
                .unwrap()
                .insert(node_id, resolver);
            #[cfg(feature = "lock-free")]
            self.inner.eager_resolvers.insert(node_id, resolver);
        }

        #[cfg(feature = "tracing")]
        debug!(
            type_name = type_name,
            scope = %scope,
            op = "provider_store",
            "Provider definition stored"
        );

        Ok(())
    }

    /// Appends `provider` to this injector's set of `T` and returns it as
    /// stored.
    pub(crate) fn store_set_provider<T>(
        &self,
        provider: Provider<T>,
    ) -> Result<Shared<Provider<T>>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();
        #[cfg(feature = "tracing")]
        let scope = provider.scope;
        let graph_meta =
            ProviderGraphMeta::of::<T>(provider.scope, provider.dependency_hints.clone());
        let provider = Shared::new(provider);

        #[cfg(feature = "lock-free")]
        {
            match self.inner.set_providers.entry(type_id) {
                DashEntry::Occupied(mut entry) => {
                    entry.get_mut().push(provider.clone());
                }
                DashEntry::Vacant(entry) => {
                    entry.insert(vec![provider.clone()]);
                }
            }
            match self.inner.graph_set_providers.entry(type_id) {
                DashEntry::Occupied(mut entry) => {
                    entry.get_mut().push(graph_meta);
                }
                DashEntry::Vacant(entry) => {
                    entry.insert(vec![graph_meta]);
                }
            }
        }

        #[cfg(not(feature = "lock-free"))]
        {
            let mut providers = self.inner.set_providers.write().unwrap();
            providers.entry(type_id).or_default().push(provider.clone());
            drop(providers);
            self.inner
                .graph_set_providers
                .write()
                .unwrap()
                .entry(type_id)
                .or_default()
                .push(graph_meta);
        }

        #[cfg(feature = "tracing")]
        debug!(
            type_name = type_name,
            scope = %scope,
            op = "provider_store_set",
            "Provider appended to set binding"
        );

        Ok(provider)
    }
}
