use super::*;

impl Injector {
    pub(crate) fn store_named_provider<T>(
        &self,
        name: &str,
        provider: Provider<T>,
    ) -> Result<(), Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let key = NamedTypeKey::of::<T>(name);
        let type_name = std::any::type_name::<T>();
        let scope = provider.scope;
        let scope_text = scope.to_string();
        let graph_meta = ProviderGraphMeta::of::<T>(scope, provider.dependency_hints.clone());

        #[cfg(feature = "tracing")]
        debug!(
            type_name = type_name,
            name = %name,
            scope = %scope,
            op = "provider_store_named",
            "Storing named provider definition"
        );

        #[cfg(feature = "lock-free")]
        {
            match self.inner.named_providers.entry(key.clone()) {
                DashEntry::Occupied(_) => {
                    #[cfg(feature = "tracing")]
                    debug!(
                        type_name = type_name,
                        name = %name,
                        scope = %scope,
                        op = "provider_store_named",
                        "Named provider registration rejected: duplicate binding"
                    );
                    return Err(Error::provider_already_registered_named(
                        type_name,
                        name,
                        scope_text.as_str(),
                    ));
                }
                DashEntry::Vacant(entry) => {
                    entry.insert(Shared::new(provider));
                }
            }
            self.inner.graph_named_providers.insert(key, graph_meta);
        }

        #[cfg(not(feature = "lock-free"))]
        {
            let mut providers = self.inner.named_providers.write().unwrap();
            if providers.contains_key(&key) {
                #[cfg(feature = "tracing")]
                debug!(
                    type_name = type_name,
                    name = %name,
                    scope = %scope,
                    op = "provider_store_named",
                    "Named provider registration rejected: duplicate binding"
                );
                return Err(Error::provider_already_registered_named(
                    type_name,
                    name,
                    scope_text.as_str(),
                ));
            }
            providers.insert(key.clone(), Shared::new(provider));
            drop(providers);
            self.inner
                .graph_named_providers
                .write()
                .unwrap()
                .insert(key, graph_meta);
        }

        #[cfg(feature = "eager-resolution")]
        {
            let node_id = format!("named::{}::{}", type_name, name);
            let name_owned = name.to_string();
            let resolver: EagerResolverFn = Shared::new(
                move |inj: Injector| -> std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<(), Error>> + Send>,
                > {
                    let name = name_owned.clone();
                    Box::pin(
                        async move { inj.try_resolve_named_async::<T>(&name).await.map(|_| ()) },
                    )
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
            name = %name,
            scope = %scope,
            op = "provider_store_named",
            "Named provider definition stored"
        );

        Ok(())
    }

    pub(crate) fn replace_provider<T>(&self, provider: Provider<T>) -> Result<(), Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        let type_name = std::any::type_name::<T>();
        #[cfg(feature = "tracing")]
        let scope = provider.scope;
        let graph_meta =
            ProviderGraphMeta::of::<T>(provider.scope, provider.dependency_hints.clone());

        #[cfg(feature = "tracing")]
        debug!(
            type_name = type_name,
            scope = %scope,
            op = "provider_replace",
            "Replacing provider definition"
        );

        #[cfg(feature = "lock-free")]
        {
            match self.inner.providers.entry(type_id) {
                DashEntry::Occupied(mut entry) => {
                    entry.insert(Shared::new(provider));
                }
                DashEntry::Vacant(_) => {
                    #[cfg(feature = "tracing")]
                    debug!(
                        type_name = type_name,
                        scope = %scope,
                        op = "provider_replace",
                        "Provider replace rejected: no previous binding"
                    );
                    return Err(Error::service_not_provided_for_override(type_name));
                }
            }
            self.inner.graph_providers.insert(type_id, graph_meta);
        }

        #[cfg(not(feature = "lock-free"))]
        {
            let mut providers = self.inner.providers.write().unwrap();
            if !providers.contains_key(&type_id) {
                #[cfg(feature = "tracing")]
                debug!(
                    type_name = type_name,
                    scope = %scope,
                    op = "provider_replace",
                    "Provider replace rejected: no previous binding"
                );
                return Err(Error::service_not_provided_for_override(type_name));
            }
            providers.insert(type_id, Shared::new(provider));
            drop(providers);
            self.inner
                .graph_providers
                .write()
                .unwrap()
                .insert(type_id, graph_meta);
        }

        self.clear_instance_cache::<T>();

        #[cfg(feature = "tracing")]
        debug!(
            type_name = type_name,
            scope = %scope,
            op = "provider_replace",
            "Provider definition replaced and cache invalidated"
        );

        Ok(())
    }

    pub(crate) fn clear_instance_cache<T>(&self)
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "lock-free")]
        {
            self.inner.instances.remove(&type_id);
        }

        #[cfg(not(feature = "lock-free"))]
        {
            self.inner.instances.write().unwrap().remove(&type_id);
        }

        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "instance_cache_clear",
            "Cleared cached instance for type"
        );
    }

    pub(crate) fn get_instance<T>(&self) -> Option<Shared<Instance<T>>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "lock-free")]
        let local = self
            .inner
            .instances
            .get(&type_id)
            .map(|value| value.value().clone());
        #[cfg(not(feature = "lock-free"))]
        let local = self.inner.instances.read().unwrap().get(&type_id).cloned();

        if local.is_some() {
            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                op = "instance_lookup",
                source = "local",
                hit = true,
                "Instance cache hit"
            );
            return local.map(stored_instance::<T>);
        }

        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "instance_lookup",
            source = "local",
            hit = false,
            has_parent = self.inner.parent.is_some(),
            "Instance cache miss"
        );

        if let Some(parent) = &self.inner.parent {
            let parent_injector = Injector {
                inner: parent.clone(),
            };
            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                op = "instance_lookup",
                source = "parent",
                "Falling back to parent cache"
            );
            return parent_injector.get_instance::<T>();
        }

        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "instance_lookup",
            source = "none",
            "Instance not cached in injector hierarchy"
        );

        None
    }

    pub(crate) fn get_set_instance<T>(
        &self,
        provider_ref: &Shared<Provider<T>>,
    ) -> Option<Shared<Instance<T>>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let key = SetProviderKey::of::<T>(provider_ref);
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "lock-free")]
        let local = self
            .inner
            .set_instances
            .get(&key)
            .map(|value| value.value().clone());
        #[cfg(not(feature = "lock-free"))]
        let local = self.inner.set_instances.read().unwrap().get(&key).cloned();

        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            op = "instance_lookup_set",
            provider_ptr = key.provider_ptr,
            hit = local.is_some(),
            "Set-binding instance lookup completed"
        );

        local.map(stored_instance::<T>)
    }

    pub(crate) fn get_instance_named<T>(&self, name: &str) -> Option<Shared<Instance<T>>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let key = NamedTypeKey::of::<T>(name);
        #[cfg(feature = "tracing")]
        let type_name = std::any::type_name::<T>();

        #[cfg(feature = "lock-free")]
        let local = self
            .inner
            .named_instances
            .get(&key)
            .map(|value| value.value().clone());
        #[cfg(not(feature = "lock-free"))]
        let local = self
            .inner
            .named_instances
            .read()
            .unwrap()
            .get(&key)
            .cloned();

        if local.is_some() {
            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                name = %name,
                op = "instance_lookup_named",
                source = "local",
                hit = true,
                "Named instance cache hit"
            );
            return local.map(stored_instance::<T>);
        }

        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            name = %name,
            op = "instance_lookup_named",
            source = "local",
            hit = false,
            has_parent = self.inner.parent.is_some(),
            "Named instance cache miss"
        );

        if let Some(parent) = &self.inner.parent {
            let parent_injector = Injector {
                inner: parent.clone(),
            };
            #[cfg(feature = "tracing")]
            trace!(
                type_name = type_name,
                name = %name,
                op = "instance_lookup_named",
                source = "parent",
                "Falling back to parent cache for named instance"
            );
            return parent_injector.get_instance_named::<T>(name);
        }

        #[cfg(feature = "tracing")]
        trace!(
            type_name = type_name,
            name = %name,
            op = "instance_lookup_named",
            source = "none",
            "Named instance not cached in injector hierarchy"
        );

        None
    }
}
