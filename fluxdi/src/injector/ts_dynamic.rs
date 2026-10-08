use super::*;
use crate::dynamic::{DynamicDependency, DynamicProvider};
use crate::graph::{DependencyHint, DynamicProviderGraphMeta};

impl Injector {
    /// Registers a dynamic (string-keyed) async provider with declared dependencies.
    pub fn try_provide_dynamic(
        &self,
        name: impl Into<String>,
        provider: DynamicProvider,
    ) -> Result<(), Error> {
        let name = name.into();
        let scope = provider.scope;

        let dependencies: Vec<DependencyHint> = provider
            .dependencies
            .iter()
            .map(|dep| match dep {
                DynamicDependency::Static { type_id, type_name } => DependencyHint {
                    type_id: *type_id,
                    type_name,
                    name: None,
                    cardinality: DependencyCardinality::One,
                    is_dynamic: false,
                },
                DynamicDependency::Named(n) => DependencyHint::dynamic(n.clone()),
            })
            .collect();
        let graph_meta = DynamicProviderGraphMeta {
            name: name.clone(),
            scope,
            dependencies,
        };

        #[cfg(not(feature = "lock-free"))]
        {
            let mut providers = self.inner.dynamic_providers.write().unwrap();
            if providers.contains_key(&name) {
                return Err(Error::provider_already_registered(
                    &name,
                    &scope.to_string(),
                ));
            }
            providers.insert(name.clone(), Shared::new(provider));
            drop(providers);
            self.inner
                .graph_dynamic_providers
                .write()
                .unwrap()
                .insert(name.clone(), graph_meta);
        }

        #[cfg(feature = "lock-free")]
        {
            match self.inner.dynamic_providers.entry(name.clone()) {
                DashEntry::Occupied(_) => {
                    return Err(Error::provider_already_registered(
                        &name,
                        &scope.to_string(),
                    ));
                }
                DashEntry::Vacant(entry) => {
                    entry.insert(Shared::new(provider));
                }
            }
            self.inner
                .graph_dynamic_providers
                .insert(name.clone(), graph_meta);
        }

        #[cfg(feature = "eager-resolution")]
        {
            let node_id = format!("dynamic::{}", name);
            let name_for_resolver = name.clone();
            let resolver: EagerResolverFn = Shared::new(
                move |inj: Injector| -> std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<(), Error>> + Send>,
                > {
                    let name = name_for_resolver.clone();
                    Box::pin(async move { inj.try_resolve_dynamic(&name).await.map(|_| ()) })
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

        Ok(())
    }

    /// Registers a dynamic provider. Panics on error.
    pub fn provide_dynamic(&self, name: impl Into<String>, provider: DynamicProvider) -> &Self {
        self.try_provide_dynamic(name, provider).unwrap();
        self
    }

    /// Resolves a dynamic provider by name. Returns the type-erased instance.
    pub async fn try_resolve_dynamic(
        &self,
        name: &str,
    ) -> Result<Shared<dyn Any + Send + Sync>, Error> {
        // Check cache first
        if let Some(instance) = self.get_dynamic_instance(name) {
            return Ok(instance);
        }

        // Get provider
        let provider = self
            .get_dynamic_provider(name)
            .ok_or_else(|| Error::dynamic_provider_not_found(name))?;

        // Call factory
        let run = factory_boundary::run_sync(name, || (provider.factory)(self.clone()))?;
        let instance = factory_boundary::run_async(name, run).await?;

        // Cache if non-transient
        if provider.scope != Scope::Transient
            && let Some(target) = self.cache_target_for_scope(provider.scope)
        {
            target.store_dynamic_instance(name, instance.clone());
        }

        Ok(instance)
    }

    /// Resolves a dynamic provider by name. Panics on error.
    pub async fn resolve_dynamic(&self, name: &str) -> Shared<dyn Any + Send + Sync> {
        self.try_resolve_dynamic(name).await.unwrap()
    }

    pub(crate) fn get_dynamic_provider(&self, name: &str) -> Option<Shared<DynamicProvider>> {
        #[cfg(not(feature = "lock-free"))]
        let local = self
            .inner
            .dynamic_providers
            .read()
            .unwrap()
            .get(name)
            .cloned();
        #[cfg(feature = "lock-free")]
        let local = self
            .inner
            .dynamic_providers
            .get(name)
            .map(|v| v.value().clone());

        if local.is_some() {
            return local;
        }

        if let Some(parent) = &self.inner.parent {
            let parent_injector = Injector {
                inner: parent.clone(),
            };
            return parent_injector.get_dynamic_provider(name);
        }

        None
    }

    fn get_dynamic_instance(&self, name: &str) -> Option<Shared<dyn Any + Send + Sync>> {
        #[cfg(not(feature = "lock-free"))]
        let local = self
            .inner
            .dynamic_instances
            .read()
            .unwrap()
            .get(name)
            .cloned();
        #[cfg(feature = "lock-free")]
        let local = self
            .inner
            .dynamic_instances
            .get(name)
            .map(|v| v.value().clone());

        if local.is_some() {
            return local;
        }

        if let Some(parent) = &self.inner.parent {
            let parent_injector = Injector {
                inner: parent.clone(),
            };
            return parent_injector.get_dynamic_instance(name);
        }

        None
    }

    fn store_dynamic_instance(&self, name: &str, instance: Shared<dyn Any + Send + Sync>) {
        #[cfg(not(feature = "lock-free"))]
        self.inner
            .dynamic_instances
            .write()
            .unwrap()
            .insert(name.to_string(), instance);
        #[cfg(feature = "lock-free")]
        self.inner
            .dynamic_instances
            .insert(name.to_string(), instance);
    }
}
