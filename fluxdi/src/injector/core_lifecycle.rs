use super::*;

impl Injector {
    pub fn root() -> Self {
        Self {
            inner: Shared::new(InjectorInner {
                parent: None,
                is_scope_boundary: false,
                #[cfg(feature = "async-factory")]
                scope_id: ScopeId::next(),
                #[cfg(feature = "async-factory")]
                cells: Shared::new(CellRegistry::default()),
                #[cfg(not(feature = "thread-safe"))]
                providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                graph_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                set_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                graph_set_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                named_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                graph_named_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                instances: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                set_instances: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                named_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                graph_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                set_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                graph_set_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                named_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                graph_named_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                set_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                named_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                graph_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                set_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                graph_set_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                named_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                graph_named_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                instances: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                set_instances: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                named_instances: DashMap::new(),
                #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
                dynamic_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
                graph_dynamic_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
                dynamic_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "dynamic", feature = "lock-free"))]
                dynamic_providers: DashMap::new(),
                #[cfg(all(feature = "dynamic", feature = "lock-free"))]
                graph_dynamic_providers: DashMap::new(),
                #[cfg(all(feature = "dynamic", feature = "lock-free"))]
                dynamic_instances: DashMap::new(),
                #[cfg(all(feature = "eager-resolution", not(feature = "lock-free")))]
                eager_resolvers: Store::new(HashMap::new()),
                #[cfg(all(feature = "eager-resolution", feature = "lock-free"))]
                eager_resolvers: DashMap::new(),
                #[cfg(feature = "metrics")]
                metrics: Shared::new(MetricsState::default()),
            }),
        }
    }

    pub fn child(parent: Shared<Injector>) -> Self {
        Self {
            inner: Shared::new(InjectorInner {
                parent: Some(parent.inner.clone()),
                is_scope_boundary: false,
                #[cfg(feature = "async-factory")]
                scope_id: ScopeId::next(),
                #[cfg(feature = "async-factory")]
                cells: parent.inner.cells.clone(),
                #[cfg(not(feature = "thread-safe"))]
                providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                graph_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                set_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                graph_set_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                named_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                graph_named_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                instances: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                set_instances: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                named_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                graph_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                set_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                graph_set_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                named_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                graph_named_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                set_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                named_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                graph_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                set_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                graph_set_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                named_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                graph_named_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                instances: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                set_instances: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                named_instances: DashMap::new(),
                #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
                dynamic_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
                graph_dynamic_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
                dynamic_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "dynamic", feature = "lock-free"))]
                dynamic_providers: DashMap::new(),
                #[cfg(all(feature = "dynamic", feature = "lock-free"))]
                graph_dynamic_providers: DashMap::new(),
                #[cfg(all(feature = "dynamic", feature = "lock-free"))]
                dynamic_instances: DashMap::new(),
                #[cfg(all(feature = "eager-resolution", not(feature = "lock-free")))]
                eager_resolvers: Store::new(HashMap::new()),
                #[cfg(all(feature = "eager-resolution", feature = "lock-free"))]
                eager_resolvers: DashMap::new(),
                #[cfg(feature = "metrics")]
                metrics: parent.inner.metrics.clone(),
            }),
        }
    }

    fn scoped(parent: Shared<Injector>) -> Self {
        Self {
            inner: Shared::new(InjectorInner {
                parent: Some(parent.inner.clone()),
                is_scope_boundary: true,
                #[cfg(feature = "async-factory")]
                scope_id: ScopeId::next(),
                #[cfg(feature = "async-factory")]
                cells: parent.inner.cells.clone(),
                #[cfg(not(feature = "thread-safe"))]
                providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                graph_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                set_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                graph_set_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                named_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                graph_named_providers: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                instances: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                set_instances: Store::new(HashMap::new()),
                #[cfg(not(feature = "thread-safe"))]
                named_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                graph_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                set_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                graph_set_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                named_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                graph_named_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                set_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
                named_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                graph_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                set_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                graph_set_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                named_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                graph_named_providers: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                instances: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                set_instances: DashMap::new(),
                #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
                named_instances: DashMap::new(),
                #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
                dynamic_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
                graph_dynamic_providers: Store::new(HashMap::new()),
                #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
                dynamic_instances: Store::new(HashMap::new()),
                #[cfg(all(feature = "dynamic", feature = "lock-free"))]
                dynamic_providers: DashMap::new(),
                #[cfg(all(feature = "dynamic", feature = "lock-free"))]
                graph_dynamic_providers: DashMap::new(),
                #[cfg(all(feature = "dynamic", feature = "lock-free"))]
                dynamic_instances: DashMap::new(),
                #[cfg(all(feature = "eager-resolution", not(feature = "lock-free")))]
                eager_resolvers: Store::new(HashMap::new()),
                #[cfg(all(feature = "eager-resolution", feature = "lock-free"))]
                eager_resolvers: DashMap::new(),
                #[cfg(feature = "metrics")]
                metrics: parent.inner.metrics.clone(),
            }),
        }
    }

    pub fn create_scope(&self) -> Self {
        Self::scoped(Shared::new(self.clone()))
    }

    pub(crate) fn root_injector(&self) -> Injector {
        let mut current = self.clone();

        while let Some(parent) = &current.inner.parent {
            current = Injector {
                inner: parent.clone(),
            };
        }

        current
    }

    fn nearest_scope_injector(&self) -> Option<Injector> {
        let mut current = self.clone();

        loop {
            if current.inner.is_scope_boundary {
                return Some(current);
            }

            let Some(parent) = &current.inner.parent else {
                return None;
            };

            current = Injector {
                inner: parent.clone(),
            };
        }
    }

    pub(super) fn cache_target_for_scope(&self, scope: Scope) -> Option<Injector> {
        match scope {
            Scope::Transient => None,
            Scope::Root => Some(self.root_injector()),
            Scope::Module => Some(self.clone()),
            Scope::Scoped => Some(
                self.nearest_scope_injector()
                    .unwrap_or_else(|| self.clone()),
            ),
        }
    }

    #[cfg(feature = "metrics")]
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.inner.metrics.snapshot()
    }

    #[cfg(feature = "prometheus")]
    pub fn prometheus_metrics(&self) -> String {
        self.inner.metrics.to_prometheus()
    }
}
