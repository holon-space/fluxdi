use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};

#[cfg(all(feature = "thread-safe", feature = "lock-free"))]
use dashmap::{DashMap, mapref::entry::Entry as DashEntry};

#[cfg(feature = "dynamic")]
use crate::dynamic::DynamicProvider;
use crate::error::Error;
#[cfg(feature = "eager-resolution")]
use crate::error::ErrorKind;
#[cfg(feature = "dynamic")]
use crate::graph::DynamicProviderGraphMeta;
use crate::graph::{
    DependencyCardinality, DependencyGraph, GraphBinding, GraphDependency, GraphEdge, GraphNode,
    GraphValidationIssue, GraphValidationIssueKind, GraphValidationReport, ProviderGraphMeta,
};
use crate::instance::Instance;
#[cfg(feature = "metrics")]
use crate::observability::{MetricsSnapshot, MetricsState};
#[cfg(feature = "tracing")]
use crate::observability::{SPAN_FACTORY_EXECUTE, SPAN_PROVIDE, SPAN_RESOLVE};
use crate::provider::Provider;
use crate::resolve_guard::ResolveGuard;
use crate::runtime::Shared;
#[cfg(not(all(feature = "thread-safe", feature = "lock-free")))]
use crate::runtime::Store;
use crate::scope::Scope;

#[cfg(feature = "tracing")]
use tracing::{debug, info_span, trace};

#[cfg(feature = "eager-resolution")]
pub(crate) type EagerResolverFn = Shared<
    dyn Fn(
            Injector,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send>>
        + Send
        + Sync,
>;

pub struct Injector {
    inner: Shared<InjectorInner>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NamedTypeKey {
    type_id: TypeId,
    name: String,
}

impl NamedTypeKey {
    fn of<T>(name: &str) -> Self
    where
        T: ?Sized + 'static,
    {
        Self {
            type_id: TypeId::of::<T>(),
            name: name.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct SetProviderKey {
    type_id: TypeId,
    provider_ptr: usize,
}

impl SetProviderKey {
    fn of<T>(provider: &Shared<Provider<T>>) -> Self
    where
        T: ?Sized + 'static,
    {
        Self {
            type_id: TypeId::of::<T>(),
            provider_ptr: Shared::as_ptr(provider) as *const () as usize,
        }
    }
}

#[derive(Default)]
struct GraphBuildState {
    singles: HashMap<TypeId, ProviderGraphMeta>,
    named: HashMap<NamedTypeKey, ProviderGraphMeta>,
    sets: HashMap<TypeId, Vec<ProviderGraphMeta>>,
    #[cfg(feature = "dynamic")]
    dynamics: HashMap<String, DynamicProviderGraphMeta>,
}

fn dependency_label(cardinality: DependencyCardinality, name: Option<&str>) -> Option<String> {
    match cardinality {
        DependencyCardinality::One => match name {
            Some(name) => Some(format!("one:{name}")),
            None => Some("one".to_string()),
        },
        DependencyCardinality::All => Some("all".to_string()),
    }
}

fn detect_cycles(
    start: &str,
    adjacency: &HashMap<String, Vec<String>>,
    visiting: &mut HashSet<String>,
    visited: &mut HashSet<String>,
    stack: &mut Vec<String>,
    seen_signatures: &mut HashSet<String>,
    report: &mut GraphValidationReport,
) {
    if visited.contains(start) {
        return;
    }

    visiting.insert(start.to_string());
    stack.push(start.to_string());

    if let Some(neighbors) = adjacency.get(start) {
        for neighbor in neighbors {
            if visiting.contains(neighbor) {
                if let Some(pos) = stack.iter().position(|node| node == neighbor) {
                    let mut cycle = stack[pos..].to_vec();
                    cycle.push(neighbor.clone());
                    let signature = cycle.join("->");
                    if seen_signatures.insert(signature) {
                        report.issues.push(GraphValidationIssue {
                            kind: GraphValidationIssueKind::CircularDependency,
                            node_id: Some(start.to_string()),
                            message: format!(
                                "Graph validation failed: circular dependency detected: {}",
                                cycle.join(" -> ")
                            ),
                        });
                    }
                }
                continue;
            }

            if !visited.contains(neighbor) {
                detect_cycles(
                    neighbor,
                    adjacency,
                    visiting,
                    visited,
                    stack,
                    seen_signatures,
                    report,
                );
            }
        }
    }

    stack.pop();
    visiting.remove(start);
    visited.insert(start.to_string());
}

struct InjectorInner {
    pub(crate) parent: Option<Shared<InjectorInner>>,
    pub(crate) is_scope_boundary: bool,

    #[cfg(not(feature = "thread-safe"))]
    pub(crate) providers: Store<HashMap<TypeId, Shared<dyn Any>>>,
    #[cfg(not(feature = "thread-safe"))]
    pub(crate) graph_providers: Store<HashMap<TypeId, ProviderGraphMeta>>,
    #[cfg(not(feature = "thread-safe"))]
    pub(crate) set_providers: Store<HashMap<TypeId, Vec<Shared<dyn Any>>>>,
    #[cfg(not(feature = "thread-safe"))]
    pub(crate) graph_set_providers: Store<HashMap<TypeId, Vec<ProviderGraphMeta>>>,
    #[cfg(not(feature = "thread-safe"))]
    pub(crate) named_providers: Store<HashMap<NamedTypeKey, Shared<dyn Any>>>,
    #[cfg(not(feature = "thread-safe"))]
    pub(crate) graph_named_providers: Store<HashMap<NamedTypeKey, ProviderGraphMeta>>,
    #[cfg(not(feature = "thread-safe"))]
    pub(crate) instances: Store<HashMap<TypeId, Shared<dyn Any>>>,
    #[cfg(not(feature = "thread-safe"))]
    pub(crate) set_instances: Store<HashMap<SetProviderKey, Shared<dyn Any>>>,
    #[cfg(not(feature = "thread-safe"))]
    pub(crate) named_instances: Store<HashMap<NamedTypeKey, Shared<dyn Any>>>,

    #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
    pub(crate) providers: Store<HashMap<TypeId, Shared<dyn Any + Send + Sync>>>,
    #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
    pub(crate) graph_providers: Store<HashMap<TypeId, ProviderGraphMeta>>,
    #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
    pub(crate) set_providers: Store<HashMap<TypeId, Vec<Shared<dyn Any + Send + Sync>>>>,
    #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
    pub(crate) graph_set_providers: Store<HashMap<TypeId, Vec<ProviderGraphMeta>>>,
    #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
    pub(crate) named_providers: Store<HashMap<NamedTypeKey, Shared<dyn Any + Send + Sync>>>,
    #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
    pub(crate) graph_named_providers: Store<HashMap<NamedTypeKey, ProviderGraphMeta>>,
    #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
    pub(crate) instances: Store<HashMap<TypeId, Shared<dyn Any + Send + Sync>>>,
    #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
    pub(crate) set_instances: Store<HashMap<SetProviderKey, Shared<dyn Any + Send + Sync>>>,
    #[cfg(all(feature = "thread-safe", not(feature = "lock-free")))]
    pub(crate) named_instances: Store<HashMap<NamedTypeKey, Shared<dyn Any + Send + Sync>>>,

    #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
    pub(crate) providers: DashMap<TypeId, Shared<dyn Any + Send + Sync>>,
    #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
    pub(crate) graph_providers: DashMap<TypeId, ProviderGraphMeta>,
    #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
    pub(crate) set_providers: DashMap<TypeId, Vec<Shared<dyn Any + Send + Sync>>>,
    #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
    pub(crate) graph_set_providers: DashMap<TypeId, Vec<ProviderGraphMeta>>,
    #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
    pub(crate) named_providers: DashMap<NamedTypeKey, Shared<dyn Any + Send + Sync>>,
    #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
    pub(crate) graph_named_providers: DashMap<NamedTypeKey, ProviderGraphMeta>,
    #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
    pub(crate) instances: DashMap<TypeId, Shared<dyn Any + Send + Sync>>,
    #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
    pub(crate) set_instances: DashMap<SetProviderKey, Shared<dyn Any + Send + Sync>>,
    #[cfg(all(feature = "thread-safe", feature = "lock-free"))]
    pub(crate) named_instances: DashMap<NamedTypeKey, Shared<dyn Any + Send + Sync>>,

    #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
    pub(crate) dynamic_providers: Store<HashMap<String, Shared<DynamicProvider>>>,
    #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
    pub(crate) graph_dynamic_providers: Store<HashMap<String, DynamicProviderGraphMeta>>,
    #[cfg(all(feature = "dynamic", not(feature = "lock-free")))]
    pub(crate) dynamic_instances: Store<HashMap<String, Shared<dyn Any + Send + Sync>>>,
    #[cfg(all(feature = "dynamic", feature = "lock-free"))]
    pub(crate) dynamic_providers: DashMap<String, Shared<DynamicProvider>>,
    #[cfg(all(feature = "dynamic", feature = "lock-free"))]
    pub(crate) graph_dynamic_providers: DashMap<String, DynamicProviderGraphMeta>,
    #[cfg(all(feature = "dynamic", feature = "lock-free"))]
    pub(crate) dynamic_instances: DashMap<String, Shared<dyn Any + Send + Sync>>,

    #[cfg(all(feature = "eager-resolution", not(feature = "lock-free")))]
    pub(crate) eager_resolvers: Store<HashMap<String, EagerResolverFn>>,
    #[cfg(all(feature = "eager-resolution", feature = "lock-free"))]
    pub(crate) eager_resolvers: DashMap<String, EagerResolverFn>,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Shared<MetricsState>,
}

#[cfg(feature = "debug")]
impl std::fmt::Debug for InjectorInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("InjectorInner");
        ds.field("parent", &self.parent.is_some());
        ds.field("is_scope_boundary", &self.is_scope_boundary);
        ds.field("providers", &self.providers);
        ds.field("graph_providers", &self.graph_providers);
        ds.field("set_providers", &self.set_providers);
        ds.field("graph_set_providers", &self.graph_set_providers);
        ds.field("named_providers", &self.named_providers);
        ds.field("graph_named_providers", &self.graph_named_providers);
        ds.field("instances", &self.instances);
        ds.field("set_instances", &self.set_instances);
        ds.field("named_instances", &self.named_instances);
        #[cfg(feature = "metrics")]
        ds.field("metrics", &self.metrics.snapshot());
        ds.finish()
    }
}

#[cfg(feature = "debug")]
impl std::fmt::Debug for Injector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("inner", &self.inner)
            .finish()
    }
}

impl Clone for Injector {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(all(test, feature = "async-factory"))]
mod async_factory_tests;
mod core_lifecycle;
#[cfg(all(test, feature = "dynamic"))]
mod dynamic_tests;
#[cfg(all(test, feature = "eager-resolution"))]
mod eager_resolution_tests;
#[cfg(all(test, feature = "async-factory"))]
mod fallible_async_factory_tests;
mod graph_state;
mod graph_validation;

#[cfg(not(feature = "thread-safe"))]
mod nts_instance_factory;
#[cfg(not(feature = "thread-safe"))]
mod nts_provider_lookup;
#[cfg(not(feature = "thread-safe"))]
mod nts_registration;
#[cfg(not(feature = "thread-safe"))]
mod nts_resolve_async;
#[cfg(not(feature = "thread-safe"))]
mod nts_resolve_sync;
#[cfg(not(feature = "thread-safe"))]
mod nts_storage;

#[cfg(feature = "dynamic")]
mod ts_dynamic;
#[cfg(feature = "thread-safe")]
mod ts_instance_factory;
#[cfg(feature = "thread-safe")]
mod ts_provider_lookup;
#[cfg(feature = "thread-safe")]
mod ts_registration;
#[cfg(feature = "thread-safe")]
mod ts_resolve_async;
#[cfg(feature = "eager-resolution")]
mod ts_resolve_eager;
#[cfg(feature = "thread-safe")]
mod ts_resolve_sync;
#[cfg(feature = "thread-safe")]
mod ts_storage_read;
#[cfg(feature = "thread-safe")]
mod ts_storage_write;
