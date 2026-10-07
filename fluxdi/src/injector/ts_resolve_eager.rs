use super::*;
use std::collections::VecDeque;
use std::pin::Pin;

/// Intermediate representation of the eager resolution graph.
struct EagerGraph {
    node_deps: HashMap<String, Vec<String>>,
    single_ids: HashMap<TypeId, String>,
    #[allow(dead_code)]
    named_ids: HashMap<NamedTypeKey, String>,
    #[cfg(feature = "dynamic")]
    #[allow(dead_code)]
    dynamic_ids: HashMap<String, String>,
    resolvers: HashMap<String, EagerResolverFn>,
}

impl Injector {
    /// Resolves all registered async providers in dependency order,
    /// maximizing parallelism for independent providers.
    ///
    /// Uses topological sort (Kahn's algorithm) to group providers into waves.
    /// Within each wave, all providers whose dependencies are satisfied run
    /// concurrently. Already-cached instances are skipped (idempotent).
    ///
    /// Requires that `.with_dependency()` hints accurately reflect actual
    /// dependencies. Call `validate_graph()` beforehand to check.
    pub async fn resolve_all_eager(&self) -> Result<(), Error> {
        let graph = self.build_eager_graph();
        self.execute_eager_waves(graph.node_deps, &graph.resolvers)
            .await
    }

    /// Resolves a single typed provider and all its transitive dependencies
    /// in correct topological order with maximum parallelism.
    ///
    /// Only the specified type and the providers it transitively depends on
    /// (via `.with_dependency()` hints) are resolved. All other registered
    /// providers are left untouched.
    pub async fn resolve_eager_for<T>(&self) -> Result<(), Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.resolve_eager_roots(&[TypeId::of::<T>()]).await
    }

    /// Resolves multiple typed providers and all their transitive dependencies
    /// in correct topological order with maximum parallelism.
    ///
    /// Only the specified types and the providers they transitively depend on
    /// (via `.with_dependency()` hints) are resolved. All other registered
    /// providers are left untouched.
    pub async fn resolve_eager_roots(&self, roots: &[TypeId]) -> Result<(), Error> {
        let graph = self.build_eager_graph();

        // Map root TypeIds to node_ids
        let root_node_ids: Vec<String> = roots
            .iter()
            .filter_map(|type_id| graph.single_ids.get(type_id).cloned())
            .collect();

        if root_node_ids.is_empty() {
            return Ok(());
        }

        // Compute transitive closure: all nodes reachable from roots via dependencies
        let reachable = transitive_closure(&root_node_ids, &graph.node_deps);

        // Filter graph to only reachable nodes
        let filtered_deps: HashMap<String, Vec<String>> = graph
            .node_deps
            .into_iter()
            .filter(|(node, _)| reachable.contains(node))
            .collect();

        self.execute_eager_waves(filtered_deps, &graph.resolvers)
            .await
    }

    /// Builds the full eager resolution graph from all registered providers.
    fn build_eager_graph(&self) -> EagerGraph {
        let mut state = GraphBuildState::default();
        self.collect_graph_state(&mut state);
        let resolvers = self.collect_eager_resolvers();

        let mut single_ids: HashMap<TypeId, String> = HashMap::new();
        let mut named_ids: HashMap<NamedTypeKey, String> = HashMap::new();
        #[cfg(feature = "dynamic")]
        let mut dynamic_ids: HashMap<String, String> = HashMap::new();
        let mut node_deps: HashMap<String, Vec<String>> = HashMap::new();

        for (type_id, meta) in &state.singles {
            let node_id = format!("single::{}", meta.type_name);
            single_ids.insert(*type_id, node_id.clone());
            node_deps.insert(node_id, Vec::new());
        }

        for (key, meta) in &state.named {
            let node_id = format!("named::{}::{}", meta.type_name, key.name);
            named_ids.insert(key.clone(), node_id.clone());
            node_deps.insert(node_id, Vec::new());
        }

        #[cfg(feature = "dynamic")]
        for name in state.dynamics.keys() {
            let node_id = format!("dynamic::{}", name);
            dynamic_ids.insert(name.clone(), node_id.clone());
            node_deps.insert(node_id, Vec::new());
        }

        // Resolve dependency hints to node_id targets
        let resolve_dep_targets = |dep: &crate::graph::DependencyHint| -> Vec<String> {
            if dep.is_dynamic {
                #[cfg(feature = "dynamic")]
                if let Some(name) = &dep.name {
                    return dynamic_ids.get(name).cloned().into_iter().collect();
                }
                #[cfg(not(feature = "dynamic"))]
                return Vec::new();
                #[allow(unreachable_code)]
                Vec::new()
            } else {
                match dep.cardinality {
                    DependencyCardinality::One => {
                        if let Some(name) = &dep.name {
                            named_ids
                                .get(&NamedTypeKey {
                                    type_id: dep.type_id,
                                    name: name.clone(),
                                })
                                .cloned()
                                .into_iter()
                                .collect()
                        } else {
                            single_ids.get(&dep.type_id).cloned().into_iter().collect()
                        }
                    }
                    // Set dependencies are skipped for eager resolution
                    DependencyCardinality::All => Vec::new(),
                }
            }
        };

        for meta in state.singles.values() {
            let node_id = format!("single::{}", meta.type_name);
            let deps: Vec<String> = meta
                .dependencies
                .iter()
                .flat_map(&resolve_dep_targets)
                .collect();
            node_deps.insert(node_id, deps);
        }

        for (key, meta) in &state.named {
            let node_id = format!("named::{}::{}", meta.type_name, key.name);
            let deps: Vec<String> = meta
                .dependencies
                .iter()
                .flat_map(&resolve_dep_targets)
                .collect();
            node_deps.insert(node_id, deps);
        }

        #[cfg(feature = "dynamic")]
        for (name, meta) in &state.dynamics {
            let node_id = format!("dynamic::{}", name);
            let deps: Vec<String> = meta
                .dependencies
                .iter()
                .flat_map(&resolve_dep_targets)
                .collect();
            node_deps.insert(node_id, deps);
        }

        EagerGraph {
            node_deps,
            single_ids,
            named_ids,
            #[cfg(feature = "dynamic")]
            dynamic_ids,
            resolvers,
        }
    }

    /// Executes topological waves over the given node dependency graph.
    async fn execute_eager_waves(
        &self,
        node_deps: HashMap<String, Vec<String>>,
        resolvers: &HashMap<String, EagerResolverFn>,
    ) -> Result<(), Error> {
        let waves = topological_waves(&node_deps)?;

        type BoxedResolveFuture =
            Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send>>;

        for (wave_index, wave) in waves.iter().enumerate() {
            let mut futures: Vec<BoxedResolveFuture> = Vec::new();

            for node_id in wave {
                if let Some(resolver) = resolvers.get(node_id) {
                    let resolver = resolver.clone();
                    let injector = self.clone();
                    futures.push(Box::pin(async move { (resolver)(injector).await }));
                }
            }

            if futures.is_empty() {
                continue;
            }

            let results: Vec<Result<(), Error>> = futures::future::join_all(futures).await;
            let errors: Vec<&Error> = results.iter().filter_map(|r| r.as_ref().err()).collect();

            if !errors.is_empty() {
                return Err(Error::eager_wave_failed(wave_index, &errors));
            }
        }

        Ok(())
    }

    fn collect_eager_resolvers(&self) -> HashMap<String, EagerResolverFn> {
        let mut resolvers = HashMap::new();

        if let Some(parent) = &self.inner.parent {
            let parent_injector = Injector {
                inner: parent.clone(),
            };
            resolvers = parent_injector.collect_eager_resolvers();
        }

        #[cfg(not(feature = "lock-free"))]
        {
            for (key, resolver) in self.inner.eager_resolvers.read().unwrap().iter() {
                resolvers.insert(key.clone(), resolver.clone());
            }
        }

        #[cfg(feature = "lock-free")]
        {
            for entry in self.inner.eager_resolvers.iter() {
                resolvers.insert(entry.key().clone(), entry.value().clone());
            }
        }

        resolvers
    }
}

/// Computes the set of all nodes transitively reachable from `roots`
/// by following dependency edges.
fn transitive_closure(
    roots: &[String],
    node_deps: &HashMap<String, Vec<String>>,
) -> HashSet<String> {
    let mut reachable = HashSet::new();
    let mut queue: VecDeque<String> = roots.iter().cloned().collect();

    while let Some(node) = queue.pop_front() {
        if !reachable.insert(node.clone()) {
            continue;
        }
        if let Some(deps) = node_deps.get(&node) {
            for dep in deps {
                if node_deps.contains_key(dep) && !reachable.contains(dep) {
                    queue.push_back(dep.clone());
                }
            }
        }
    }

    reachable
}

/// Groups nodes into topological waves using Kahn's algorithm.
///
/// Each wave contains nodes whose dependencies are fully satisfied by
/// previous waves. Nodes within a wave can be resolved in parallel.
fn topological_waves(node_deps: &HashMap<String, Vec<String>>) -> Result<Vec<Vec<String>>, Error> {
    let mut dep_count: HashMap<String, usize> = HashMap::new();
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

    for (node, deps) in node_deps {
        // Only count dependencies that are known (registered) nodes
        let known_deps = deps
            .iter()
            .filter(|d| node_deps.contains_key(d.as_str()))
            .count();
        dep_count.insert(node.clone(), known_deps);
        for dep in deps {
            if node_deps.contains_key(dep) {
                dependents
                    .entry(dep.clone())
                    .or_default()
                    .push(node.clone());
            }
        }
    }

    let mut waves = Vec::new();
    let mut resolved: HashSet<String> = HashSet::new();

    loop {
        let mut current_wave: Vec<String> = dep_count
            .iter()
            .filter(|(node, count)| **count == 0 && !resolved.contains(node.as_str()))
            .map(|(node, _)| node.clone())
            .collect();

        if current_wave.is_empty() {
            break;
        }

        current_wave.sort();

        for node in &current_wave {
            resolved.insert(node.clone());
            if let Some(deps) = dependents.get(node) {
                for dep in deps {
                    if let Some(count) = dep_count.get_mut(dep) {
                        *count = count.saturating_sub(1);
                    }
                }
            }
        }

        waves.push(current_wave);
    }

    if resolved.len() < node_deps.len() {
        return Err(Error::graph_validation_failed(
            "circular dependency detected during eager resolution",
        ));
    }

    Ok(waves)
}
