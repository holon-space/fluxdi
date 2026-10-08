//! Live cells: one observable state per cached binding, filled by a producer
//! task on the injector tree's tokio runtime.
//!
//! A producer resolves its binding through the in-flight cells like a hard
//! resolve does, so live and hard resolves share one factory run. It runs as
//! a run of its own in the wait-for graph: `Live::ready` waits on it, and it
//! waits on the run it drives or joins. A cycle through a spawned producer
//! therefore closes in the graph like any other cycle.
//!
//! Each generation of a cell has its own producer. A new generation starts
//! only when the newest one is terminal, and never after `shutdown_live`;
//! both checks and the producer's record are made under the producers lock.
//!
//! A live set is observed from then on: a member registered later gets the
//! next slot in every live set that sees it. Creating a set and registering a
//! member both hold the sets lock, so a member is never in neither and never
//! twice.

use std::panic::AssertUnwindSafe;
use std::sync::{Mutex, MutexGuard, OnceLock, Weak};
use std::time::Instant;

use futures::FutureExt;
use futures::future::AbortHandle;
use tokio::runtime::Handle;
use tokio::sync::watch;

use super::cells::{CURRENT_RUN, CellRegistry, RunRef};
use super::*;
use crate::future_local::WithLocal;
use crate::live::{
    CellState, CellWatch, Generation, Live, LiveOutcome, LivePublisher, LiveReportChanges, LiveSet,
    LiveState, LiveTiming, is_terminal,
};

type Publish<T> = Shared<watch::Sender<CellState<T>>>;
type CachedLookup<T> = Box<dyn Fn(&Injector) -> Option<Shared<Instance<T>>> + Send + Sync>;
type Produce<T> = Box<dyn Fn(Injector, Injector, LivePublisher<T>) -> RunFuture<T> + Send + Sync>;
type Membership<T> = Shared<watch::Sender<Vec<Live<T>>>>;

/// The live part of a [`CellRegistry`].
pub(crate) struct LiveCells {
    runtime: OnceLock<Handle>,
    cells: Mutex<HashMap<CellKey, LiveEntry>>,
    sets: Mutex<HashMap<TypeId, Vec<LiveSetEntry>>>,
    producers: Mutex<Producers>,
    /// Counts producer starts and ends.
    report: watch::Sender<u64>,
}

impl Default for LiveCells {
    fn default() -> Self {
        Self {
            runtime: OnceLock::new(),
            cells: Mutex::new(HashMap::new()),
            sets: Mutex::new(HashMap::new()),
            producers: Mutex::new(Producers::default()),
            report: watch::channel(0).0,
        }
    }
}

#[derive(Default)]
struct Producers {
    records: Vec<ProducerRecord>,
    shut_down: bool,
}

struct LiveEntry {
    /// `Publish<T>` for the key's `T`.
    state: Box<dyn Any + Send + Sync>,
}

impl LiveEntry {
    fn publish<T: ?Sized + Send + Sync + 'static>(&self) -> &Publish<T> {
        self.state
            .downcast_ref::<Publish<T>>()
            .expect("a live cell key maps to one value type")
    }
}

/// The live set of one value type resolved from one injector.
struct LiveSetEntry {
    resolver: Weak<InjectorInner>,
    /// The resolver and its ancestors: a member registered in any of them
    /// is a member of this set.
    lineage: Vec<ScopeId>,
    /// `Membership<T>` for the entry's `T`.
    membership: Box<dyn Any + Send + Sync>,
}

impl LiveSetEntry {
    fn membership<T: ?Sized + Send + Sync + 'static>(&self) -> &Membership<T> {
        self.membership
            .downcast_ref::<Membership<T>>()
            .expect("a live set entry maps to one value type")
    }
}

/// What a cell that would start a producer after `shutdown_live` does.
#[derive(Clone, Copy)]
enum AfterShutdown {
    /// The resolve fails.
    Refuse,
    /// The cell is created `Failed(LiveProducerCancelled)`, like the cells
    /// the shutdown cancelled.
    Cancelled,
}

struct ProducerRecord {
    type_name: &'static str,
    member: Option<usize>,
    generation: Generation,
    started: Instant,
    ended: Option<(LiveOutcome, std::time::Duration)>,
    abort: AbortHandle,
}

impl LiveCells {
    /// The held runtime, or the caller's, which is then held.
    fn runtime(&self, type_name: &str) -> Result<Handle, Error> {
        if let Some(runtime) = self.runtime.get() {
            return Ok(runtime.clone());
        }
        let current = Handle::try_current().map_err(|_| Error::live_runtime_missing(type_name))?;
        Ok(self.runtime.get_or_init(|| current).clone())
    }

    /// The producers, locked for starting one of `type_name`.
    fn producers_for_start(&self, type_name: &str) -> Result<MutexGuard<'_, Producers>, Error> {
        let producers = self.producers.lock().expect("live producer mutex poisoned");
        if producers.shut_down {
            return Err(Error::live_shut_down(type_name));
        }
        Ok(producers)
    }

    fn bump_report(&self) {
        self.report.send_modify(|changes| *changes += 1);
    }
}

/// Starts generations of one live cell. Held by the cell's [`Live`] handles,
/// not by the registry, and it refers to its injectors weakly: a service
/// cached in the injector may hold a `Live` handle without keeping the
/// injector alive.
pub(crate) struct Starter<T: ?Sized + 'static> {
    resolver: Weak<InjectorInner>,
    target: Weak<InjectorInner>,
    key: CellKey,
    member: Option<usize>,
    state: Publish<T>,
    cached: CachedLookup<T>,
    /// Runs the factory with `(resolver, target, publisher)` and stores the
    /// instance in `target`'s cache.
    produce: Produce<T>,
}

impl<T: ?Sized + Send + Sync + 'static> Starter<T> {
    fn injector(weak: &Weak<InjectorInner>) -> Result<Injector, Error> {
        let inner = weak
            .upgrade()
            .ok_or_else(|| Error::live_injector_dropped(std::any::type_name::<T>()))?;
        Ok(Injector { inner })
    }

    pub(crate) fn restart(self: &Shared<Self>) -> Result<Generation, Error> {
        let type_name = std::any::type_name::<T>();
        let resolver = Self::injector(&self.resolver)?;
        let target = Self::injector(&self.target)?;
        let registry = &resolver.inner.cells;
        let producers = registry.live.producers_for_start(type_name)?;
        let newest = self.state.borrow().generation;
        if !is_terminal(&self.state.borrow().state) {
            return Err(Error::live_restart_while_running(type_name, newest.get()));
        }
        let runtime = registry.live.runtime(type_name)?;
        let producer = RunRef::next(type_name);
        let generation = newest.next();
        self.state.send_modify(|cell| {
            assert!(
                is_terminal(&cell.state),
                "only a restart ends a terminal generation"
            );
            *cell = CellState {
                generation,
                producer,
                state: LiveState::Pending,
            };
        });
        registry.spawn_producer(
            producers,
            &runtime,
            self.member,
            generation,
            self.state.clone(),
            self.run(producer, generation, false, resolver.clone(), target),
        );
        Ok(generation)
    }

    /// The producer of one generation. Only the first generation may take
    /// a value cached meanwhile; a restart always runs the factory.
    fn run(
        self: &Shared<Self>,
        producer: RunRef,
        generation: Generation,
        first: bool,
        resolver: Injector,
        target: Injector,
    ) -> impl Future<Output = Result<Shared<Instance<T>>, Error>> + Send + 'static {
        let starter = self.clone();
        let type_name = std::any::type_name::<T>();
        let run = async move {
            let key = starter.key.clone();
            let cached = || first.then(|| (starter.cached)(&target)).flatten();
            let produce = || {
                Box::pin(
                    AssertUnwindSafe((starter.produce)(
                        resolver.clone(),
                        target.clone(),
                        LivePublisher::new(starter.state.clone(), generation),
                    ))
                    .catch_unwind()
                    .map(|outcome| {
                        outcome.unwrap_or_else(|panic| {
                            Err(Error::factory_panicked(type_name, &panic_message(&*panic)))
                        })
                    }),
                ) as RunFuture<T>
            };
            resolver.produce_in_cell(key, cached, produce).await
        };
        WithLocal::new(
            &CURRENT_RUN,
            producer,
            crate::resolve_guard::resolving_on_new_path(TypeId::of::<T>(), run),
        )
    }
}

impl CellRegistry {
    /// The live cell a resolve of `key` must wait on instead of starting or
    /// joining a run.
    pub(super) fn live_observed<T: ?Sized + Send + Sync + 'static>(
        self: &Shared<Self>,
        key: &CellKey,
    ) -> Option<CellWatch<T>> {
        let cells = self.live.cells.lock().expect("live cell mutex poisoned");
        cells
            .get(key)
            .map(|entry| CellWatch::new(entry.publish::<T>().subscribe(), self.clone()))
    }

    /// Records `run` as the producer of `generation` of `state` and spawns
    /// it. The future of an aborted producer is dropped, and its observers
    /// see `LiveProducerCancelled`.
    fn spawn_producer<T, F>(
        self: &Shared<Self>,
        mut producers: MutexGuard<'_, Producers>,
        runtime: &Handle,
        member: Option<usize>,
        generation: Generation,
        state: Publish<T>,
        run: F,
    ) where
        T: ?Sized + Send + Sync + 'static,
        F: Future<Output = Result<Shared<Instance<T>>, Error>> + Send + 'static,
    {
        let type_name = std::any::type_name::<T>();
        let reporter = Reporter {
            registry: self.clone(),
            record: producers.records.len(),
            type_name,
            generation,
            state,
            reported: false,
        };
        let (task, abort) = futures::future::abortable(async move {
            let mut reporter = reporter;
            // Declared after `reporter`, so an aborted run is dropped before
            // its reporter publishes the cancellation.
            let run = run;
            let outcome = match run.await {
                Ok(instance) => LiveState::Ready(instance.value()),
                Err(error) => LiveState::Failed(Error::live_producer_failed(type_name, &error)),
            };
            reporter.finish(outcome);
        });
        producers.records.push(ProducerRecord {
            type_name,
            member,
            generation,
            started: Instant::now(),
            ended: None,
            abort,
        });
        // A runtime that is shut down drops the task inside `spawn`, and the
        // reporter's drop takes this lock.
        drop(producers);
        self.live.bump_report();
        runtime.spawn(task);
    }

    pub(crate) fn live_report(&self) -> Vec<LiveTiming> {
        let producers = self
            .live
            .producers
            .lock()
            .expect("live producer mutex poisoned");
        producers
            .records
            .iter()
            .map(|record| {
                let (outcome, elapsed) = match &record.ended {
                    Some((outcome, elapsed)) => (outcome.clone(), *elapsed),
                    None => (LiveOutcome::Running, record.started.elapsed()),
                };
                LiveTiming {
                    type_name: record.type_name,
                    member: record.member,
                    generation: record.generation,
                    outcome,
                    elapsed,
                }
            })
            .collect()
    }
}

/// Publishes a producer's outcome; dropped without one, it publishes
/// `LiveProducerCancelled`.
struct Reporter<T: ?Sized> {
    registry: Shared<CellRegistry>,
    record: usize,
    type_name: &'static str,
    generation: Generation,
    state: Publish<T>,
    reported: bool,
}

impl<T: ?Sized> Reporter<T> {
    fn finish(&mut self, state: LiveState<T>) {
        assert!(!self.reported, "a producer reports one outcome");
        self.reported = true;
        let outcome = match &state {
            LiveState::Ready(_) => LiveOutcome::Ready,
            LiveState::Failed(error) => LiveOutcome::Failed(error.clone()),
            _ => unreachable!("a producer reports a terminal state"),
        };
        {
            let mut producers = self
                .registry
                .live
                .producers
                .lock()
                .expect("live producer mutex poisoned");
            let record = &mut producers.records[self.record];
            record.ended = Some((outcome, record.started.elapsed()));
        }
        self.state.send_modify(|cell| {
            assert_eq!(
                cell.generation, self.generation,
                "a generation stays newest until its producer reports"
            );
            cell.state = state;
        });
        self.registry.live.bump_report();
    }
}

impl<T: ?Sized> Drop for Reporter<T> {
    fn drop(&mut self) {
        if self.reported {
            return;
        }
        let error = if std::thread::panicking() {
            Error::live_producer_failed(
                self.type_name,
                &Error::factory_panicked(self.type_name, "the live producer task panicked"),
            )
        } else {
            Error::live_producer_cancelled(self.type_name)
        };
        self.finish(LiveState::Failed(error));
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = panic.downcast_ref::<String>() {
        return message.clone();
    }
    "a non-string payload".to_string()
}

impl Injector {
    /// A root injector whose live producers run on `runtime`, so live
    /// resolves work from threads without a tokio runtime.
    pub fn root_with_runtime(runtime: Handle) -> Self {
        let injector = Self::root();
        injector
            .inner
            .cells
            .live
            .runtime
            .set(runtime)
            .expect("a new root holds no runtime");
        injector
    }

    /// Returns a handle to `T` at once and starts its producer on the first
    /// call; later live and hard resolves share that producer. A runtime is
    /// needed only to start the producer, not for a value already cached or
    /// a cell already live.
    pub fn try_resolve_live<T>(&self) -> Result<Live<T>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let provider = self.resolve_provider::<T>()?;
        let target = self.live_cache_target::<T>(provider.scope)?;
        let key = CellKey::new(&target, BindingKey::Single(TypeId::of::<T>()));
        self.live_cell(
            key,
            None,
            &target,
            AfterShutdown::Refuse,
            |target: &Injector| target.get_instance::<T>(),
            |injector: Injector, target: Injector, publisher: LivePublisher<T>| {
                Box::pin(async move {
                    let instance = injector
                        .resolve_instance_async_with::<T>(|provider, injector| {
                            provider.live_run(injector, publisher)
                        })
                        .await?;
                    target.store_instance::<T>(instance.clone());
                    Ok(instance)
                }) as RunFuture<T>
            },
        )
    }

    pub fn resolve_live<T>(&self) -> Live<T>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_resolve_live::<T>()
            .unwrap_or_else(|error| panic!("{}", error))
    }

    /// Returns the set of `T` at once, one slot per registered member, and
    /// starts the producer of every member that is not cached or running.
    /// A member registered later, in this injector or an ancestor, gets the
    /// next slot and its producer starts; see [`LiveSet`]. A set with no
    /// members yet is empty, not an error.
    pub fn try_resolve_all_live<T>(&self) -> Result<LiveSet<T>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let registry = &self.inner.cells;
        let mut sets = registry.live.sets.lock().expect("live set mutex poisoned");
        let entries = sets.entry(TypeId::of::<T>()).or_default();
        if let Some(entry) = entries
            .iter()
            .find(|entry| entry.lineage[0] == self.inner.scope_id)
        {
            return Ok(LiveSet::new(entry.membership::<T>().clone()));
        }
        let mut providers = Vec::new();
        self.collect_set_providers::<T>(&mut providers)?;
        let targets = providers
            .iter()
            .map(|provider| self.live_cache_target::<T>(provider.scope))
            .collect::<Result<Vec<_>, _>>()?;
        let members = providers
            .into_iter()
            .zip(targets)
            .enumerate()
            .map(|(member, (provider, target))| {
                self.live_set_member(member, provider, &target, AfterShutdown::Refuse)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let membership: Membership<T> = Shared::new(watch::channel(members).0);
        entries.push(LiveSetEntry {
            resolver: Shared::downgrade(&self.inner),
            lineage: std::iter::successors(Some(&self.inner), |inner| inner.parent.as_ref())
                .map(|inner| inner.scope_id)
                .collect(),
            membership: Box::new(membership.clone()),
        });
        Ok(LiveSet::new(membership))
    }

    /// Stores a set member in this injector and gives it the next slot in
    /// every live set that sees it. When such a set could not start the
    /// member, nothing is stored.
    pub(super) fn store_set_provider_live<T>(&self, provider: Provider<T>) -> Result<(), Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_name = std::any::type_name::<T>();
        let registry = &self.inner.cells;
        let mut sets = registry.live.sets.lock().expect("live set mutex poisoned");
        let mut observers = Vec::new();
        if let Some(entries) = sets.get_mut(&TypeId::of::<T>()) {
            entries.retain(|entry| {
                let Some(inner) = entry.resolver.upgrade() else {
                    return false;
                };
                if entry.lineage.contains(&self.inner.scope_id) {
                    observers.push((Injector { inner }, entry.membership::<T>().clone()));
                }
                true
            });
        }
        let targets = observers
            .iter()
            .map(|(resolver, _)| resolver.live_cache_target::<T>(provider.scope))
            .collect::<Result<Vec<_>, _>>()?;
        if !observers.is_empty() {
            registry.live.runtime(type_name)?;
        }
        let provider = self.store_set_provider::<T>(provider)?;
        for ((resolver, membership), target) in observers.into_iter().zip(targets) {
            let slot = membership.borrow().len();
            let member = resolver
                .live_set_member(slot, provider.clone(), &target, AfterShutdown::Cancelled)
                .expect(
                    "the cache target and the runtime were checked before the member was stored",
                );
            membership.send_modify(|members| members.push(member));
        }
        Ok(())
    }

    /// The live cell of set member `provider` in slot `member`.
    fn live_set_member<T>(
        &self,
        member: usize,
        provider: Shared<Provider<T>>,
        target: &Injector,
        after_shutdown: AfterShutdown,
    ) -> Result<Live<T>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let key = CellKey::new(
            target,
            BindingKey::SetMember(SetProviderKey::of::<T>(&provider)),
        );
        let cached_provider = provider.clone();
        self.live_cell(
            key,
            Some(member),
            target,
            after_shutdown,
            move |target: &Injector| target.get_set_instance::<T>(&cached_provider),
            move |injector: Injector, target: Injector, publisher: LivePublisher<T>| {
                let provider = provider.clone();
                Box::pin(async move {
                    let instance = injector
                        .resolve_instance_from_provider_async_with::<T>(
                            &provider,
                            |provider, injector| provider.live_run(injector, publisher),
                        )
                        .await?;
                    target.store_set_instance::<T>(&provider, instance.clone());
                    Ok(instance)
                }) as RunFuture<T>
            },
        )
    }

    pub fn resolve_all_live<T>(&self) -> LiveSet<T>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_resolve_all_live::<T>()
            .unwrap_or_else(|error| panic!("{}", error))
    }

    /// Ends live production in this injector tree for good: every running
    /// producer is cancelled, and its observers see `LiveProducerCancelled`.
    /// Afterwards no producer starts: a restart, or a live resolve that would
    /// need one, fails with `LiveProducerCancelled`.
    pub fn shutdown_live(&self) {
        let mut producers = self
            .inner
            .cells
            .live
            .producers
            .lock()
            .expect("live producer mutex poisoned");
        producers.shut_down = true;
        for record in producers
            .records
            .iter()
            .filter(|record| record.ended.is_none())
        {
            record.abort.abort();
        }
    }

    /// Outcome and timing of every live producer started so far, one row per
    /// generation.
    pub fn live_report(&self) -> Vec<LiveTiming> {
        self.inner.cells.live_report()
    }

    /// Follows [`Self::live_report`]: each change after this call is seen.
    pub fn report_changed(&self) -> LiveReportChanges {
        LiveReportChanges::new(
            self.inner.cells.live.report.subscribe(),
            self.inner.cells.clone(),
        )
    }

    /// The live cell of `key`, created on the first call; its first
    /// generation starts unless `cached` finds the value in `target`.
    fn live_cell<T>(
        &self,
        key: CellKey,
        member: Option<usize>,
        target: &Injector,
        after_shutdown: AfterShutdown,
        cached: impl Fn(&Injector) -> Option<Shared<Instance<T>>> + Send + Sync + 'static,
        produce: impl Fn(Injector, Injector, LivePublisher<T>) -> RunFuture<T> + Send + Sync + 'static,
    ) -> Result<Live<T>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_name = std::any::type_name::<T>();
        let registry = &self.inner.cells;
        let mut cells = registry
            .live
            .cells
            .lock()
            .expect("live cell mutex poisoned");
        let (state, start) = match cells.get(&key) {
            Some(entry) => (entry.publish::<T>().clone(), None),
            None => {
                let producer = RunRef::next(type_name);
                // A cell without a producer is not entered in `cells`: a hard
                // resolve then runs the factory itself instead of following it.
                let (initial, start, observed) = match cached(target) {
                    Some(instance) => (LiveState::Ready(instance.value()), None, true),
                    None => {
                        let runtime = registry.live.runtime(type_name)?;
                        match (registry.live.producers_for_start(type_name), after_shutdown) {
                            (Ok(producers), _) => (
                                LiveState::Pending,
                                Some((runtime, producers, producer)),
                                true,
                            ),
                            (Err(error), AfterShutdown::Cancelled) => {
                                (LiveState::Failed(error), None, false)
                            }
                            (Err(error), AfterShutdown::Refuse) => return Err(error),
                        }
                    }
                };
                let state: Publish<T> = Shared::new(
                    watch::channel(CellState {
                        generation: Generation::FIRST,
                        producer,
                        state: initial,
                    })
                    .0,
                );
                if observed {
                    cells.insert(
                        key.clone(),
                        LiveEntry {
                            state: Box::new(state.clone()),
                        },
                    );
                }
                (state, start)
            }
        };
        drop(cells);

        let starter = Shared::new(Starter {
            resolver: Shared::downgrade(&self.inner),
            target: Shared::downgrade(&target.inner),
            key,
            member,
            state: state.clone(),
            cached: Box::new(cached),
            produce: Box::new(produce),
        });
        let live = Live::new(
            CellWatch::new(state.subscribe(), registry.clone()),
            starter.clone(),
        );
        if let Some((runtime, producers, producer)) = start {
            registry.spawn_producer(
                producers,
                &runtime,
                member,
                Generation::FIRST,
                state,
                starter.run(
                    producer,
                    Generation::FIRST,
                    true,
                    self.clone(),
                    target.clone(),
                ),
            );
        }
        Ok(live)
    }

    fn live_cache_target<T: ?Sized>(&self, scope: Scope) -> Result<Injector, Error> {
        self.cache_target_for_scope(scope)
            .ok_or_else(|| Error::live_requires_cached_scope(std::any::type_name::<T>()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::Provider;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cached_service_holding_a_live_handle_does_not_keep_its_injector_alive() {
        struct Db;
        struct Holder(Live<Db>);
        let injector = Injector::root();
        injector.provide::<Db>(Provider::root_async(|_| async { Shared::new(Db) }));
        injector.provide::<Holder>(Provider::root(|injector: &Injector| {
            Shared::new(Holder(injector.resolve_live::<Db>()))
        }));
        let holder = injector.try_resolve::<Holder>().unwrap();
        holder.0.ready().await.unwrap();
        drop(holder);

        let inner = Shared::downgrade(&injector.inner);
        drop(injector);
        tokio::time::timeout(Duration::from_secs(5), async {
            while inner.strong_count() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a live handle in the cache keeps its injector alive");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_restart_after_the_injector_is_dropped_is_an_error() {
        struct Db;
        let injector = Injector::root();
        injector.provide::<Db>(Provider::root_async(|_| async { Shared::new(Db) }));
        let live = injector.resolve_live::<Db>();
        live.ready().await.unwrap();

        let inner = Shared::downgrade(&injector.inner);
        drop(injector);
        tokio::time::timeout(Duration::from_secs(5), async {
            while inner.strong_count() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the finished producer keeps its injector alive");

        let error = live.restart().err().expect("restarted without an injector");
        assert_eq!(error.kind, crate::ErrorKind::LiveInjectorDropped);
        assert_eq!(live.state().generation, Generation::FIRST);
    }
}
