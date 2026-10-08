//! Live cells: one observable state per cached binding, filled by a producer
//! task on the injector tree's tokio runtime.
//!
//! A producer resolves its binding through the in-flight cells like a hard
//! resolve does, so live and hard resolves share one factory run. It runs as
//! a run of its own in the wait-for graph: `Live::ready` waits on it, and it
//! waits on the run it drives or joins. A cycle through a spawned producer
//! therefore closes in the graph like any other cycle.

use std::panic::AssertUnwindSafe;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use futures::FutureExt;
use futures::future::AbortHandle;
use tokio::runtime::Handle;
use tokio::sync::watch;

use super::cells::{CURRENT_RUN, CellRegistry, RunRef};
use super::*;
use crate::future_local::WithLocal;
use crate::live::{Live, LiveOutcome, LiveReportChanges, LiveSet, LiveState, LiveTiming};

type Publish<T> = Shared<watch::Sender<LiveState<T>>>;

/// The live part of a [`CellRegistry`].
pub(crate) struct LiveCells {
    runtime: OnceLock<Handle>,
    cells: Mutex<HashMap<CellKey, LiveEntry>>,
    producers: Mutex<Vec<ProducerRecord>>,
    /// Counts producer starts and ends.
    report: watch::Sender<u64>,
}

impl Default for LiveCells {
    fn default() -> Self {
        Self {
            runtime: OnceLock::new(),
            cells: Mutex::new(HashMap::new()),
            producers: Mutex::new(Vec::new()),
            report: watch::channel(0).0,
        }
    }
}

struct LiveEntry {
    producer: RunRef,
    /// `Publish<T>` for the key's `T`.
    state: Box<dyn Any + Send + Sync>,
}

impl LiveEntry {
    fn handle<T: ?Sized + Send + Sync + 'static>(
        &self,
        registry: &Shared<CellRegistry>,
    ) -> Live<T> {
        let state = self
            .state
            .downcast_ref::<Publish<T>>()
            .expect("a live cell key maps to one value type");
        Live::new(state.subscribe(), self.producer, registry.clone())
    }
}

struct ProducerRecord {
    type_name: &'static str,
    member: Option<usize>,
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

    fn bump_report(&self) {
        self.report.send_modify(|changes| *changes += 1);
    }
}

impl CellRegistry {
    /// The live cell a resolve of `key` must wait on instead of starting or
    /// joining a run.
    pub(super) fn live_observed<T: ?Sized + Send + Sync + 'static>(
        self: &Shared<Self>,
        key: &CellKey,
    ) -> Option<Live<T>> {
        let cells = self.live.cells.lock().expect("live cell mutex poisoned");
        cells.get(key).map(|entry| entry.handle(self))
    }

    /// Spawns `run` as the producer of `state`. The future of an aborted
    /// producer is dropped, and its observers see `LiveProducerCancelled`.
    fn spawn_producer<T, F>(
        self: &Shared<Self>,
        runtime: &Handle,
        member: Option<usize>,
        state: Publish<T>,
        run: F,
    ) where
        T: ?Sized + Send + Sync + 'static,
        F: Future<Output = Result<Shared<Instance<T>>, Error>> + Send + 'static,
    {
        let type_name = std::any::type_name::<T>();
        let mut producers = self
            .live
            .producers
            .lock()
            .expect("live producer mutex poisoned");
        let reporter = Reporter {
            registry: self.clone(),
            record: producers.len(),
            type_name,
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
        producers.push(ProducerRecord {
            type_name,
            member,
            started: Instant::now(),
            ended: None,
            abort,
        });
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
            .iter()
            .map(|record| {
                let (outcome, elapsed) = match &record.ended {
                    Some((outcome, elapsed)) => (outcome.clone(), *elapsed),
                    None => (LiveOutcome::Running, record.started.elapsed()),
                };
                LiveTiming {
                    type_name: record.type_name,
                    member: record.member,
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
            LiveState::Pending | LiveState::Partial(_) => {
                unreachable!("a producer reports a terminal state")
            }
        };
        {
            let mut producers = self
                .registry
                .live
                .producers
                .lock()
                .expect("live producer mutex poisoned");
            let record = &mut producers[self.record];
            record.ended = Some((outcome, record.started.elapsed()));
        }
        self.state.send_replace(state);
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
    /// call; later live and hard resolves share that producer.
    pub fn try_resolve_live<T>(&self) -> Result<Live<T>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let provider = self.resolve_provider::<T>()?;
        let target = self.live_cache_target::<T>(provider.scope)?;
        let key = CellKey::new(&target, BindingKey::Single(TypeId::of::<T>()));
        let cache = target.clone();
        let resolver = self.clone();
        self.live_cell(
            key,
            None,
            move || cache.get_instance::<T>(),
            move || {
                let (injector, target) = (resolver.clone(), target.clone());
                Box::pin(async move {
                    let instance = injector.resolve_instance_async::<T>().await?;
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
    pub fn try_resolve_all_live<T>(&self) -> Result<LiveSet<T>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let providers = self.resolve_set_providers::<T>()?;
        let targets = providers
            .iter()
            .map(|provider| self.live_cache_target::<T>(provider.scope))
            .collect::<Result<Vec<_>, _>>()?;
        let members = providers
            .into_iter()
            .zip(targets)
            .enumerate()
            .map(|(member, (provider, target))| {
                let key = CellKey::new(
                    &target,
                    BindingKey::SetMember(SetProviderKey::of::<T>(&provider)),
                );
                let (cache, cached_provider) = (target.clone(), provider.clone());
                let resolver = self.clone();
                self.live_cell(
                    key,
                    Some(member),
                    move || cache.get_set_instance::<T>(&cached_provider),
                    move || {
                        let (injector, provider, target) =
                            (resolver.clone(), provider.clone(), target.clone());
                        Box::pin(async move {
                            let instance = injector
                                .resolve_instance_from_provider_async::<T>(&provider)
                                .await?;
                            target.store_set_instance::<T>(&provider, instance.clone());
                            Ok(instance)
                        }) as RunFuture<T>
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LiveSet::new(members))
    }

    pub fn resolve_all_live<T>(&self) -> LiveSet<T>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.try_resolve_all_live::<T>()
            .unwrap_or_else(|error| panic!("{}", error))
    }

    /// Cancels every live producer of this injector tree that has not
    /// finished. Their observers see `LiveProducerCancelled`.
    pub fn shutdown_live(&self) {
        let producers = self
            .inner
            .cells
            .live
            .producers
            .lock()
            .expect("live producer mutex poisoned");
        for record in producers.iter().filter(|record| record.ended.is_none()) {
            record.abort.abort();
        }
    }

    /// Outcome and timing of every live producer started so far.
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

    /// The live cell of `key`, created and started on the first call.
    fn live_cell<T>(
        &self,
        key: CellKey,
        member: Option<usize>,
        cached: impl Fn() -> Option<Shared<Instance<T>>> + Send + Sync + 'static,
        produce: impl Fn() -> RunFuture<T> + Send + Sync + 'static,
    ) -> Result<Live<T>, Error>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let type_name = std::any::type_name::<T>();
        let registry = &self.inner.cells;
        let runtime = registry.live.runtime(type_name)?;
        let mut cells = registry
            .live
            .cells
            .lock()
            .expect("live cell mutex poisoned");
        if let Some(entry) = cells.get(&key) {
            return Ok(entry.handle(registry));
        }

        let producer = RunRef::next(type_name);
        let initial = match cached() {
            Some(instance) => LiveState::Ready(instance.value()),
            None => LiveState::Pending,
        };
        let start = matches!(initial, LiveState::Pending);
        let state: Publish<T> = Shared::new(watch::channel(initial).0);
        let entry = LiveEntry {
            producer,
            state: Box::new(state.clone()),
        };
        let live = entry.handle(registry);
        cells.insert(key.clone(), entry);
        drop(cells);

        if start {
            let resolver = self.clone();
            let run = async move {
                resolver
                    .produce_in_cell(key, cached, move || {
                        Box::pin(AssertUnwindSafe(produce()).catch_unwind().map(|outcome| {
                            outcome.unwrap_or_else(|panic| {
                                Err(Error::factory_panicked(type_name, &panic_message(&*panic)))
                            })
                        })) as RunFuture<T>
                    })
                    .await
            };
            let run = WithLocal::new(
                &CURRENT_RUN,
                producer,
                crate::resolve_guard::resolving_on_new_path(TypeId::of::<T>(), run),
            );
            registry.spawn_producer(&runtime, member, state, run);
        }
        Ok(live)
    }

    fn live_cache_target<T: ?Sized>(&self, scope: Scope) -> Result<Injector, Error> {
        self.cache_target_for_scope(scope)
            .ok_or_else(|| Error::live_requires_cached_scope(std::any::type_name::<T>()))
    }
}
