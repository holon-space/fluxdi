//! In-flight cells: at most one factory run per cached binding at a time.
//!
//! A resolve that misses the cache either drives the binding's run or joins
//! the run already in flight. The driver polls the factory inline on its own
//! task, so no task is spawned and the factory never wakes its own poller;
//! joiners wait for the outcome the driver publishes. A driver that goes away
//! before the outcome publishes `Abandoned`, and its joiners start over.
//!
//! Joining is a wait across resolution paths, which `ResolveGuard` cannot
//! see. The registry therefore keeps a wait-for graph between runs and
//! refuses a wait that would close a cycle with `CircularDependency`.

use std::any::Any;
use std::cell::Cell as LocalCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use super::{Injector, NamedTypeKey, SetProviderKey};
use crate::error::Error;
use crate::future_local::{self, WithLocal};
use crate::instance::Instance;
use crate::runtime::Shared;

/// Process-unique identity of one injector; never reused, unlike its address.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ScopeId(u64);

impl ScopeId {
    pub(crate) fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum BindingKey {
    Single(std::any::TypeId),
    Named(NamedTypeKey),
    SetMember(SetProviderKey),
}

/// A cached binding in the injector that caches it.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct CellKey {
    scope: ScopeId,
    binding: BindingKey,
}

impl CellKey {
    pub(super) fn new(cache_target: &Injector, binding: BindingKey) -> Self {
        Self {
            scope: cache_target.inner.scope_id,
            binding,
        }
    }
}

/// One factory run; a retry after a failure is a new run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RunRef {
    id: u64,
    type_name: &'static str,
}

thread_local! {
    /// The run whose factory is being polled on this thread.
    static CURRENT_RUN: LocalCell<Option<RunRef>> = const { LocalCell::new(None) };
}

#[cfg(feature = "thread-safe")]
pub(crate) trait CellValue: Send + Sync + 'static {}
#[cfg(feature = "thread-safe")]
impl<T: ?Sized + Send + Sync + 'static> CellValue for T {}
#[cfg(not(feature = "thread-safe"))]
pub(crate) trait CellValue: 'static {}
#[cfg(not(feature = "thread-safe"))]
impl<T: ?Sized + 'static> CellValue for T {}

#[cfg(feature = "thread-safe")]
pub(super) type RunFuture<T> =
    Pin<Box<dyn Future<Output = Result<Shared<Instance<T>>, Error>> + Send>>;
#[cfg(not(feature = "thread-safe"))]
pub(super) type RunFuture<T> = Pin<Box<dyn Future<Output = Result<Shared<Instance<T>>, Error>>>>;

#[cfg(feature = "thread-safe")]
type ErasedRunCell = Box<dyn Any + Send + Sync>;
#[cfg(not(feature = "thread-safe"))]
type ErasedRunCell = Box<dyn Any>;

type RunResult<T> = Result<Shared<Instance<T>>, Error>;

enum Outcome<T: ?Sized + CellValue> {
    Done(RunResult<T>),
    /// The driver went away before its factory completed.
    Abandoned,
}

impl<T: ?Sized + CellValue> Clone for Outcome<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Done(result) => Self::Done(result.clone()),
            Self::Abandoned => Self::Abandoned,
        }
    }
}

/// Where one run publishes its outcome to its joiners.
struct RunCell<T: ?Sized + CellValue> {
    state: Mutex<RunState<T>>,
}

struct RunState<T: ?Sized + CellValue> {
    outcome: Option<Outcome<T>>,
    joiners: Vec<Waker>,
}

impl<T: ?Sized + CellValue> RunCell<T> {
    fn publish(&self, outcome: Outcome<T>) {
        let joiners = {
            let mut state = self.state.lock().expect("run cell mutex poisoned");
            assert!(state.outcome.is_none(), "a run publishes one outcome");
            state.outcome = Some(outcome);
            std::mem::take(&mut state.joiners)
        };
        for joiner in joiners {
            joiner.wake();
        }
    }
}

struct Join<T: ?Sized + CellValue> {
    cell: Shared<RunCell<T>>,
}

impl<T: ?Sized + CellValue> Future for Join<T> {
    type Output = Outcome<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome<T>> {
        let mut state = self.cell.state.lock().expect("run cell mutex poisoned");
        if let Some(outcome) = &state.outcome {
            return Poll::Ready(outcome.clone());
        }
        if !state.joiners.iter().any(|w| w.will_wake(cx.waker())) {
            state.joiners.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

struct RunEntry {
    run: RunRef,
    /// `Shared<RunCell<T>>` for the key's `T`.
    cell: ErasedRunCell,
}

/// In-flight runs and the waits between them, shared by an injector tree.
#[derive(Default)]
pub(crate) struct CellRegistry {
    runs: Mutex<HashMap<CellKey, RunEntry>>,
    /// `(waiter, awaited)`: the factory of `waiter` awaits the run `awaited`.
    waits: Mutex<Vec<(RunRef, RunRef)>>,
}

enum Cell<T: ?Sized + CellValue> {
    Cached(Shared<Instance<T>>),
    Drive(Driver<T>),
    Join(RunRef, Shared<RunCell<T>>),
}

impl CellRegistry {
    /// The run in flight for `key`, or `cached()` if the binding was cached
    /// meanwhile, or a new run for the caller to drive.
    ///
    /// The driver must store its instance in the cache before it finishes:
    /// the entry is removed only then, so a lookup that finds no entry finds
    /// the instance.
    fn cell<T: ?Sized + CellValue>(
        self: &Shared<Self>,
        key: CellKey,
        cached: impl FnOnce() -> Option<Shared<Instance<T>>>,
    ) -> Cell<T> {
        static NEXT_RUN: AtomicU64 = AtomicU64::new(0);

        let mut runs = self.runs.lock().expect("cell registry mutex poisoned");
        if let Some(entry) = runs.get(&key) {
            let cell = entry
                .cell
                .downcast_ref::<Shared<RunCell<T>>>()
                .expect("a cell key maps to one instance type");
            return Cell::Join(entry.run, cell.clone());
        }
        if let Some(instance) = cached() {
            return Cell::Cached(instance);
        }

        let run = RunRef {
            id: NEXT_RUN.fetch_add(1, Ordering::Relaxed),
            type_name: std::any::type_name::<T>(),
        };
        let cell = Shared::new(RunCell {
            state: Mutex::new(RunState {
                outcome: None,
                joiners: Vec::new(),
            }),
        });
        runs.insert(
            key.clone(),
            RunEntry {
                run,
                cell: Box::new(cell.clone()),
            },
        );
        Cell::Drive(Driver {
            registry: self.clone(),
            key,
            run,
            cell,
            finished: false,
        })
    }

    /// Records that the run being polled on this thread, if any, awaits
    /// `awaited`, unless `awaited` already waits for it.
    fn begin_wait(&self, awaited: RunRef) -> Result<WaitEdge<'_>, Error> {
        let Some(waiter) = future_local::current(&CURRENT_RUN) else {
            return Ok(WaitEdge {
                registry: self,
                edge: None,
            });
        };
        let mut waits = self.waits.lock().expect("cell wait mutex poisoned");
        let mut path = Vec::new();
        if wait_path(&waits, awaited, waiter, &mut path) {
            let names: Vec<&str> = std::iter::once(waiter)
                .chain(path)
                .map(|run| run.type_name)
                .collect();
            return Err(Error::circular_dependency(&names));
        }
        waits.push((waiter, awaited));
        Ok(WaitEdge {
            registry: self,
            edge: Some((waiter, awaited)),
        })
    }

    #[cfg(test)]
    pub(super) fn in_flight(&self) -> usize {
        self.runs
            .lock()
            .expect("cell registry mutex poisoned")
            .len()
    }
}

/// The caller that runs a new run's factory. Dropped unfinished, it
/// publishes `Abandoned`.
struct Driver<T: ?Sized + CellValue> {
    registry: Shared<CellRegistry>,
    key: CellKey,
    run: RunRef,
    cell: Shared<RunCell<T>>,
    finished: bool,
}

impl<T: ?Sized + CellValue> Driver<T> {
    fn finish(mut self, result: RunResult<T>) {
        self.finished = true;
        self.end(Outcome::Done(result));
    }

    fn end(&self, outcome: Outcome<T>) {
        let removed = self
            .registry
            .runs
            .lock()
            .expect("cell registry mutex poisoned")
            .remove(&self.key)
            .expect("a run's entry stays until its driver ends");
        assert_eq!(removed.run, self.run, "a run's entry belongs to its driver");
        self.cell.publish(outcome);
    }
}

impl<T: ?Sized + CellValue> Drop for Driver<T> {
    fn drop(&mut self) {
        if !self.finished {
            self.end(Outcome::Abandoned);
        }
    }
}

struct WaitEdge<'a> {
    registry: &'a CellRegistry,
    edge: Option<(RunRef, RunRef)>,
}

impl Drop for WaitEdge<'_> {
    fn drop(&mut self) {
        let Some(edge) = self.edge else {
            return;
        };
        let mut waits = self
            .registry
            .waits
            .lock()
            .expect("cell wait mutex poisoned");
        let position = waits
            .iter()
            .position(|recorded| *recorded == edge)
            .expect("a wait edge stays recorded until its guard drops");
        waits.swap_remove(position);
    }
}

/// Whether `from` reaches `to` along recorded waits; on success `path` holds
/// the runs from `from` to `to`.
fn wait_path(waits: &[(RunRef, RunRef)], from: RunRef, to: RunRef, path: &mut Vec<RunRef>) -> bool {
    path.push(from);
    if from == to {
        return true;
    }
    for &(waiter, awaited) in waits {
        if waiter == from && !path.contains(&awaited) && wait_path(waits, awaited, to, path) {
            return true;
        }
    }
    path.pop();
    false
}

impl Injector {
    /// Resolves a cached binding through its cell in `key`; `produce` runs
    /// the factory and stores the instance in the cache.
    pub(super) async fn resolve_in_cell<T: ?Sized + CellValue>(
        &self,
        key: CellKey,
        cached: impl Fn() -> Option<Shared<Instance<T>>>,
        produce: impl Fn() -> RunFuture<T>,
    ) -> RunResult<T> {
        let registry = &self.inner.cells;
        loop {
            match registry.cell(key.clone(), &cached) {
                Cell::Cached(instance) => return Ok(instance),
                Cell::Drive(driver) => {
                    let _edge = registry.begin_wait(driver.run)?;
                    let result = WithLocal::new(&CURRENT_RUN, driver.run, produce()).await;
                    driver.finish(result.clone());
                    return result;
                }
                Cell::Join(run, cell) => {
                    let _edge = registry.begin_wait(run)?;
                    match (Join { cell }).await {
                        Outcome::Done(result) => return result,
                        Outcome::Abandoned => continue,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    fn run(id: u64) -> RunRef {
        RunRef { id, type_name: "T" }
    }

    #[test]
    fn a_run_waiting_for_itself_is_a_cycle() {
        let registry = CellRegistry::default();
        let err = block_on(WithLocal::new(&CURRENT_RUN, run(1), async {
            registry.begin_wait(run(1)).map(|_| ())
        }))
        .unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::CircularDependency);
    }

    #[test]
    fn a_wait_closing_a_chain_of_waits_is_a_cycle() {
        let registry = CellRegistry::default();
        let first = block_on(WithLocal::new(&CURRENT_RUN, run(1), async {
            registry.begin_wait(run(2))
        }))
        .unwrap();
        let second = block_on(WithLocal::new(&CURRENT_RUN, run(2), async {
            registry.begin_wait(run(3))
        }))
        .unwrap();

        let err = block_on(WithLocal::new(&CURRENT_RUN, run(3), async {
            registry.begin_wait(run(1)).map(|_| ())
        }))
        .unwrap_err();
        assert_eq!(err.kind, crate::ErrorKind::CircularDependency);

        drop((first, second));
        block_on(WithLocal::new(&CURRENT_RUN, run(3), async {
            registry.begin_wait(run(1)).map(|_| ())
        }))
        .unwrap();
        assert!(registry.waits.lock().unwrap().is_empty());
    }
}
