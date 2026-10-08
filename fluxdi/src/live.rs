//! Live dependency edges.
//!
//! A hard edge (`resolve_async`) waits for the finished value. A live edge
//! ([`Injector::resolve_live`](crate::Injector::resolve_live),
//! [`Injector::resolve_all_live`](crate::Injector::resolve_all_live)) returns
//! a handle at once; the handle reports the producer's state as it changes.
//! The producer starts on the first live resolve of its binding, never at
//! container build, and every live or hard resolve shares it.
//!
//! Each run of the producer is a [`Generation`]. Within a generation `Ready`
//! and `Failed` are terminal; [`Live::restart`] starts the next generation in
//! the same cell. Every state and value a handle returns names its generation.
//! A hard resolve returns the value of the last `Ready` generation and never
//! waits for a restart; to follow restarts, hold a [`Live`] and check
//! [`Live::is_current`].

use std::time::Duration;

use tokio::sync::watch;

use crate::error::Error;
use crate::injector::cells::{CellRegistry, RunRef};
use crate::injector::live_cells::Starter;
use crate::runtime::Shared;

pub use live_envelope::{Completeness, Generation, Generational, Observed};

/// State of one live dependency.
pub type LiveState<T> = Observed<Shared<T>, Error>;

pub(crate) fn is_terminal<T: ?Sized>(state: &LiveState<T>) -> bool {
    matches!(state, LiveState::Ready(_) | LiveState::Failed(_))
}

/// What a live cell publishes.
pub(crate) struct CellState<T: ?Sized> {
    pub(crate) generation: Generation,
    /// The producer of `generation`.
    pub(crate) producer: RunRef,
    pub(crate) state: LiveState<T>,
}

impl<T: ?Sized> Clone for CellState<T> {
    fn clone(&self) -> Self {
        Self {
            generation: self.generation,
            producer: self.producer,
            state: self.state.clone(),
        }
    }
}

impl<T: ?Sized> CellState<T> {
    fn observed(&self) -> Generational<LiveState<T>> {
        Generational::new(self.generation, self.state.clone())
    }
}

/// Reads and waits on one live cell; restarting is up to [`Live`].
pub(crate) struct CellWatch<T: ?Sized> {
    rx: watch::Receiver<CellState<T>>,
    /// Holds the cell's sender, so `rx` never sees the channel closed.
    registry: Shared<CellRegistry>,
}

impl<T: ?Sized> Clone for CellWatch<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
            registry: self.registry.clone(),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> CellWatch<T> {
    pub(crate) fn new(rx: watch::Receiver<CellState<T>>, registry: Shared<CellRegistry>) -> Self {
        Self { rx, registry }
    }

    /// Waits for a terminal state of the newest generation seen, and records
    /// the wait on that generation's producer in the wait-for graph.
    pub(crate) async fn ready(&self) -> Result<Generational<Shared<T>>, Error> {
        self.settle(true).await
    }

    async fn settle(&self, record_wait: bool) -> Result<Generational<Shared<T>>, Error> {
        let mut rx = self.rx.clone();
        let mut current = rx.borrow_and_update().clone();
        while !is_terminal(&current.state) {
            let _wait = record_wait
                .then(|| self.registry.begin_wait(current.producer))
                .transpose()?;
            let generation = current.generation;
            current = rx
                .wait_for(|cell| cell.generation != generation || is_terminal(&cell.state))
                .await
                .expect("the registry keeps a live cell's sender")
                .clone();
        }
        match current.state {
            LiveState::Ready(value) => Ok(Generational::new(current.generation, value)),
            LiveState::Failed(error) => Err(error),
            _ => unreachable!("the loop ends on a terminal state"),
        }
    }
}

/// Handle to one live dependency. Cloning shares the same producer.
pub struct Live<T: ?Sized + 'static> {
    watch: CellWatch<T>,
    starter: Shared<Starter<T>>,
}

impl<T: ?Sized + 'static> Clone for Live<T> {
    fn clone(&self) -> Self {
        Self {
            watch: self.watch.clone(),
            starter: self.starter.clone(),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> Live<T> {
    pub(crate) fn new(watch: CellWatch<T>, starter: Shared<Starter<T>>) -> Self {
        Self { watch, starter }
    }

    pub fn state(&self) -> Generational<LiveState<T>> {
        self.watch.rx.borrow().observed()
    }

    /// Waits for the newest state after the last one this handle saw. States
    /// that were replaced before this handle looked are skipped; a jump in
    /// the generation shows that one was.
    pub async fn changed(&mut self) -> Generational<LiveState<T>> {
        self.watch
            .rx
            .changed()
            .await
            .expect("the registry keeps a live cell's sender");
        self.watch.rx.borrow_and_update().observed()
    }

    /// The hard edge on a live dependency: waits for a terminal state of the
    /// newest generation it sees. Fails at once with `CircularDependency`
    /// when the producer, directly or through other runs, already waits for
    /// the caller's run.
    pub async fn ready(&self) -> Result<Generational<Shared<T>>, Error> {
        self.watch.ready().await
    }

    /// Whether `generation` is the newest generation of this dependency.
    pub fn is_current(&self, generation: Generation) -> bool {
        self.watch.rx.borrow().generation == generation
    }

    /// Starts the next generation: the factory runs again, and observers
    /// see the new generation `Pending`. Only a terminal generation can be
    /// restarted (`LiveRestartWhileRunning` otherwise), so one cell never has
    /// two producers. Fails with `LiveProducerCancelled` after
    /// [`Injector::shutdown_live`](crate::Injector::shutdown_live).
    pub fn restart(&self) -> Result<Generation, Error> {
        self.starter.restart()
    }
}

/// Handle to a set of live dependencies, one slot per registered member in
/// registration order.
pub struct LiveSet<T: ?Sized + 'static> {
    members: Vec<Live<T>>,
}

impl<T: ?Sized + 'static> Clone for LiveSet<T> {
    fn clone(&self) -> Self {
        Self {
            members: self.members.clone(),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> LiveSet<T> {
    pub(crate) fn new(members: Vec<Live<T>>) -> Self {
        Self { members }
    }

    pub fn members(&self) -> Vec<Generational<LiveState<T>>> {
        self.members.iter().map(Live::state).collect()
    }

    pub fn ready_members(&self) -> Vec<Generational<Shared<T>>> {
        self.members
            .iter()
            .filter_map(|member| {
                let state = member.state();
                match state.value {
                    LiveState::Ready(value) => Some(Generational::new(state.generation, value)),
                    _ => None,
                }
            })
            .collect()
    }

    /// Waits for the next change of any member after the last states this
    /// handle saw, and returns every member's state.
    pub async fn changed(&mut self) -> Vec<Generational<LiveState<T>>> {
        if self.members.is_empty() {
            return std::future::pending().await;
        }
        let changes = self
            .members
            .iter_mut()
            .map(|member| Box::pin(member.watch.rx.changed()));
        futures::future::select_all(changes)
            .await
            .0
            .expect("the registry keeps a live cell's sender");
        self.members
            .iter_mut()
            .map(|member| member.watch.rx.borrow_and_update().observed())
            .collect()
    }

    /// Waits until every member's newest generation is terminal. Fails with
    /// the first failed member's error, in registration order.
    pub async fn complete(&self) -> Result<Vec<Generational<Shared<T>>>, Error> {
        let mut outcomes = Vec::with_capacity(self.members.len());
        for member in &self.members {
            outcomes.push(member.watch.settle(false).await);
        }
        outcomes.into_iter().collect()
    }

    /// [`Live::restart`] for the member in `slot`; the slot keeps its place.
    /// Fails with `LiveSlotOutOfRange` when the set has no such slot.
    pub fn restart(&self, slot: usize) -> Result<Generation, Error> {
        let member = self.members.get(slot).ok_or_else(|| {
            Error::live_slot_out_of_range(std::any::type_name::<T>(), slot, self.members.len())
        })?;
        member.restart()
    }
}

/// More outcomes may be added, so a match needs a wildcard arm:
///
/// ```compile_fail,E0004
/// use fluxdi::LiveOutcome;
///
/// fn label(outcome: &LiveOutcome) -> &'static str {
///     match outcome {
///         LiveOutcome::Running => "running",
///         LiveOutcome::Ready => "ready",
///         LiveOutcome::Failed(_) => "failed",
///     }
/// }
/// ```
#[derive(Clone)]
#[cfg_attr(feature = "debug", derive(Debug))]
#[non_exhaustive]
pub enum LiveOutcome {
    Running,
    Ready,
    Failed(Error),
}

/// One generation's producer: outcome and timing. Only fluxdi builds it, so
/// fields may be added:
///
/// ```compile_fail,E0639
/// use fluxdi::{Generation, LiveOutcome, LiveTiming};
///
/// let timing = LiveTiming {
///     type_name: "Db",
///     member: None,
///     generation: Generation::FIRST,
///     outcome: LiveOutcome::Running,
///     elapsed: std::time::Duration::ZERO,
/// };
/// ```
#[derive(Clone)]
#[cfg_attr(feature = "debug", derive(Debug))]
#[non_exhaustive]
pub struct LiveTiming {
    pub type_name: &'static str,
    /// The slot in the set this producer fills, for a set member.
    pub member: Option<usize>,
    pub generation: Generation,
    pub outcome: LiveOutcome,
    /// Until the outcome, or until now while running.
    pub elapsed: Duration,
}

/// Follows [`Injector::live_report`](crate::Injector::live_report).
pub struct LiveReportChanges {
    rx: watch::Receiver<u64>,
    registry: Shared<CellRegistry>,
}

impl LiveReportChanges {
    pub(crate) fn new(rx: watch::Receiver<u64>, registry: Shared<CellRegistry>) -> Self {
        Self { rx, registry }
    }

    /// Waits until a producer starts or ends after the last report this
    /// handle returned, and returns the report.
    pub async fn changed(&mut self) -> Vec<LiveTiming> {
        self.rx
            .changed()
            .await
            .expect("the registry keeps the report sender");
        self.rx.borrow_and_update();
        self.registry.live_report()
    }
}

#[cfg(test)]
mod tests;
