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
//!
//! The factory of a live provider gets a [`LivePublisher`] and may publish
//! `Partial` values before it returns. Only a live handle's state shows
//! them; `ready()` and hard resolves wait for the final value.

use std::time::Duration;

use tokio::sync::watch;

mod channel;
pub(crate) use channel::CellReceiver;
use channel::RunEnd;

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
    end: RunEnd<T>,
}

impl<T: ?Sized> Clone for CellState<T> {
    fn clone(&self) -> Self {
        Self {
            generation: self.generation,
            producer: self.producer,
            state: self.state.clone(),
            end: self.end.clone(),
        }
    }
}

impl<T: ?Sized> CellState<T> {
    pub(crate) fn new(generation: Generation, producer: RunRef, state: LiveState<T>) -> Self {
        Self {
            generation,
            producer,
            end: RunEnd::new(&state),
            state,
        }
    }

    /// Ends this generation with the terminal `state`.
    pub(crate) fn finish(&mut self, state: LiveState<T>) {
        self.end.publish(&state);
        self.state = state;
    }

    fn observed(&self) -> Generational<LiveState<T>> {
        Generational::new(self.generation, self.state.clone())
    }
}

/// Reads and waits on one live cell; restarting is up to [`Live`].
///
/// Every wait for a producer to end goes through [`CellWatch::settle`],
/// which records it in the wait-for graph. `changed()` records nothing: it
/// observes the next state and does not wait for a producer to end.
pub(crate) struct CellWatch<T: ?Sized> {
    rx: CellReceiver<T>,
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
        Self {
            rx: CellReceiver::new(rx),
            registry,
        }
    }

    /// The outcome of the newest generation seen when the wait ends.
    pub(crate) async fn ready(&self) -> Result<Generational<Shared<T>>, Error> {
        loop {
            let generation = self.rx.borrow().generation;
            let outcome = self.settle().await?;
            if self.rx.borrow().generation == generation {
                return outcome;
            }
        }
    }

    /// Waits for the terminal state of the generation current at the call,
    /// with a wait edge on its producer while it runs. The outer `Err` is a
    /// refused wait (`CircularDependency`); the inner result is the outcome.
    fn settle(
        &self,
    ) -> impl Future<Output = Result<Result<Generational<Shared<T>>, Error>, Error>> + Send + 'static
    {
        let registry = self.registry.clone();
        let current = self.rx.borrow().clone();
        async move {
            let state = if is_terminal(&current.state) {
                current.state
            } else {
                let edge = registry.begin_wait(current.producer)?;
                current.end.wait(edge).await?
            };
            Ok(match state {
                LiveState::Ready(value) => Ok(Generational::new(current.generation, value)),
                LiveState::Failed(error) => Err(error),
                _ => unreachable!("a generation ends in a terminal state"),
            })
        }
    }
}

/// Lets the factory of a live provider (`Provider::root_live` and its
/// siblings) publish usable but incomplete values while it runs. A partial
/// never completes a [`Live::ready`] or a hard resolve; those return only
/// the factory's final value.
///
/// A publisher belongs to one generation of one live cell and must not
/// outlive its factory run: publishing after that generation ended, by
/// returning, failing, being cancelled, or being followed by a restart, is
/// a programming error and panics.
pub struct LivePublisher<T: ?Sized + 'static> {
    /// `None` when no live cell observes the run, as in a hard resolve that
    /// started it.
    cell: Option<(Shared<watch::Sender<CellState<T>>>, Generation)>,
    /// The provider's decorators, applied to each partial.
    decorate: Option<Decorate<T>>,
}

type Decorate<T> = Shared<dyn Fn(Shared<T>) -> Shared<T> + Send + Sync>;

impl<T: ?Sized + 'static> LivePublisher<T> {
    pub(crate) fn new(cell: Shared<watch::Sender<CellState<T>>>, generation: Generation) -> Self {
        Self {
            cell: Some((cell, generation)),
            decorate: None,
        }
    }

    pub(crate) fn unobserved() -> Self {
        Self {
            cell: None,
            decorate: None,
        }
    }

    /// This publisher with `decorator` applied to each partial before the
    /// decorators it already applies.
    pub(crate) fn decorated<F>(self, decorator: Shared<F>) -> Self
    where
        F: Fn(Shared<T>) -> Shared<T> + Send + Sync + 'static,
    {
        let decorate: Decorate<T> = match self.decorate {
            None => decorator,
            Some(outer) => Shared::new(move |value| outer(decorator(value))),
        };
        Self {
            cell: self.cell,
            decorate: Some(decorate),
        }
    }

    /// Publishes `value` as the `Partial` state of this publisher's
    /// generation, replacing an earlier partial.
    pub fn partial(&self, value: Shared<T>) {
        let Some((cell, generation)) = &self.cell else {
            return;
        };
        let value = match &self.decorate {
            Some(decorate) => decorate(value),
            None => value,
        };
        let mut newest = *generation;
        let mut ended = false;
        cell.send_if_modified(|cell| {
            newest = cell.generation;
            ended = is_terminal(&cell.state);
            if newest != *generation || ended {
                return false;
            }
            cell.state = LiveState::Partial(value);
            true
        });
        let type_name = std::any::type_name::<T>();
        assert_eq!(
            newest,
            *generation,
            "a live publisher of {type_name} generation {} published after generation {} started",
            generation.get(),
            newest.get()
        );
        assert!(
            !ended,
            "a live publisher of {type_name} generation {} published after its producer ended",
            generation.get()
        );
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
    ///
    /// An observation, not a wait for the producer to end: the wait-for graph
    /// does not see it, so a cycle through `changed()` is not refused.
    pub async fn changed(&mut self) -> Generational<LiveState<T>> {
        self.watch.rx.changed().await;
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

/// Handle to a set of live dependencies, one slot per registered member.
///
/// The set grows: a member registered after the set was first resolved gets
/// the next slot, and its producer starts. A slot is never removed or
/// moved, so a slot index names one member for good. Slot order is the order
/// in which the set first saw each member, which is not always the order of
/// [`Injector::try_resolve_all_async`](crate::Injector::try_resolve_all_async).
pub struct LiveSet<T: ?Sized + 'static> {
    /// Every slot of the set, shared with the registry, which appends.
    membership: Shared<watch::Sender<Vec<Live<T>>>>,
    slots: watch::Receiver<Vec<Live<T>>>,
    /// The slots this handle has seen, each with the last state it saw.
    seen: Vec<Live<T>>,
}

impl<T: ?Sized + 'static> Clone for LiveSet<T> {
    fn clone(&self) -> Self {
        Self {
            membership: self.membership.clone(),
            slots: self.slots.clone(),
            seen: self.seen.clone(),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> LiveSet<T> {
    pub(crate) fn new(membership: Shared<watch::Sender<Vec<Live<T>>>>) -> Self {
        let mut slots = membership.subscribe();
        let seen = slots
            .borrow_and_update()
            .iter()
            .map(|member| {
                let mut member = member.clone();
                member.watch.rx.borrow_and_update();
                member
            })
            .collect();
        Self {
            membership,
            slots,
            seen,
        }
    }

    fn snapshot(&self) -> Vec<Live<T>> {
        self.slots.borrow().clone()
    }

    pub fn members(&self) -> Vec<Generational<LiveState<T>>> {
        self.slots.borrow().iter().map(Live::state).collect()
    }

    pub fn ready_members(&self) -> Vec<Generational<Shared<T>>> {
        self.slots
            .borrow()
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

    /// Waits for the next change after the last states this handle saw: a
    /// member's state, or a new slot (which starts in generation 1). Returns
    /// every slot's state. Like [`Live::changed`], an observation the
    /// wait-for graph does not see.
    pub async fn changed(&mut self) -> Vec<Generational<LiveState<T>>> {
        if self.seen.is_empty() {
            self.slots
                .changed()
                .await
                .expect("a live set holds its membership sender");
        } else {
            let grown = Box::pin(self.slots.changed());
            let changes = futures::future::select_all(
                self.seen
                    .iter_mut()
                    .map(|member| Box::pin(member.watch.rx.changed())),
            );
            match futures::future::select(grown, changes).await {
                futures::future::Either::Left((grown, _)) => {
                    grown.expect("a live set holds its membership sender")
                }
                futures::future::Either::Right(_) => {}
            }
        }
        let slots = self.slots.borrow_and_update().clone();
        self.seen.extend(slots.into_iter().skip(self.seen.len()));
        self.seen
            .iter_mut()
            .map(|member| member.watch.rx.borrow_and_update().observed())
            .collect()
    }

    /// Waits until every slot present at the call ends the generation it
    /// had at the call; a slot added or a generation restarted later is not
    /// waited for. Fails with the first failed member's error, in slot order.
    ///
    /// Waits on every unfinished member at once, so it fails at once with
    /// `CircularDependency` when a cycle closes through one of these waits,
    /// on either side.
    pub fn complete(
        &self,
    ) -> impl Future<Output = Result<Vec<Generational<Shared<T>>>, Error>> + Send + 'static {
        let settles: Vec<_> = self
            .snapshot()
            .iter()
            .map(|member| member.watch.settle())
            .collect();
        async move {
            let outcomes = futures::future::try_join_all(settles).await?;
            outcomes.into_iter().collect()
        }
    }

    /// [`Live::restart`] for the member in `slot`; the slot keeps its place.
    /// Fails with `LiveSlotOutOfRange` when the set has no such slot.
    pub fn restart(&self, slot: usize) -> Result<Generation, Error> {
        let members = self.snapshot();
        let member = members.get(slot).ok_or_else(|| {
            Error::live_slot_out_of_range(std::any::type_name::<T>(), slot, members.len())
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
