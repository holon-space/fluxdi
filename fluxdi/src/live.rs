//! Live dependency edges.
//!
//! A hard edge (`resolve_async`) waits for the finished value. A live edge
//! ([`Injector::resolve_live`](crate::Injector::resolve_live),
//! [`Injector::resolve_all_live`](crate::Injector::resolve_all_live)) returns
//! a handle at once; the handle reports the producer's state as it changes.
//! The producer starts on the first live resolve of its binding, never at
//! container build, and every live or hard resolve shares it.
//!
//! A producer finishes for good: `Ready` and `Failed` are terminal.

use std::time::Duration;

use tokio::sync::watch;

use crate::error::Error;
use crate::injector::cells::{CellRegistry, RunRef};
use crate::runtime::Shared;

pub use live_envelope::{Completeness, Observed};

/// State of one live dependency.
pub type LiveState<T> = Observed<Shared<T>, Error>;

pub(crate) fn is_terminal<T: ?Sized>(state: &LiveState<T>) -> bool {
    matches!(state, LiveState::Ready(_) | LiveState::Failed(_))
}

/// Handle to one live dependency. Cloning shares the same producer.
pub struct Live<T: ?Sized> {
    rx: watch::Receiver<LiveState<T>>,
    producer: RunRef,
    /// Holds the cell's sender, so `rx` never sees the channel closed.
    registry: Shared<CellRegistry>,
}

impl<T: ?Sized> Clone for Live<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
            producer: self.producer,
            registry: self.registry.clone(),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> Live<T> {
    pub(crate) fn new(
        rx: watch::Receiver<LiveState<T>>,
        producer: RunRef,
        registry: Shared<CellRegistry>,
    ) -> Self {
        Self {
            rx,
            producer,
            registry,
        }
    }

    pub fn state(&self) -> LiveState<T> {
        self.rx.borrow().clone()
    }

    /// Waits for the next state after the last one this handle saw.
    pub async fn changed(&mut self) -> LiveState<T> {
        self.rx
            .changed()
            .await
            .expect("the registry keeps a live cell's sender");
        self.rx.borrow_and_update().clone()
    }

    /// The hard edge on a live dependency: waits for a terminal state.
    /// Fails at once with `CircularDependency` when the producer, directly
    /// or through other runs, already waits for the caller's run.
    pub async fn ready(&self) -> Result<Shared<T>, Error> {
        let _wait = self.registry.begin_wait(self.producer)?;
        self.terminal().await
    }

    async fn terminal(&self) -> Result<Shared<T>, Error> {
        let mut rx = self.rx.clone();
        let state = rx
            .wait_for(is_terminal)
            .await
            .expect("the registry keeps a live cell's sender")
            .clone();
        match state {
            LiveState::Ready(value) => Ok(value),
            LiveState::Failed(error) => Err(error),
            LiveState::Pending | LiveState::Partial(_) => {
                unreachable!("wait_for returned a non-terminal state")
            }
        }
    }
}

/// Handle to a set of live dependencies, one slot per registered member in
/// registration order.
pub struct LiveSet<T: ?Sized> {
    members: Vec<Live<T>>,
}

impl<T: ?Sized> Clone for LiveSet<T> {
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

    pub fn members(&self) -> Vec<LiveState<T>> {
        self.members.iter().map(Live::state).collect()
    }

    pub fn ready_members(&self) -> Vec<Shared<T>> {
        self.members
            .iter()
            .filter_map(|member| match member.state() {
                LiveState::Ready(value) => Some(value),
                _ => None,
            })
            .collect()
    }

    /// Waits for the next change of any member after the last states this
    /// handle saw, and returns every member's state.
    pub async fn changed(&mut self) -> Vec<LiveState<T>> {
        if self.members.is_empty() {
            return std::future::pending().await;
        }
        let changes = self
            .members
            .iter_mut()
            .map(|member| Box::pin(member.rx.changed()));
        futures::future::select_all(changes)
            .await
            .0
            .expect("the registry keeps a live cell's sender");
        self.members
            .iter_mut()
            .map(|member| member.rx.borrow_and_update().clone())
            .collect()
    }

    /// Waits until every member is terminal. Fails with the first failed
    /// member's error, in registration order.
    pub async fn complete(&self) -> Result<Vec<Shared<T>>, Error> {
        let mut outcomes = Vec::with_capacity(self.members.len());
        for member in &self.members {
            outcomes.push(member.terminal().await);
        }
        outcomes.into_iter().collect()
    }
}

#[derive(Clone)]
#[cfg_attr(feature = "debug", derive(Debug))]
pub enum LiveOutcome {
    Running,
    Ready,
    Failed(Error),
}

/// One producer's outcome and timing.
#[derive(Clone)]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct LiveTiming {
    pub type_name: &'static str,
    /// The slot in the set this producer fills, for a set member.
    pub member: Option<usize>,
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
