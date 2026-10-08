//! The two ways to read a live cell: observe its state, or await the end of
//! one generation through a recorded wait.

use tokio::sync::watch;

use super::{CellState, LiveState, is_terminal};
use crate::error::Error;
use crate::injector::cells::WaitEdge;
use crate::runtime::Shared;

/// Observes a live cell; it cannot wait for a producer to end.
pub(crate) struct CellReceiver<T: ?Sized>(watch::Receiver<CellState<T>>);

impl<T: ?Sized> Clone for CellReceiver<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: ?Sized> CellReceiver<T> {
    pub(crate) fn new(rx: watch::Receiver<CellState<T>>) -> Self {
        Self(rx)
    }

    pub(crate) fn borrow(&self) -> watch::Ref<'_, CellState<T>> {
        self.0.borrow()
    }

    pub(crate) fn borrow_and_update(&mut self) -> watch::Ref<'_, CellState<T>> {
        self.0.borrow_and_update()
    }

    pub(crate) async fn changed(&mut self) {
        self.0
            .changed()
            .await
            .expect("the registry keeps a live cell's sender");
    }
}

/// The terminal state of one generation, set once by its producer.
pub(crate) struct RunEnd<T: ?Sized>(Shared<watch::Sender<Option<LiveState<T>>>>);

impl<T: ?Sized> Clone for RunEnd<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: ?Sized> RunEnd<T> {
    /// The end of a generation whose state is `state`.
    pub(crate) fn new(state: &LiveState<T>) -> Self {
        let end = is_terminal(state).then(|| state.clone());
        Self(Shared::new(watch::channel(end).0))
    }

    pub(crate) fn publish(&self, state: &LiveState<T>) {
        assert!(is_terminal(state), "a generation ends in a terminal state");
        self.0.send_modify(|end| {
            assert!(end.is_none(), "a generation ends once");
            *end = Some(state.clone());
        });
    }

    pub(crate) async fn wait(&self, edge: WaitEdge<'_>) -> Result<LiveState<T>, Error> {
        let mut rx = self.0.subscribe();
        let end = edge
            .wait(Box::pin(rx.wait_for(Option::is_some)))
            .await?
            .expect("a run end holds its own sender")
            .clone();
        Ok(end.expect("wait_for returns a set end"))
    }
}
