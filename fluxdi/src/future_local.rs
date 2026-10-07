use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread::LocalKey;

pub(crate) type Slot<V> = LocalKey<Cell<Option<V>>>;

/// Sets `slot` to `value` while `inner` is polled, so code that runs inside
/// `inner` sees the value on whatever thread polls it, and sibling futures in
/// the same task do not.
pub(crate) struct WithLocal<V: Clone + Unpin + 'static, F> {
    slot: &'static Slot<V>,
    value: V,
    inner: Pin<Box<F>>,
}

impl<V: Clone + Unpin + 'static, F: Future> WithLocal<V, F> {
    pub(crate) fn new(slot: &'static Slot<V>, value: V, inner: F) -> Self {
        Self {
            slot,
            value,
            inner: Box::pin(inner),
        }
    }
}

impl<V: Clone + Unpin + 'static, F: Future> Future for WithLocal<V, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        let previous = this
            .slot
            .with(|slot| slot.replace(Some(this.value.clone())));
        let _restore = Restore {
            slot: this.slot,
            previous,
        };
        this.inner.as_mut().poll(cx)
    }
}

struct Restore<V: 'static> {
    slot: &'static Slot<V>,
    previous: Option<V>,
}

impl<V: 'static> Drop for Restore<V> {
    fn drop(&mut self) {
        let previous = self.previous.take();
        self.slot.with(|slot| slot.set(previous));
    }
}

pub(crate) fn current<V: Clone + 'static>(slot: &'static Slot<V>) -> Option<V> {
    slot.with(|slot| {
        let value = slot.take();
        slot.set(value.clone());
        value
    })
}
