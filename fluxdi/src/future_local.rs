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
    value: Option<V>,
    inner: Pin<Box<F>>,
}

impl<V: Clone + Unpin + 'static, F: Future> WithLocal<V, F> {
    pub(crate) fn new(slot: &'static Slot<V>, value: V, inner: F) -> Self {
        Self {
            slot,
            value: Some(value),
            inner: Box::pin(inner),
        }
    }

    /// Clears `slot` while `inner` is polled.
    #[cfg_attr(not(feature = "live"), allow(dead_code))]
    pub(crate) fn cleared(slot: &'static Slot<V>, inner: F) -> Self {
        Self {
            slot,
            value: None,
            inner: Box::pin(inner),
        }
    }
}

impl<V: Clone + Unpin + 'static, F: Future> Future for WithLocal<V, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        let _restore = Restore::set(this.slot, this.value.clone());
        this.inner.as_mut().poll(cx)
    }
}

/// Sets `slot` to `value` while `f` runs on this thread.
#[cfg_attr(not(feature = "live"), allow(dead_code))]
pub(crate) fn set_while<V: 'static, R>(
    slot: &'static Slot<V>,
    value: V,
    f: impl FnOnce() -> R,
) -> R {
    let _restore = Restore::set(slot, Some(value));
    f()
}

struct Restore<V: 'static> {
    slot: &'static Slot<V>,
    previous: Option<V>,
}

impl<V: 'static> Restore<V> {
    fn set(slot: &'static Slot<V>, value: Option<V>) -> Self {
        Self {
            slot,
            previous: slot.with(|slot| slot.replace(value)),
        }
    }
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
