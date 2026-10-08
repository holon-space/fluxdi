//! Non-blocking paths, such as a UI render: code that may read a live
//! dependency's state but must never wait for a producer.

use std::cell::Cell;
use std::future::Future;

use crate::error::Error;
use crate::future_local::{self, WithLocal};

thread_local! {
    static NON_BLOCKING: Cell<Option<()>> = const { Cell::new(None) };
}

/// Polls `future` as a non-blocking path. Inside it, [`Live::ready`] and
/// [`LiveSet::complete`] fail with `LiveWaitOnRenderPath` whatever the state,
/// and so does a hard resolve that would wait for a live producer or for
/// another resolve's run; debug builds panic instead. `state()` and
/// `changed()` stay allowed. Tasks spawned from `future` are not on the path.
///
/// [`Live::ready`]: super::Live::ready
/// [`LiveSet::complete`]: super::LiveSet::complete
pub fn non_blocking_scope<F: Future>(future: F) -> impl Future<Output = F::Output> {
    WithLocal::new(&NON_BLOCKING, (), future)
}

/// [`non_blocking_scope`] for synchronous code, such as a UI thread's render:
/// it covers every future that `f` polls on the calling thread (a
/// `block_on`), not threads that `f` starts.
pub fn non_blocking_section<R>(f: impl FnOnce() -> R) -> R {
    future_local::set_while(&NON_BLOCKING, (), f)
}

/// Polls a live producer off the non-blocking path of whoever polls it, such
/// as a current-thread runtime driven by a `block_on` inside a section.
pub(crate) fn off_non_blocking_path<F: Future>(future: F) -> WithLocal<(), F> {
    WithLocal::cleared(&NON_BLOCKING, future)
}

/// Refuses a wait for `type_name` on a non-blocking path.
pub(crate) fn refuse_wait(type_name: &str) -> Result<(), Error> {
    if future_local::current(&NON_BLOCKING).is_none() {
        return Ok(());
    }
    let error = Error::live_wait_on_render_path(type_name);
    // fluxdi's own unit tests observe the error; every other debug build panics.
    debug_assert!(cfg!(test), "{error}");
    Err(error)
}
