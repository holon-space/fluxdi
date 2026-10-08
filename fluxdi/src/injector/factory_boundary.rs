//! The panic boundary around every factory call: a panicking factory is a
//! failure of its binding (`FactoryFailed` with a [`FactoryPanic`] source),
//! not an unwind through the resolver. In a `panic = "abort"` build nothing
//! is caught and the process aborts at the panic.
//!
//! `AssertUnwindSafe` holds for the resolver's own state: no cache entry is
//! written before a factory returns, and the resolve-path guards, future
//! locals and creation permits that a panic unwinds through restore
//! themselves on drop. A lock that a panic poisons is never read through
//! the poison: every fluxdi lock site unwraps it and fails loudly.
//!
//! [`FactoryPanic`]: crate::FactoryPanic

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::error::Error;

/// Runs the synchronous `factory` of `type_name`.
pub(crate) fn run_sync<R>(type_name: &str, factory: impl FnOnce() -> R) -> Result<R, Error> {
    catch_unwind(AssertUnwindSafe(factory)).map_err(|panic| panicked(type_name, &*panic))
}

/// Drives the future of an async factory of `type_name`.
#[cfg(feature = "async-factory")]
pub(crate) async fn run_async<R>(
    type_name: &str,
    run: impl std::future::Future<Output = Result<R, Error>>,
) -> Result<R, Error> {
    use futures::FutureExt;
    AssertUnwindSafe(run)
        .catch_unwind()
        .await
        .unwrap_or_else(|panic| Err(panicked(type_name, &*panic)))
}

pub(crate) fn panicked(type_name: &str, panic: &(dyn Any + Send)) -> Error {
    Error::factory_panicked(type_name, &panic_message(panic))
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
