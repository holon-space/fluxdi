//! Provider module for dependency injection.
//!
//! This module defines the [`Provider`] struct, which encapsulates the logic for creating
//! instances of dependencies with different lifecycle scopes (singleton, transient, root).
//!
//! # Lifecycle Scopes
//!
//! - **Singleton (Module)**: One instance per injector module
//! - **Transient**: New instance on every resolution
//! - **Root**: One instance per root injector (application-wide)
//!
//! # Thread Safety
//!
//! The module supports two compilation modes via the `thread-safe` feature flag:
//!
//! - **With `thread-safe`**: Factories must be `Send + Sync`, allowing safe concurrent access
//! - **Without `thread-safe`**: Single-threaded mode with no thread safety overhead
//!
//! # Examples
//!
//! ```
//! use fluxdi::{Provider, Injector, Shared};
//!
//! // Concrete type - singleton
//! struct Database {
//!     url: String,
//! }
//!
//! let provider = Provider::singleton(|_| {
//!     Shared::new(Database {
//!         url: "postgresql://localhost".to_string(),
//!     })
//! });
//! ```
//!
//! For trait objects:
//!
//! ```
//! use fluxdi::{Provider, Shared};
//!
//! trait Logger {}
//! struct ConsoleLogger;
//! impl Logger for ConsoleLogger {}
//!
//! let provider = Provider::<dyn Logger>::singleton(|_| {
//!     Shared::new(ConsoleLogger) as Shared<dyn Logger>
//! });
//! ```

use crate::error::Error;
use crate::graph::DependencyHint;
use crate::injector::Injector;
use crate::instance::Instance;
use crate::runtime::Shared;
use crate::scope::Scope;

#[cfg(feature = "async-factory")]
use std::future::Future;
#[cfg(feature = "async-factory")]
use std::pin::Pin;
use std::time::Duration;

#[cfg(all(feature = "thread-safe", feature = "resource-limit-async"))]
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[cfg(feature = "tracing")]
use tracing::{debug, info};

#[cfg(all(feature = "async-factory", not(feature = "thread-safe")))]
type AsyncFactory<T> = Box<
    dyn Fn(Injector) -> Pin<Box<dyn Future<Output = Result<Instance<T>, Error>> + 'static>>
        + 'static,
>;

#[cfg(all(feature = "async-factory", feature = "thread-safe"))]
type AsyncFactory<T> = Box<
    dyn Fn(Injector) -> Pin<Box<dyn Future<Output = Result<Instance<T>, Error>> + Send + 'static>>
        + Send
        + Sync
        + 'static,
>;

#[cfg(all(not(feature = "thread-safe"), feature = "async-factory"))]
mod constructors_non_thread_safe_async;
#[cfg(not(feature = "thread-safe"))]
mod constructors_non_thread_safe_sync;
#[cfg(all(feature = "thread-safe", feature = "async-factory"))]
mod constructors_thread_safe_async;
#[cfg(feature = "thread-safe")]
mod constructors_thread_safe_sync;
mod limiter_acquire;
mod limiter_release;
mod limiter_types;
mod provider_common;
#[cfg(feature = "debug")]
mod provider_debug;
mod provider_type;

use limiter_types::{CreationPermit, Limiter};
pub use limiter_types::{Limits, Policy};
pub use provider_type::Provider;

#[cfg(test)]
mod tests;
