//! Error types for the FluxDI dependency injection container.
//!
//! This module defines a lightweight error model used across the container to
//! describe failures that can occur during service registration, resolution,
//! scope handling, and module initialization.
//!
//! # Design
//!
//! - `ErrorKind` captures the error category.
//! - `Error` stores the category and a human-readable message.
//!
//! The helpers in `Error` are provided to keep call sites concise and to
//! maintain consistent error messages.
//!
//! # Feature Flags
//!
//! - `tracing`: logs errors when they are created.
//! - `debug`: enables extra diagnostic formatting in `Display`.
//!
//! # Examples
//!
//! ```
//! use fluxdi::error::Error;
//!
//! let err = Error::service_not_provided("MyService");
//! assert!(err.message.contains("MyService"));
//! ```

use core::fmt;
use std::sync::Arc;

#[cfg(feature = "tracing")]
use tracing::error;

/// Error categories for the container.
///
/// These variants are intentionally coarse-grained to keep error handling
/// straightforward while still expressive enough for diagnostics. More kinds
/// may be added, so a match needs a wildcard arm:
///
/// ```compile_fail,E0004
/// use fluxdi::ErrorKind;
///
/// fn retry(kind: &ErrorKind) -> bool {
///     match kind {
///         ErrorKind::ServiceNotProvided => false,
///         ErrorKind::TypeMismatch => false,
///         ErrorKind::ProviderAlreadyRegistered => false,
///         ErrorKind::CircularDependency => false,
///         ErrorKind::AsyncFactoryRequiresAsyncResolve => false,
///         ErrorKind::ResourceLimitExceeded => true,
///         ErrorKind::ModuleLifecycleFailed => false,
///         ErrorKind::GraphValidationFailed => false,
///         ErrorKind::DynamicProviderNotFound => false,
///         ErrorKind::EagerResolutionFailed => false,
///         ErrorKind::FactoryFailed => true,
///         ErrorKind::LiveProducerFailed => true,
///         ErrorKind::LiveProducerCancelled => false,
///         ErrorKind::LiveRuntimeMissing => false,
///         ErrorKind::LiveRequiresCachedScope => false,
///         ErrorKind::LiveRestartWhileRunning => true,
///         ErrorKind::LiveInjectorDropped => false,
///         ErrorKind::LiveSlotOutOfRange => false,
///         ErrorKind::LiveWaitOnRenderPath => false,
///     }
/// }
/// ```
#[derive(Clone, PartialEq, Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Service provider not found for the requested type.
    ServiceNotProvided,
    /// Type mismatch during downcast or resolution.
    TypeMismatch,
    /// Factory closure already registered for this type.
    ProviderAlreadyRegistered,
    /// Circular dependency detected in resolution chain.
    CircularDependency,
    /// Async provider was resolved through a synchronous resolve method.
    AsyncFactoryRequiresAsyncResolve,
    /// Service creation was denied by configured resource limits.
    ResourceLimitExceeded,
    /// Module lifecycle hook failed.
    ModuleLifecycleFailed,
    /// Dependency graph validation failed.
    GraphValidationFailed,
    /// Dynamic provider not found by name.
    DynamicProviderNotFound,
    /// One or more providers failed during eager resolution.
    EagerResolutionFailed,
    /// A fallible async factory returned an error; it is the [`Error`]'s `source()`.
    FactoryFailed,
    /// A live dependency's producer failed or panicked.
    LiveProducerFailed,
    /// A live dependency's producer was cancelled before it finished.
    LiveProducerCancelled,
    /// A live resolve found no tokio runtime: none was given to the
    /// injector, and the calling thread has none.
    LiveRuntimeMissing,
    /// A transient provider was resolved live; a live cell needs a cached value.
    LiveRequiresCachedScope,
    /// A live dependency was restarted while its current generation still runs.
    LiveRestartWhileRunning,
    /// A live dependency was restarted after every clone of its injector
    /// was dropped.
    LiveInjectorDropped,
    /// A live set was asked for a slot it does not have.
    LiveSlotOutOfRange,
    /// Code marked as non-blocking (`live::non_blocking_scope`,
    /// `live::non_blocking_section`) waited for a live producer or for
    /// another resolve's run. A factory that fails with this refusal keeps
    /// this kind, not `FactoryFailed`.
    LiveWaitOnRenderPath,
}

/// Container error structure.
///
/// `kind` enables programmatic handling, while `message` is human-readable.
#[derive(Clone, Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
    source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
    module: Option<String>,
}

impl Error {
    /// Creates a new error with the given kind and message.
    ///
    /// If the `tracing` feature is enabled, the error is automatically logged.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        let error = Self {
            kind: kind.clone(),
            message: message.into(),
            source: None,
            module: None,
        };

        #[cfg(feature = "tracing")]
        error!("{}", error);

        error
    }

    /// The module whose lifecycle hook failed, for a `ModuleLifecycleFailed`
    /// error; for an aggregate, the module of its first failure.
    pub fn module_name(&self) -> Option<&str> {
        self.module.as_deref()
    }

    /// Service provider not found for the requested type.
    pub fn service_not_provided(type_name: &str) -> Self {
        Self::new(
            ErrorKind::ServiceNotProvided,
            format!(
                "No provider registered for type: {}. Register it in Module::configure(...) with injector.provide::<T>(Provider::...).",
                type_name
            ),
        )
    }

    /// Named service provider not found for the requested type.
    pub fn service_not_provided_named(type_name: &str, name: &str) -> Self {
        Self::new(
            ErrorKind::ServiceNotProvided,
            format!(
                "No provider registered for type: {} with name: {}. Register it with provide_named/try_provide_named.",
                type_name, name
            ),
        )
    }

    /// Override requested for a type that has no registered provider.
    pub fn service_not_provided_for_override(type_name: &str) -> Self {
        Self::new(
            ErrorKind::ServiceNotProvided,
            format!(
                "Cannot override provider for type: {} because no provider is registered. Register one first with provide/try_provide.",
                type_name
            ),
        )
    }

    /// Type mismatch during downcast or factory execution.
    ///
    /// This covers both immediate type mismatches and cached instance type mismatches.
    pub fn type_mismatch(type_name: &str) -> Self {
        Self::new(
            ErrorKind::TypeMismatch,
            format!("Type mismatch when resolving: {}", type_name),
        )
    }

    /// Provider already registered for this type.
    ///
    /// Attempting to register a provider for a type that already has one.
    pub fn provider_already_registered(type_name: &str, scope: &str) -> Self {
        Self::new(
            ErrorKind::ProviderAlreadyRegistered,
            format!(
                "Provider ({} scope) already registered for type: {}. Use override_provider/try_override_provider to replace it.",
                scope, type_name
            ),
        )
    }

    /// Named provider already registered for this type.
    pub fn provider_already_registered_named(type_name: &str, name: &str, scope: &str) -> Self {
        Self::new(
            ErrorKind::ProviderAlreadyRegistered,
            format!(
                "Provider ({} scope) already registered for type: {} with name: {}.",
                scope, type_name, name
            ),
        )
    }

    /// Circular dependency detected in resolution chain.
    pub fn circular_dependency(dependency_chain: &[&str]) -> Self {
        Self::new(
            ErrorKind::CircularDependency,
            format!(
                "Circular dependency detected: {}. Break the cycle by introducing a trait boundary, lazy lookup, or refactoring the dependency direction.",
                dependency_chain.join(" -> ")
            ),
        )
    }

    /// Async provider resolved through a synchronous resolve method.
    pub fn async_factory_requires_async_resolve(type_name: &str) -> Self {
        Self::new(
            ErrorKind::AsyncFactoryRequiresAsyncResolve,
            format!(
                "Type {} is registered with an async provider; use try_resolve_async/resolve_async",
                type_name
            ),
        )
    }

    /// Service creation was denied by configured resource limits.
    pub fn resource_limit_exceeded(type_name: &str, details: &str) -> Self {
        Self::new(
            ErrorKind::ResourceLimitExceeded,
            format!(
                "Resource limit exceeded while creating type {}: {}",
                type_name, details
            ),
        )
    }

    /// Module lifecycle hook failed.
    pub fn module_lifecycle_failed(module_name: &str, phase: &str, details: &str) -> Self {
        let mut error = Self::new(
            ErrorKind::ModuleLifecycleFailed,
            format!(
                "Module lifecycle failed: module={}, phase={}, details={}",
                module_name, phase, details
            ),
        );
        error.module = Some(module_name.to_string());
        error
    }

    /// A module's lifecycle `phase` failed with `source`, which is the
    /// [`Error`]'s `source()`; its text is the message's `details`.
    pub fn module_lifecycle_failed_with_source(
        module_name: &str,
        phase: &str,
        source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        let source = source.into();
        let mut error = Self::module_lifecycle_failed(module_name, phase, &source.to_string());
        error.source = Some(Arc::from(source));
        error
    }

    /// Dynamic provider not found by name.
    pub fn dynamic_provider_not_found(name: &str) -> Self {
        Self::new(
            ErrorKind::DynamicProviderNotFound,
            format!(
                "No dynamic provider registered with name: {}. Register it with provide_dynamic/try_provide_dynamic.",
                name
            ),
        )
    }

    /// One or more providers failed during eager resolution; `source` is the
    /// [`Error`]'s `source()`.
    pub fn eager_resolution_failed(provider: &str, source: &Error) -> Self {
        let mut error = Self::new(
            ErrorKind::EagerResolutionFailed,
            format!(
                "Eager resolution failed for provider {}: {}",
                provider, source.message
            ),
        );
        error.source = Some(Arc::new(source.clone()));
        error
    }

    /// Eager resolution wave `wave` failed; the first failure is the
    /// [`Error`]'s `source()`, every failure is in the message.
    #[cfg(feature = "eager-resolution")]
    pub(crate) fn eager_wave_failed(wave: usize, failures: &[&Error]) -> Self {
        let first = *failures
            .first()
            .expect("a failed wave has at least one failure");
        let mut error = Self::new(
            ErrorKind::EagerResolutionFailed,
            format!(
                "Eager resolution wave {} failed: {}",
                wave,
                failures
                    .iter()
                    .map(|failure| failure.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        );
        error.source = Some(Arc::new(first.clone()));
        error
    }

    /// A fallible async factory returned `source` instead of an instance.
    /// When `source` or an error in its `source()` chain is a
    /// `LiveWaitOnRenderPath` refusal, the kind stays `LiveWaitOnRenderPath`.
    pub fn factory_failed(
        type_name: &str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    ) -> Self {
        let refused_on_render_path =
            std::iter::successors(Some(&*source as &(dyn std::error::Error + 'static)), |e| {
                e.source()
            })
            .any(|e| {
                e.downcast_ref::<Error>()
                    .is_some_and(|e| e.kind == ErrorKind::LiveWaitOnRenderPath)
            });
        let kind = if refused_on_render_path {
            ErrorKind::LiveWaitOnRenderPath
        } else {
            ErrorKind::FactoryFailed
        };
        let mut error = Self::new(
            kind,
            format!("Factory for type {} failed: {}", type_name, source),
        );
        error.source = Some(Arc::from(source));
        error
    }

    /// A live dependency's producer failed with `source`.
    pub fn live_producer_failed(type_name: &str, source: &Error) -> Self {
        let mut error = Self::new(
            ErrorKind::LiveProducerFailed,
            format!("Live producer for {} failed: {}", type_name, source.message),
        );
        error.source = Some(Arc::new(source.clone()));
        error
    }

    /// The factory of `type_name` panicked with `panic`, which is the
    /// [`Error`]'s `source()` as a [`FactoryPanic`].
    pub fn factory_panicked(type_name: &str, panic: &str) -> Self {
        let mut error = Self::new(
            ErrorKind::FactoryFailed,
            format!("Factory for type {} panicked: {}", type_name, panic),
        );
        error.source = Some(Arc::new(FactoryPanic {
            message: panic.to_string(),
        }));
        error
    }

    /// A live dependency's producer was cancelled before it finished.
    pub fn live_producer_cancelled(type_name: &str) -> Self {
        Self::new(
            ErrorKind::LiveProducerCancelled,
            format!(
                "Live producer for {} was cancelled before it finished",
                type_name
            ),
        )
    }

    /// `shutdown_live` was called, so no producer of `type_name` starts.
    pub fn live_shut_down(type_name: &str) -> Self {
        Self::new(
            ErrorKind::LiveProducerCancelled,
            format!(
                "Live producer for {} cannot start: shutdown_live ended live production",
                type_name
            ),
        )
    }

    /// `type_name` was restarted while its generation `generation` runs.
    pub fn live_restart_while_running(type_name: &str, generation: u64) -> Self {
        Self::new(
            ErrorKind::LiveRestartWhileRunning,
            format!(
                "Cannot restart {}: its generation {} is still running",
                type_name, generation
            ),
        )
    }

    /// `type_name` was restarted after its injector was dropped.
    pub fn live_injector_dropped(type_name: &str) -> Self {
        Self::new(
            ErrorKind::LiveInjectorDropped,
            format!(
                "Cannot restart {}: every clone of its injector was dropped",
                type_name
            ),
        )
    }

    /// Slot `slot` of a live set of `type_name` with `len` members.
    pub fn live_slot_out_of_range(type_name: &str, slot: usize, len: usize) -> Self {
        Self::new(
            ErrorKind::LiveSlotOutOfRange,
            format!(
                "Live set of {} has no slot {}: it has {} members",
                type_name, slot, len
            ),
        )
    }

    /// A wait for `type_name` on a non-blocking path.
    pub fn live_wait_on_render_path(type_name: &str) -> Self {
        Self::new(
            ErrorKind::LiveWaitOnRenderPath,
            format!(
                "Waited for {} on a non-blocking path (non_blocking_scope or \
                 non_blocking_section); read state() or follow changed() there instead",
                type_name
            ),
        )
    }

    /// No tokio runtime to run a live producer of `type_name` on.
    pub fn live_runtime_missing(type_name: &str) -> Self {
        Self::new(
            ErrorKind::LiveRuntimeMissing,
            format!(
                "Cannot resolve {} live: the injector holds no tokio runtime handle \
                 (Injector::root_with_runtime) and the calling thread has no runtime",
                type_name
            ),
        )
    }

    /// A transient provider was resolved live.
    pub fn live_requires_cached_scope(type_name: &str) -> Self {
        Self::new(
            ErrorKind::LiveRequiresCachedScope,
            format!(
                "{} is transient; a live resolve needs a cached scope (singleton, root, module or scoped)",
                type_name
            ),
        )
    }

    /// Dependency graph validation failed.
    pub fn graph_validation_failed(details: &str) -> Self {
        Self::new(
            ErrorKind::GraphValidationFailed,
            format!(
                "Dependency graph validation failed: {}. Check dependency_graph()/validate_graph() output for details.",
                details
            ),
        )
    }

    /// Bootstrap failed with multiple module errors (aggregated).
    ///
    /// Used when `configure` or `on_start` fails during bootstrap (several
    /// `on_start` failures with `parallel_start`), followed by the `on_stop`
    /// failures of the rollback. Every failure is in the message; the first one is the
    /// error's `source()`.
    pub fn bootstrap_aggregate(errors: Vec<Error>) -> Self {
        if errors.len() == 1 {
            return errors.into_iter().next().unwrap();
        }
        let first = errors[0].clone();
        let message = format!(
            "Bootstrap failed: {} module(s) reported errors:\n{}",
            errors.len(),
            errors
                .iter()
                .enumerate()
                .map(|(i, e)| format!("  {}) {}", i + 1, e.message))
                .collect::<Vec<_>>()
                .join("\n")
        );
        let mut error = Self::new(ErrorKind::ModuleLifecycleFailed, message);
        error.module = first.module.clone();
        error.source = Some(Arc::new(first));
        error
    }

    /// Shutdown failed with multiple module errors (aggregated).
    ///
    /// Used when `on_stop` fails for one or more modules during shutdown.
    /// The returned error lists all failures for diagnostics; the first one
    /// is its `source()`.
    pub fn shutdown_aggregate(errors: Vec<Error>) -> Self {
        if errors.len() == 1 {
            return errors.into_iter().next().unwrap();
        }
        let first = errors[0].clone();
        let message = format!(
            "Shutdown failed: {} module(s) reported errors:\n{}",
            errors.len(),
            errors
                .iter()
                .enumerate()
                .map(|(i, e)| format!("  {}) {}", i + 1, e.message))
                .collect::<Vec<_>>()
                .join("\n")
        );
        let mut error = Self::new(ErrorKind::ModuleLifecycleFailed, message);
        error.module = first.module.clone();
        error.source = Some(Arc::new(first));
        error
    }
}

/// The panic of a factory, the `source()` of [`Error::factory_panicked`].
///
/// It holds no location: a caught panic payload carries none, only a
/// process-wide panic hook sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FactoryPanic {
    /// The panic payload when it is a string.
    pub message: String,
}

impl fmt::Display for FactoryPanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for FactoryPanic {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(feature = "debug")]
        {
            write!(f, "({:?}) - {}", self.kind, self.message)
        }
        #[cfg(not(feature = "debug"))]
        {
            write!(f, "{}", self.message)
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests;
