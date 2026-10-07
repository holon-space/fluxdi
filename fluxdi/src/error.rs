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
/// straightforward while still expressive enough for diagnostics.
#[derive(Clone, PartialEq, Debug)]
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
}

/// Container error structure.
///
/// `kind` enables programmatic handling, while `message` is human-readable.
#[derive(Clone, Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
    source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
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
        };

        #[cfg(feature = "tracing")]
        error!("{}", error);

        error
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
        Self::new(
            ErrorKind::ModuleLifecycleFailed,
            format!(
                "Module lifecycle failed: module={}, phase={}, details={}",
                module_name, phase, details
            ),
        )
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
    pub fn factory_failed(
        type_name: &str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    ) -> Self {
        let mut error = Self::new(
            ErrorKind::FactoryFailed,
            format!("Factory for type {} failed: {}", type_name, source),
        );
        error.source = Some(Arc::from(source));
        error
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
    /// Used when `on_start` fails for one or more modules during bootstrap,
    /// e.g. when using `parallel_start`.
    pub fn bootstrap_aggregate(errors: Vec<Error>) -> Self {
        if errors.len() == 1 {
            return errors.into_iter().next().unwrap();
        }
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
        Self::new(ErrorKind::ModuleLifecycleFailed, message)
    }

    /// Shutdown failed with multiple module errors (aggregated).
    ///
    /// Used when `on_stop` fails for one or more modules during shutdown.
    /// The returned error lists all failures for diagnostics.
    pub fn shutdown_aggregate(errors: Vec<Error>) -> Self {
        if errors.len() == 1 {
            return errors.into_iter().next().unwrap();
        }
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
        Self::new(ErrorKind::ModuleLifecycleFailed, message)
    }
}

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
