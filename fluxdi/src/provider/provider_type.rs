use super::*;

/// A provider encapsulates the factory logic for creating instances of type `T`.
///
/// The provider stores:
/// - The lifecycle [`Scope`] (singleton, transient, or root)
/// - A factory function that creates [`Instance<T>`] when invoked
///
/// # Type Parameters
///
/// - `T`: The type being provided. Can be `?Sized` to support trait objects.
///
/// # Thread Safety
///
/// When compiled with the `thread-safe` feature, the factory function must be
/// `Send + Sync` to allow safe sharing across threads.
///
/// # Examples
///
/// Creating a singleton provider:
///
/// ```
/// use fluxdi::{Provider, Shared};
///
/// struct Service {
///     name: String,
/// }
///
/// let provider = Provider::singleton(|_injector| {
///     Shared::new(Service {
///         name: "MyService".to_string(),
///     })
/// });
/// ```
pub struct Provider<T: ?Sized + 'static> {
    /// The lifecycle scope of this provider
    pub scope: Scope,

    /// The factory function that creates instances
    ///
    /// In single-threaded mode, the factory only needs to be `'static`.
    /// In thread-safe mode, the factory must also be `Send + Sync`.
    #[allow(clippy::type_complexity)]
    #[cfg(not(feature = "thread-safe"))]
    pub factory: Box<dyn Fn(&Injector) -> Instance<T> + 'static>,

    /// The factory function that creates instances (thread-safe variant)
    #[allow(clippy::type_complexity)]
    #[cfg(feature = "thread-safe")]
    pub factory: Box<dyn Fn(&Injector) -> Instance<T> + Send + Sync + 'static>,

    /// Optional async factory used by `Injector::try_resolve_async`.
    #[cfg(feature = "async-factory")]
    pub async_factory: Option<AsyncFactory<T>>,

    /// The factory a live producer runs with its cell's publisher; set only
    /// by the `*_live` constructors.
    #[cfg(feature = "live")]
    pub(crate) live_factory: Option<LiveFactory<T>>,

    /// Optional resource limits for this provider factory.
    pub limits: Limits,

    pub(crate) dependency_hints: Vec<DependencyHint>,

    pub(super) limiter: Option<Shared<Limiter>>,
}
