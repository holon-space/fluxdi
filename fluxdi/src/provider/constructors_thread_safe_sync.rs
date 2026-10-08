use super::*;

#[cfg(feature = "thread-safe")]
impl<T: ?Sized + 'static> Provider<T> {
    /// Creates a singleton provider with module scope (thread-safe).
    ///
    /// A singleton provider creates **one instance per injector module**.
    /// Once created, the same instance is returned on subsequent resolutions
    /// within the same module. The instance can be safely shared across threads.
    ///
    /// # Type Parameters
    ///
    /// - `F`: Factory function type that takes an [`Injector`] reference and returns `Shared<T>`.
    ///   Must be `Send + Sync` for thread safety.
    ///
    /// # Arguments
    ///
    /// - `factory`: A closure that creates the instance when first requested.
    ///   The closure must be `Send + Sync`.
    ///
    /// # Thread Safety
    ///
    /// The factory function must be `Send + Sync` because it may be called
    /// from any thread. The returned `Shared<T>` (which is `Arc<T>` in thread-safe mode)
    /// ensures safe concurrent access to the instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxdi::{Provider, Shared};
    ///
    /// #[derive(Debug)]
    /// struct Database {
    ///     connection_count: std::sync::atomic::AtomicUsize,
    /// }
    ///
    /// let provider = Provider::singleton(|_injector| {
    ///     Shared::new(Database {
    ///         connection_count: std::sync::atomic::AtomicUsize::new(0),
    ///     })
    /// });
    /// ```
    ///
    /// With trait objects:
    ///
    /// ```
    /// use fluxdi::{Provider, Shared};
    /// use std::sync::Arc;
    ///
    /// trait Cache: Send + Sync {}
    /// struct MemoryCache;
    /// impl Cache for MemoryCache {}
    ///
    /// let provider = Provider::<dyn Cache>::singleton(|_injector| {
    ///     Arc::new(MemoryCache) as Arc<dyn Cache>
    /// });
    /// ```
    pub fn singleton<F>(factory: F) -> Provider<T>
    where
        F: Fn(&Injector) -> Shared<T> + Send + Sync + 'static,
    {
        #[cfg(feature = "tracing")]
        info!(
            type_name = std::any::type_name::<T>(),
            scope = %Scope::Module,
            threading = "thread-safe",
            factory_mode = "sync",
            "Creating singleton provider"
        );

        Provider::<T> {
            scope: Scope::Module,
            factory: Box::new(move |injector| {
                #[cfg(feature = "tracing")]
                debug!(
                    type_name = std::any::type_name::<T>(),
                    scope = %Scope::Module,
                    op = "provider_factory_call",
                    "Executing singleton factory"
                );

                Instance::new(factory(injector))
            }),
            #[cfg(feature = "async-factory")]
            async_factory: None,
            #[cfg(feature = "live")]
            live_factory: None,
            limits: Limits::default(),
            dependency_hints: Vec::new(),
            limiter: None,
        }
    }

    /// Creates a scope-scoped provider (thread-safe).
    ///
    /// A scoped provider creates **one instance per runtime scope**
    /// (see `Injector::create_scope()`).
    pub fn scoped<F>(factory: F) -> Provider<T>
    where
        F: Fn(&Injector) -> Shared<T> + Send + Sync + 'static,
    {
        #[cfg(feature = "tracing")]
        info!(
            type_name = std::any::type_name::<T>(),
            scope = %Scope::Scoped,
            threading = "thread-safe",
            factory_mode = "sync",
            "Creating scoped provider"
        );

        Provider::<T> {
            scope: Scope::Scoped,
            factory: Box::new(move |injector| Instance::new(factory(injector))),
            #[cfg(feature = "async-factory")]
            async_factory: None,
            #[cfg(feature = "live")]
            live_factory: None,
            limits: Limits::default(),
            dependency_hints: Vec::new(),
            limiter: None,
        }
    }

    /// Creates a transient provider (thread-safe).
    ///
    /// A transient provider creates a **new instance on every resolution**.
    /// No caching or instance reuse occurs. Each instance can be safely used
    /// across threads.
    ///
    /// # Type Parameters
    ///
    /// - `F`: Factory function type that takes an [`Injector`] reference and returns `Shared<T>`.
    ///   Must be `Send + Sync` for thread safety.
    ///
    /// # Arguments
    ///
    /// - `factory`: A closure that creates a new instance on each invocation.
    ///   The closure must be `Send + Sync`.
    ///
    /// # Use Cases
    ///
    /// - Per-request services in web applications
    /// - Task-specific handlers
    /// - Stateful operations that should not be shared
    ///
    /// # Thread Safety
    ///
    /// While each resolution creates a new instance, the factory itself must
    /// be thread-safe (`Send + Sync`) as it may be called from multiple threads.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxdi::{Provider, Shared};
    /// use std::sync::atomic::{AtomicU64, Ordering};
    ///
    /// static COUNTER: AtomicU64 = AtomicU64::new(0);
    ///
    /// struct RequestHandler {
    ///     id: u64,
    /// }
    ///
    /// let provider = Provider::transient(|_injector| {
    ///     Shared::new(RequestHandler {
    ///         id: COUNTER.fetch_add(1, Ordering::SeqCst),
    ///     })
    /// });
    /// ```
    pub fn transient<F>(factory: F) -> Provider<T>
    where
        F: Fn(&Injector) -> Shared<T> + Send + Sync + 'static,
    {
        #[cfg(feature = "tracing")]
        info!(
            type_name = std::any::type_name::<T>(),
            scope = %Scope::Transient,
            threading = "thread-safe",
            factory_mode = "sync",
            "Creating transient provider"
        );

        Provider::<T> {
            scope: Scope::Transient,
            factory: Box::new(move |injector| {
                #[cfg(feature = "tracing")]
                debug!(
                    type_name = std::any::type_name::<T>(),
                    scope = %Scope::Transient,
                    op = "provider_factory_call",
                    "Executing transient factory"
                );

                Instance::new(factory(injector))
            }),
            #[cfg(feature = "async-factory")]
            async_factory: None,
            #[cfg(feature = "live")]
            live_factory: None,
            limits: Limits::default(),
            dependency_hints: Vec::new(),
            limiter: None,
        }
    }

    /// Creates a root-scoped provider (thread-safe).
    ///
    /// A root provider creates **one instance per root injector** (application-wide).
    /// This is the highest level of singleton, shared across all child injectors
    /// and safe to access from any thread.
    ///
    /// # Type Parameters
    ///
    /// - `F`: Factory function type that takes an [`Injector`] reference and returns `Shared<T>`.
    ///   Must be `Send + Sync` for thread safety.
    ///
    /// # Arguments
    ///
    /// - `factory`: A closure that creates the instance when first requested.
    ///   The closure must be `Send + Sync`.
    ///
    /// # Use Cases
    ///
    /// - Application-wide configuration
    /// - Logging infrastructure
    /// - Thread-safe connection pools
    /// - Global metrics collectors
    /// - Shared caches
    ///
    /// # Thread Safety
    ///
    /// The root-scoped instance is shared across all threads and modules.
    /// Both the factory and the instance must be thread-safe.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxdi::{Provider, Shared};
    /// use std::sync::RwLock;
    ///
    /// struct GlobalConfig {
    ///     settings: RwLock<std::collections::HashMap<String, String>>,
    /// }
    ///
    /// let provider = Provider::root(|_injector| {
    ///     Shared::new(GlobalConfig {
    ///         settings: RwLock::new(std::collections::HashMap::new()),
    ///     })
    /// });
    /// ```
    pub fn root<F>(factory: F) -> Provider<T>
    where
        F: Fn(&Injector) -> Shared<T> + Send + Sync + 'static,
    {
        #[cfg(feature = "tracing")]
        info!(
            type_name = std::any::type_name::<T>(),
            scope = %Scope::Root,
            threading = "thread-safe",
            factory_mode = "sync",
            "Creating root provider"
        );

        Provider::<T> {
            scope: Scope::Root,
            factory: Box::new(move |injector| {
                #[cfg(feature = "tracing")]
                debug!(
                    type_name = std::any::type_name::<T>(),
                    scope = %Scope::Root,
                    op = "provider_factory_call",
                    "Executing root factory"
                );

                Instance::new(factory(injector))
            }),
            #[cfg(feature = "async-factory")]
            async_factory: None,
            #[cfg(feature = "live")]
            live_factory: None,
            limits: Limits::default(),
            dependency_hints: Vec::new(),
            limiter: None,
        }
    }

    /// Creates a singleton provider with resource limits.
    pub fn singleton_with_limits<F>(limits: Limits, factory: F) -> Provider<T>
    where
        F: Fn(&Injector) -> Shared<T> + Send + Sync + 'static,
    {
        Self::singleton(factory).with_limits(limits)
    }

    /// Creates a transient provider with resource limits.
    pub fn transient_with_limits<F>(limits: Limits, factory: F) -> Provider<T>
    where
        F: Fn(&Injector) -> Shared<T> + Send + Sync + 'static,
    {
        Self::transient(factory).with_limits(limits)
    }

    /// Creates a root provider with resource limits.
    pub fn root_with_limits<F>(limits: Limits, factory: F) -> Provider<T>
    where
        F: Fn(&Injector) -> Shared<T> + Send + Sync + 'static,
    {
        Self::root(factory).with_limits(limits)
    }

    /// Creates a scoped provider with resource limits.
    pub fn scoped_with_limits<F>(limits: Limits, factory: F) -> Provider<T>
    where
        F: Fn(&Injector) -> Shared<T> + Send + Sync + 'static,
    {
        Self::scoped(factory).with_limits(limits)
    }
}
