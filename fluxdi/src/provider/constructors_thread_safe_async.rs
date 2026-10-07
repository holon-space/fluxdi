use super::*;

#[cfg(all(feature = "thread-safe", feature = "async-factory"))]
impl<T: ?Sized + 'static> Provider<T> {
    /// Creates a singleton provider whose factory resolves asynchronously.
    #[cfg(feature = "async-factory")]
    pub fn singleton_async<F, Fut>(factory: F) -> Provider<T>
    where
        F: Fn(Injector) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Shared<T>> + Send + 'static,
    {
        Self::try_async_with_scope::<_, _, std::convert::Infallible>(
            Scope::Module,
            move |injector| {
                let future = factory(injector);
                async move { Ok(future.await) }
            },
        )
    }

    /// Creates a transient provider whose factory resolves asynchronously.
    #[cfg(feature = "async-factory")]
    pub fn transient_async<F, Fut>(factory: F) -> Provider<T>
    where
        F: Fn(Injector) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Shared<T>> + Send + 'static,
    {
        Self::try_async_with_scope::<_, _, std::convert::Infallible>(
            Scope::Transient,
            move |injector| {
                let future = factory(injector);
                async move { Ok(future.await) }
            },
        )
    }

    /// Creates a root-scoped provider whose factory resolves asynchronously.
    #[cfg(feature = "async-factory")]
    pub fn root_async<F, Fut>(factory: F) -> Provider<T>
    where
        F: Fn(Injector) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Shared<T>> + Send + 'static,
    {
        Self::try_async_with_scope::<_, _, std::convert::Infallible>(Scope::Root, move |injector| {
            let future = factory(injector);
            async move { Ok(future.await) }
        })
    }

    /// Creates a scope-scoped provider whose factory resolves asynchronously.
    #[cfg(feature = "async-factory")]
    pub fn scoped_async<F, Fut>(factory: F) -> Provider<T>
    where
        F: Fn(Injector) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Shared<T>> + Send + 'static,
    {
        Self::try_async_with_scope::<_, _, std::convert::Infallible>(
            Scope::Scoped,
            move |injector| {
                let future = factory(injector);
                async move { Ok(future.await) }
            },
        )
    }

    /// Creates a singleton provider whose async factory may fail.
    ///
    /// An `Err` reaches the resolve call as [`ErrorKind::FactoryFailed`](crate::ErrorKind::FactoryFailed)
    /// and nothing is cached, so the next resolve runs the factory again.
    #[cfg(feature = "async-factory")]
    pub fn singleton_try_async<F, Fut, E>(factory: F) -> Provider<T>
    where
        F: Fn(Injector) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Shared<T>, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        Self::try_async_with_scope(Scope::Module, factory)
    }

    /// Creates a transient provider whose async factory may fail.
    ///
    /// An `Err` reaches the resolve call as [`ErrorKind::FactoryFailed`](crate::ErrorKind::FactoryFailed).
    #[cfg(feature = "async-factory")]
    pub fn transient_try_async<F, Fut, E>(factory: F) -> Provider<T>
    where
        F: Fn(Injector) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Shared<T>, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        Self::try_async_with_scope(Scope::Transient, factory)
    }

    /// Creates a root-scoped provider whose async factory may fail.
    ///
    /// An `Err` reaches the resolve call as [`ErrorKind::FactoryFailed`](crate::ErrorKind::FactoryFailed)
    /// and nothing is cached, so the next resolve runs the factory again.
    #[cfg(feature = "async-factory")]
    pub fn root_try_async<F, Fut, E>(factory: F) -> Provider<T>
    where
        F: Fn(Injector) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Shared<T>, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        Self::try_async_with_scope(Scope::Root, factory)
    }

    /// Creates a scope-scoped provider whose async factory may fail.
    ///
    /// An `Err` reaches the resolve call as [`ErrorKind::FactoryFailed`](crate::ErrorKind::FactoryFailed)
    /// and nothing is cached, so the next resolve runs the factory again.
    #[cfg(feature = "async-factory")]
    pub fn scoped_try_async<F, Fut, E>(factory: F) -> Provider<T>
    where
        F: Fn(Injector) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Shared<T>, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        Self::try_async_with_scope(Scope::Scoped, factory)
    }

    fn try_async_with_scope<F, Fut, E>(scope: Scope, factory: F) -> Provider<T>
    where
        F: Fn(Injector) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Shared<T>, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        #[cfg(feature = "tracing")]
        info!(
            type_name = std::any::type_name::<T>(),
            scope = %scope,
            threading = "thread-safe",
            factory_mode = "async",
            "Creating async provider"
        );

        Provider::<T> {
            scope,
            factory: Box::new(|_| {
                panic!(
                    "async provider cannot be used with try_resolve/resolve; use try_resolve_async/resolve_async"
                )
            }),
            async_factory: Some(Box::new(move |injector| {
                Box::pin({
                    let future = factory(injector);
                    async move {
                        #[cfg(feature = "tracing")]
                        debug!(
                            type_name = std::any::type_name::<T>(),
                            scope = %scope,
                            op = "provider_factory_call_async",
                            "Executing async factory"
                        );
                        future.await.map(Instance::new).map_err(|source| {
                            Error::factory_failed(std::any::type_name::<T>(), source.into())
                        })
                    }
                })
            })),
            limits: Limits::default(),
            dependency_hints: Vec::new(),
            limiter: None,
        }
    }
}
