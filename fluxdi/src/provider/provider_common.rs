use super::*;

impl<T: ?Sized + 'static> Provider<T> {
    /// Applies resource limits to this provider.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self.limiter = Limiter::from_limits(limits);
        self
    }

    pub(crate) fn acquire_creation_permit(
        &self,
        type_name: &str,
    ) -> Result<Option<CreationPermit>, Error> {
        if let Some(limiter) = &self.limiter {
            return Limiter::try_acquire(limiter, type_name).map(Some);
        }

        Ok(None)
    }

    #[cfg(feature = "async-factory")]
    pub(crate) async fn acquire_creation_permit_async(
        &self,
        type_name: &str,
    ) -> Result<Option<CreationPermit>, Error> {
        if let Some(limiter) = &self.limiter {
            #[cfg(all(feature = "thread-safe", feature = "resource-limit-async"))]
            {
                return Limiter::try_acquire_async(limiter, type_name)
                    .await
                    .map(Some);
            }

            #[cfg(not(all(feature = "thread-safe", feature = "resource-limit-async")))]
            {
                return Limiter::try_acquire(limiter, type_name).map(Some);
            }
        }

        Ok(None)
    }

    /// Declares that this provider depends on a single service `D`.
    ///
    /// This hint is used by graph tooling (`dependency_graph`, `validate_graph`)
    /// and does not change runtime resolution behavior.
    pub fn with_dependency<D>(mut self) -> Self
    where
        D: ?Sized + 'static,
    {
        self.dependency_hints.push(DependencyHint::one::<D>());
        self
    }

    /// Declares that this provider depends on a named service `D`.
    ///
    /// This hint is used by graph tooling (`dependency_graph`, `validate_graph`)
    /// and does not change runtime resolution behavior.
    pub fn with_named_dependency<D>(mut self, name: impl Into<String>) -> Self
    where
        D: ?Sized + 'static,
    {
        self.dependency_hints
            .push(DependencyHint::named::<D>(name.into()));
        self
    }

    /// Declares that this provider depends on all set bindings of service `D`.
    ///
    /// This hint is used by graph tooling (`dependency_graph`, `validate_graph`)
    /// and does not change runtime resolution behavior.
    pub fn with_set_dependency<D>(mut self) -> Self
    where
        D: ?Sized + 'static,
    {
        self.dependency_hints.push(DependencyHint::all::<D>());
        self
    }

    /// Declares that this provider depends on a dynamic provider by name.
    ///
    /// This hint bridges typed providers into the dynamic dependency graph,
    /// enabling correct ordering in `resolve_all_eager()` and proper
    /// validation in `validate_graph()`.
    pub fn with_dynamic_dependency(mut self, name: impl Into<String>) -> Self {
        self.dependency_hints
            .push(DependencyHint::dynamic(name.into()));
        self
    }

    /// Wraps resolved instances with a decorator (e.g. logging, caching).
    ///
    /// The decorator receives the base instance and returns a wrapped instance.
    /// Order is deterministic: base factory runs first, then decorator. It
    /// wraps the value of every factory form: sync, async, and live, where it
    /// also wraps each published partial.
    ///
    /// For multiple decorators, chain: `provider.with_decorator(d1).with_decorator(d2)`.
    #[cfg(not(feature = "thread-safe"))]
    pub fn with_decorator<F>(self, decorator: F) -> Self
    where
        F: Fn(Shared<T>) -> Shared<T> + 'static,
    {
        let decorator = Shared::new(decorator);
        let Self {
            scope,
            factory,
            #[cfg(feature = "async-factory")]
            async_factory,
            limits,
            dependency_hints,
            limiter,
        } = self;
        Self {
            scope,
            factory: {
                let decorator = decorator.clone();
                Box::new(move |inj| Instance::new((*decorator)(factory(inj).value())))
            },
            #[cfg(feature = "async-factory")]
            async_factory: async_factory.map(|async_factory| {
                Box::new(move |inj| {
                    let run = async_factory(inj);
                    let decorator = decorator.clone();
                    Box::pin(async move {
                        run.await
                            .map(|instance| Instance::new((*decorator)(instance.value())))
                    })
                        as Pin<Box<dyn Future<Output = Result<Instance<T>, Error>>>>
                }) as AsyncFactory<T>
            }),
            limits,
            dependency_hints,
            limiter,
        }
    }

    /// Wraps resolved instances with a decorator (thread-safe); see the
    /// single-threaded variant.
    #[cfg(feature = "thread-safe")]
    pub fn with_decorator<F>(self, decorator: F) -> Self
    where
        F: Fn(Shared<T>) -> Shared<T> + Send + Sync + 'static,
    {
        let decorator = Shared::new(decorator);
        let Self {
            scope,
            factory,
            #[cfg(feature = "async-factory")]
            async_factory,
            #[cfg(feature = "live")]
            live_factory,
            limits,
            dependency_hints,
            limiter,
        } = self;
        Self {
            scope,
            factory: {
                let decorator = decorator.clone();
                Box::new(move |inj| Instance::new((*decorator)(factory(inj).value())))
            },
            #[cfg(feature = "async-factory")]
            async_factory: async_factory.map(|async_factory| {
                let decorator = decorator.clone();
                Box::new(move |inj| {
                    let run = async_factory(inj);
                    let decorator = decorator.clone();
                    Box::pin(async move {
                        run.await
                            .map(|instance| Instance::new((*decorator)(instance.value())))
                    }) as AsyncRun<T>
                }) as AsyncFactory<T>
            }),
            #[cfg(feature = "live")]
            live_factory: live_factory.map(|live_factory| {
                Box::new(move |inj, publisher: crate::live::LivePublisher<T>| {
                    let run = live_factory(inj, publisher.decorated(decorator.clone()));
                    let decorator = decorator.clone();
                    Box::pin(async move {
                        run.await
                            .map(|instance| Instance::new((*decorator)(instance.value())))
                    }) as AsyncRun<T>
                }) as LiveFactory<T>
            }),
            limits,
            dependency_hints,
            limiter,
        }
    }
}
