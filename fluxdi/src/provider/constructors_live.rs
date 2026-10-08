use super::*;
use crate::live::LivePublisher;

impl<T: ?Sized + Send + Sync + 'static> Provider<T> {
    /// Creates a singleton provider whose async factory may publish
    /// `Partial` values through its [`LivePublisher`] before it returns.
    ///
    /// Live observers see the partials; `Live::ready` and hard resolves get
    /// only the returned value. An `Err` reaches the resolve call as
    /// [`ErrorKind::FactoryFailed`](crate::ErrorKind::FactoryFailed) and
    /// nothing is cached.
    pub fn singleton_live<F, Fut, E>(factory: F) -> Provider<T>
    where
        F: Fn(Injector, LivePublisher<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Shared<T>, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        Self::live_with_scope(Scope::Module, factory)
    }

    /// [`Self::singleton_live`] with root scope.
    pub fn root_live<F, Fut, E>(factory: F) -> Provider<T>
    where
        F: Fn(Injector, LivePublisher<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Shared<T>, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        Self::live_with_scope(Scope::Root, factory)
    }

    /// [`Self::singleton_live`] with scoped scope.
    pub fn scoped_live<F, Fut, E>(factory: F) -> Provider<T>
    where
        F: Fn(Injector, LivePublisher<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Shared<T>, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        Self::live_with_scope(Scope::Scoped, factory)
    }

    /// A run that no live cell observes, such as a hard resolve that starts
    /// it, gets a publisher whose partials go nowhere.
    fn live_with_scope<F, Fut, E>(scope: Scope, factory: F) -> Provider<T>
    where
        F: Fn(Injector, LivePublisher<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Shared<T>, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        let factory = Shared::new(factory);
        let mut provider = Self::try_async_with_scope(scope, {
            let factory = factory.clone();
            move |injector| factory(injector, LivePublisher::unobserved())
        });
        provider.live_factory = Some(Box::new(move |injector, publisher| {
            let future = factory(injector, publisher);
            Box::pin(async move {
                future.await.map(Instance::new).map_err(|source| {
                    Error::factory_failed(std::any::type_name::<T>(), source.into())
                })
            })
        }));
        provider
    }

    /// The run of the factory a live producer uses: the live factory with
    /// `publisher`, else the async factory.
    pub(crate) fn live_run(
        &self,
        injector: Injector,
        publisher: LivePublisher<T>,
    ) -> Option<AsyncRun<T>> {
        match &self.live_factory {
            Some(factory) => Some(factory(injector, publisher)),
            None => self.async_run(injector),
        }
    }
}
