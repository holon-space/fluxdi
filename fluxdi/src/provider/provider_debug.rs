use super::*;

#[cfg(feature = "debug")]
impl<T: ?Sized + 'static> std::fmt::Debug for Provider<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct(std::any::type_name::<Self>());

        ds.field("scope", &self.scope);

        #[cfg(feature = "thread-safe")]
        {
            ds.field(
                "factory",
                &"Box<dyn Fn(&Injector) -> Instance<T> + Send + Sync + 'static>",
            );
        }

        #[cfg(not(feature = "thread-safe"))]
        {
            ds.field(
                "factory",
                &"Box<dyn Fn(&Injector) -> Instance<T> + 'static>",
            );
        }

        #[cfg(feature = "async-factory")]
        {
            ds.field("async_factory", &self.async_factory.is_some());
        }

        #[cfg(feature = "live")]
        {
            ds.field("live_factory", &self.live_factory.is_some());
        }

        ds.field("limits", &self.limits);
        ds.field("dependency_hints", &self.dependency_hints.len());
        ds.field("limiter", &self.limiter.is_some());

        ds.finish()
    }
}
