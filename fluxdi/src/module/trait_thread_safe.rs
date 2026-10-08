use super::*;

#[cfg(feature = "thread-safe")]
pub trait Module: Send + Sync {
    /// Returns the unique type identifier for this module.
    ///
    /// This method provides runtime type identification for modules, which can be useful
    /// for debugging, logging, or implementing module deduplication logic.
    ///
    /// # Returns
    ///
    /// A [`TypeId`](std::any::TypeId) that uniquely identifies the concrete type of this module.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxdi::module::Module;
    /// use fluxdi::injector::Injector;
    /// use std::any::TypeId;
    ///
    /// struct MyModule;
    /// impl Module for MyModule {
    ///     fn providers(&self, injector: &Injector) {}
    /// }
    ///
    /// let module = MyModule;
    /// let type_id = module.type_id();
    /// assert_eq!(type_id, TypeId::of::<MyModule>());
    /// ```
    fn type_id(&self) -> std::any::TypeId
    where
        Self: 'static,
    {
        std::any::TypeId::of::<Self>()
    }

    /// Returns the type name of this module as a string.
    ///
    /// This method provides a human-readable representation of the module's type,
    /// which is particularly useful for debugging, logging, and error messages.
    /// Lifecycle errors name the failing module with it ([`Error::module_name`]).
    ///
    /// # Returns
    ///
    /// A static string slice containing the fully-qualified type name of this module.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxdi::module::Module;
    /// use fluxdi::injector::Injector;
    ///
    /// struct DatabaseModule;
    /// impl Module for DatabaseModule {
    ///     fn providers(&self, injector: &Injector) {}
    /// }
    ///
    /// let module = DatabaseModule;
    /// let name = module.type_name();
    /// // The exact format depends on the module path
    /// assert!(name.contains("DatabaseModule"));
    /// ```
    fn type_name(&self) -> &'static str
    where
        Self: 'static,
    {
        std::any::type_name::<Self>()
    }

    /// Returns a list of modules that this module imports.
    ///
    /// Imported modules have their providers registered before this module's providers.
    /// This allows a module to build upon functionality provided by other modules.
    ///
    /// # Default Implementation
    ///
    /// By default, returns an empty vector (no imports).
    ///
    /// # Returns
    ///
    /// A vector of boxed `Module` trait objects representing the imported modules.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxdi::module::Module;
    /// use fluxdi::injector::Injector;
    ///
    /// struct CoreModule;
    /// impl Module for CoreModule {
    ///     fn providers(&self, injector: &Injector) {}
    /// }
    ///
    /// struct FeatureModule;
    /// impl Module for FeatureModule {
    ///     fn imports(&self) -> Vec<Box<dyn Module>> {
    ///         vec![Box::new(CoreModule)]
    ///     }
    ///
    ///     fn providers(&self, injector: &Injector) {}
    /// }
    /// ```
    fn imports(&self) -> Vec<Box<dyn Module>> {
        vec![]
    }

    /// Configures providers for this module.
    ///
    /// This is the preferred registration hook used by `Application` bootstrap flows.
    /// The default implementation delegates to [`providers`](Module::providers) for
    /// backward compatibility.
    fn configure(&self, injector: &Injector) -> Result<(), Error> {
        self.providers(injector);
        Ok(())
    }

    /// Registers providers with the given injector.
    ///
    /// This method is called to configure the dependency injection container with
    /// the services that this module provides. Use the injector to register
    /// factories, values, and other providers.
    ///
    /// # Parameters
    ///
    /// - `injector`: The injector instance to register providers with
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxdi::module::Module;
    /// use fluxdi::injector::Injector;
    ///
    /// struct MyModule;
    ///
    /// impl Module for MyModule {
    ///     fn providers(&self, injector: &Injector) {
    ///         // Register providers here
    ///         // injector.register<...>(...)
    ///     }
    /// }
    /// ```
    fn providers(&self, _injector: &Injector) {}

    /// Async variant of provider registration.
    ///
    /// Default behavior calls [`providers`](Module::providers) synchronously.
    fn providers_async(&self, injector: Shared<Injector>) -> ModuleLifecycleFuture {
        let result = self.configure(&injector);
        Box::pin(async move { result })
    }

    /// Lifecycle hook executed after this module and its imports finish registration.
    fn on_start(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        Box::pin(async { Ok(()) })
    }

    /// Lifecycle hook executed during application shutdown in reverse module order.
    fn on_stop(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        Box::pin(async { Ok(()) })
    }
}
