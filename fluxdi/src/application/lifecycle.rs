use super::*;
use crate::application::options::{BootstrapOptions, ShutdownOptions};

impl Application {
    /// Bootstraps the application by loading the root module and all its imports.
    ///
    /// This method recursively processes the module hierarchy:
    /// 1. Creates child injectors for each module
    /// 2. Loads all imported modules first
    /// 3. Registers the module's own providers
    ///
    /// # Panics
    ///
    /// Panics if called more than once on the same application instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxdi::application::Application;
    /// use fluxdi::module::Module;
    /// use fluxdi::injector::Injector;
    ///
    /// struct AppModule;
    ///
    /// impl Module for AppModule {
    ///     fn providers(&self, injector: &Injector) {
    ///         // Register providers
    ///     }
    /// }
    ///
    /// let mut app = Application::new(AppModule);
    /// app.bootstrap_sync().unwrap();
    /// assert!(app.is_bootstrapped());
    /// ```
    ///
    /// # Panics Example
    ///
    /// ```should_panic
    /// use fluxdi::application::Application;
    /// use fluxdi::module::Module;
    /// use fluxdi::injector::Injector;
    ///
    /// struct AppModule;
    /// impl Module for AppModule {
    ///     fn providers(&self, injector: &Injector) {}
    /// }
    ///
    /// let mut app = Application::new(AppModule);
    /// app.bootstrap_sync().unwrap();
    /// app.bootstrap_sync().unwrap(); // Panics: Application already bootstrapped
    /// ```
    pub fn bootstrap_sync(&mut self) -> Result<(), Error> {
        let root = self.root.take().expect("Application already bootstrapped");

        #[cfg(feature = "tracing")]
        info!("Starting application bootstrap process");

        Self::load_module(self.injector.clone(), root)?;

        #[cfg(feature = "tracing")]
        info!("Application bootstrap completed successfully");
        Ok(())
    }

    /// Unified bootstrap that supports module async providers and lifecycle hooks.
    ///
    /// This executes:
    /// 1. `configure()` for each module (imports first)
    /// 2. `on_start()` for each module
    pub async fn bootstrap(&mut self) -> Result<(), Error> {
        self.bootstrap_with_opts(BootstrapOptions::default()).await
    }

    /// Backward-compatible alias for the old async bootstrap name.
    pub async fn bootstrap_async(&mut self) -> Result<(), Error> {
        self.bootstrap().await
    }

    /// Bootstrap with options (e.g. timeout, parallel_start).
    ///
    /// When the `lifecycle` feature is enabled and `options.timeout` is `Some`,
    /// the bootstrap will fail with an error if it exceeds the given duration.
    ///
    /// When `options.parallel_start` is `true`, module `on_start` hooks run in
    /// parallel; failures are aggregated and returned as a single error.
    #[allow(unused_variables)]
    pub async fn bootstrap_with_options(&mut self, opts: BootstrapOptions) -> Result<(), Error> {
        #[cfg(feature = "lifecycle")]
        {
            if let Some(duration) = opts.timeout {
                return match tokio::time::timeout(duration, self.bootstrap_with_opts(opts)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(Error::new(
                        crate::ErrorKind::ModuleLifecycleFailed,
                        format!("bootstrap timed out after {:?}", duration),
                    )),
                };
            }
        }

        self.bootstrap_with_opts(opts).await
    }

    async fn bootstrap_with_opts(&mut self, opts: BootstrapOptions) -> Result<(), Error> {
        let root = self.root.take().expect("Application already bootstrapped");

        #[cfg(feature = "tracing")]
        info!("Starting async application bootstrap process");

        self.started_modules = Self::load_module_async(self.injector.clone(), root, opts).await?;

        #[cfg(feature = "tracing")]
        info!("Async application bootstrap completed successfully");
        Ok(())
    }

    /// Executes module `on_stop()` hooks in reverse startup order.
    ///
    /// With the `live` feature, it first calls
    /// [`Injector::shutdown_live`](crate::Injector::shutdown_live) on the root
    /// injector, so an `on_stop` that waits for a live dependency sees
    /// `LiveProducerCancelled` instead of waiting for ever.
    ///
    /// If one or more modules fail during shutdown, all modules are still
    /// attempted, and a single aggregated error is returned listing every failure.
    pub async fn shutdown(&mut self) -> Result<(), Error> {
        #[cfg(feature = "tracing")]
        info!("Starting async application shutdown process");

        Self::end_live_production(&self.injector);

        let mut shutdown_errors = Vec::new();

        while let Some(loaded) = self.started_modules.pop() {
            let module_name = std::any::type_name_of_val(&*loaded.module);
            if let Err(err) = loaded.module.on_stop(loaded.injector.clone()).await {
                shutdown_errors.push(Error::module_lifecycle_failed(
                    module_name,
                    "on_stop",
                    &err.to_string(),
                ));
            }
        }

        #[cfg(feature = "tracing")]
        info!("Async application shutdown completed successfully");

        if shutdown_errors.is_empty() {
            Ok(())
        } else {
            Err(Error::shutdown_aggregate(shutdown_errors))
        }
    }

    /// Backward-compatible alias for the old async shutdown name.
    pub async fn shutdown_async(&mut self) -> Result<(), Error> {
        self.shutdown().await
    }

    /// Shutdown with options (e.g. timeout).
    ///
    /// When the `lifecycle` feature is enabled and `options.timeout` is `Some`,
    /// the shutdown uses a graceful timeout: live production ends first, as in
    /// [`shutdown`](Self::shutdown), then each module's `on_stop` is attempted
    /// within the remaining time budget. All modules are always attempted (no
    /// partial abort); timeouts and failures are aggregated into a single error.
    #[allow(unused_variables)]
    pub async fn shutdown_with_options(&mut self, opts: ShutdownOptions) -> Result<(), Error> {
        #[cfg(feature = "lifecycle")]
        {
            if let Some(duration) = opts.timeout {
                return self.shutdown_with_deadline(duration).await;
            }
        }

        self.shutdown().await
    }

    #[cfg(feature = "lifecycle")]
    async fn shutdown_with_deadline(&mut self, duration: std::time::Duration) -> Result<(), Error> {
        use std::time::Instant;

        #[cfg(feature = "tracing")]
        info!(
            "Starting async application shutdown process (timeout: {:?})",
            duration
        );

        Self::end_live_production(&self.injector);

        let deadline = Instant::now() + duration;
        let mut shutdown_errors = Vec::new();

        while let Some(loaded) = self.started_modules.pop() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let module_name = std::any::type_name_of_val(&*loaded.module);

            match tokio::time::timeout(
                std::cmp::max(remaining, std::time::Duration::from_millis(1)),
                loaded.module.on_stop(loaded.injector.clone()),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    shutdown_errors.push(Error::module_lifecycle_failed(
                        module_name,
                        "on_stop",
                        &err.to_string(),
                    ));
                }
                Err(_) => {
                    shutdown_errors.push(Error::module_lifecycle_failed(
                        module_name,
                        "on_stop",
                        &format!("timed out during shutdown (limit: {:?})", duration),
                    ));
                }
            }
        }

        #[cfg(feature = "tracing")]
        info!("Async application shutdown completed");

        if shutdown_errors.is_empty() {
            Ok(())
        } else {
            Err(Error::shutdown_aggregate(shutdown_errors))
        }
    }

    /// Runs before every `on_stop`, at shutdown and at bootstrap rollback.
    #[cfg(feature = "live")]
    pub(super) fn end_live_production(root: &Injector) {
        root.shutdown_live();
    }

    #[cfg(not(feature = "live"))]
    pub(super) fn end_live_production(_: &Injector) {}

    /// Backward-compatible alias for startup.
    pub async fn start(&mut self) -> Result<(), Error> {
        self.bootstrap().await
    }

    /// Backward-compatible alias for shutdown.
    pub async fn stop(&mut self) -> Result<(), Error> {
        self.shutdown().await
    }
}
