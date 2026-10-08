use super::*;
use crate::application::options::BootstrapOptions;

impl Application {
    /// Recursively loads a module and its imports into the injector hierarchy.
    ///
    /// Creates a child injector for the module, loads all imported modules first,
    /// then registers the module's own providers. This ensures proper dependency
    /// resolution order.
    ///
    /// # Parameters
    ///
    /// - `parent`: The parent injector to create a child from
    /// - `module`: The module to load
    pub(super) fn load_module(parent: Shared<Injector>, module: ModuleObject) -> Result<(), Error> {
        #[cfg(feature = "tracing")]
        debug!("Loading module into injector hierarchy");

        let module_injector = Shared::new(Injector::child(parent.clone()));

        #[cfg(feature = "tracing")]
        debug!("Created child injector for module");

        let imports = module.imports();
        #[cfg(feature = "tracing")]
        if !imports.is_empty() {
            debug!("Module has {} imports, loading them first", imports.len());
        }

        #[allow(unused_variables)]
        for (index, import) in imports.into_iter().enumerate() {
            #[cfg(feature = "tracing")]
            debug!("Loading import {}", index + 1);

            Self::load_module(module_injector.clone(), import)?;
        }

        #[cfg(feature = "tracing")]
        debug!("Registering module providers");

        let module_name = std::any::type_name_of_val(&*module);
        module.configure(&module_injector).map_err(|err| {
            Error::module_lifecycle_failed(module_name, "configure", &err.to_string())
        })?;

        #[cfg(feature = "tracing")]
        debug!("Module loaded successfully");
        Ok(())
    }

    pub(super) async fn load_module_async(
        parent: Shared<Injector>,
        module: ModuleObject,
        opts: BootstrapOptions,
    ) -> Result<Vec<LoadedModule>, Error> {
        if opts.parallel_start {
            Self::load_module_async_parallel(parent, module).await
        } else {
            Self::load_module_async_sequential(parent, module).await
        }
    }

    /// Sequential bootstrap: configure + on_start per module in DFS order.
    /// Preserves original behavior (configure/on_start interleaved per module).
    async fn load_module_async_sequential(
        parent: Shared<Injector>,
        module: ModuleObject,
    ) -> Result<Vec<LoadedModule>, Error> {
        enum Frame {
            Enter {
                parent: Shared<Injector>,
                module: ModuleObject,
            },
            Exit {
                module_injector: Shared<Injector>,
                module: ModuleObject,
            },
        }

        let root = parent.clone();
        let mut stack = vec![Frame::Enter { parent, module }];
        let mut loaded: Vec<LoadedModule> = Vec::new();

        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter { parent, module } => {
                    let module_injector = Shared::new(Injector::child(parent));
                    let imports = module.imports();

                    stack.push(Frame::Exit {
                        module_injector: module_injector.clone(),
                        module,
                    });

                    for import in imports.into_iter().rev() {
                        stack.push(Frame::Enter {
                            parent: module_injector.clone(),
                            module: import,
                        });
                    }
                }
                Frame::Exit {
                    module_injector,
                    module,
                } => {
                    let module_name = std::any::type_name_of_val(&*module);
                    module.configure(&module_injector).map_err(|err| {
                        Error::module_lifecycle_failed(module_name, "configure", &err.to_string())
                    })?;

                    if let Err(err) = module.on_start(module_injector.clone()).await {
                        Self::end_live_production(&root);
                        // Rollback: call on_stop on already-started modules (reverse order)
                        while let Some(loaded_mod) = loaded.pop() {
                            let _ = loaded_mod.module.on_stop(loaded_mod.injector.clone()).await;
                        }
                        return Err(Error::module_lifecycle_failed(
                            module_name,
                            "on_start",
                            &err.to_string(),
                        ));
                    }

                    loaded.push(LoadedModule {
                        module,
                        injector: module_injector,
                    });
                }
            }
        }

        Ok(loaded)
    }

    /// Parallel bootstrap: configure all first, then on_start in parallel.
    async fn load_module_async_parallel(
        parent: Shared<Injector>,
        module: ModuleObject,
    ) -> Result<Vec<LoadedModule>, Error> {
        enum Frame {
            Enter {
                parent: Shared<Injector>,
                module: ModuleObject,
            },
            Exit {
                module_injector: Shared<Injector>,
                module: ModuleObject,
            },
        }

        let root = parent.clone();
        let mut stack = vec![Frame::Enter { parent, module }];
        let mut pending: Vec<(ModuleObject, Shared<Injector>)> = Vec::new();

        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter { parent, module } => {
                    let module_injector = Shared::new(Injector::child(parent));
                    let imports = module.imports();

                    stack.push(Frame::Exit {
                        module_injector: module_injector.clone(),
                        module,
                    });

                    for import in imports.into_iter().rev() {
                        stack.push(Frame::Enter {
                            parent: module_injector.clone(),
                            module: import,
                        });
                    }
                }
                Frame::Exit {
                    module_injector,
                    module,
                } => {
                    let module_name = std::any::type_name_of_val(&*module);
                    module.configure(&module_injector).map_err(|err| {
                        Error::module_lifecycle_failed(module_name, "configure", &err.to_string())
                    })?;

                    pending.push((module, module_injector));
                }
            }
        }

        let futures: Vec<_> = pending
            .into_iter()
            .map(|(module, injector)| {
                let module_name = std::any::type_name_of_val(&*module);
                async move {
                    let result = module.on_start(injector.clone()).await;
                    (module, injector, module_name, result)
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        let mut bootstrap_errors: Vec<Error> = Vec::new();
        let mut loaded = Vec::new();

        for (module, injector, module_name, result) in results {
            match result {
                Ok(()) => {
                    loaded.push(LoadedModule { module, injector });
                }
                Err(err) => {
                    bootstrap_errors.push(Error::module_lifecycle_failed(
                        module_name,
                        "on_start",
                        &err.to_string(),
                    ));
                }
            }
        }

        if bootstrap_errors.is_empty() {
            Ok(loaded)
        } else {
            Self::end_live_production(&root);
            // Rollback: call on_stop on successfully-started modules (reverse order)
            while let Some(loaded_mod) = loaded.pop() {
                let _ = loaded_mod.module.on_stop(loaded_mod.injector.clone()).await;
            }
            Err(Error::bootstrap_aggregate(bootstrap_errors))
        }
    }
}
