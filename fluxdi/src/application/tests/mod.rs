use super::*;
use crate::module::ModuleLifecycleFuture;
use crate::{Error, ErrorKind};
use futures::executor::block_on;

#[cfg(not(feature = "thread-safe"))]
use std::cell::RefCell;
#[cfg(not(feature = "thread-safe"))]
use std::rc::Rc;

#[cfg(feature = "thread-safe")]
use std::sync::{Arc, Mutex};

mod bootstrap_and_lifecycle;
mod error_source;
#[cfg(feature = "live")]
mod live_shutdown;
mod state_and_structure;

struct EmptyModule;

impl Module for EmptyModule {
    fn providers(&self, _injector: &Injector) {}
}

// CountingModule with conditional thread safety
#[cfg(not(feature = "thread-safe"))]
struct CountingModule {
    counter: Rc<RefCell<usize>>,
}

#[cfg(not(feature = "thread-safe"))]
impl Module for CountingModule {
    fn providers(&self, _injector: &Injector) {
        *self.counter.borrow_mut() += 1;
    }
}

#[cfg(feature = "thread-safe")]
struct CountingModule {
    counter: Arc<Mutex<usize>>,
}

#[cfg(feature = "thread-safe")]
impl Module for CountingModule {
    fn providers(&self, _injector: &Injector) {
        *self.counter.lock().unwrap() += 1;
    }
}

// ModuleWithImports with conditional thread safety
#[cfg(not(feature = "thread-safe"))]
struct ModuleWithImports {
    counter: Rc<RefCell<usize>>,
}

#[cfg(not(feature = "thread-safe"))]
impl Module for ModuleWithImports {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        vec![
            Box::new(CountingModule {
                counter: self.counter.clone(),
            }),
            Box::new(CountingModule {
                counter: self.counter.clone(),
            }),
        ]
    }

    fn providers(&self, _injector: &Injector) {
        *self.counter.borrow_mut() += 1;
    }
}

#[cfg(feature = "thread-safe")]
struct ModuleWithImports {
    counter: Arc<Mutex<usize>>,
}

#[cfg(feature = "thread-safe")]
impl Module for ModuleWithImports {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        vec![
            Box::new(CountingModule {
                counter: self.counter.clone(),
            }),
            Box::new(CountingModule {
                counter: self.counter.clone(),
            }),
        ]
    }

    fn providers(&self, _injector: &Injector) {
        *self.counter.lock().unwrap() += 1;
    }
}

#[cfg(not(feature = "thread-safe"))]
type EventLog = Rc<RefCell<Vec<String>>>;
#[cfg(feature = "thread-safe")]
type EventLog = Arc<Mutex<Vec<String>>>;

#[cfg(not(feature = "thread-safe"))]
fn push_event(log: &EventLog, event: String) {
    log.borrow_mut().push(event);
}

#[cfg(feature = "thread-safe")]
fn push_event(log: &EventLog, event: String) {
    log.lock().unwrap().push(event);
}

#[cfg(not(feature = "thread-safe"))]
fn event_snapshot(log: &EventLog) -> Vec<String> {
    log.borrow().clone()
}

#[cfg(feature = "thread-safe")]
fn event_snapshot(log: &EventLog) -> Vec<String> {
    log.lock().unwrap().clone()
}

struct LifecycleModule {
    name: &'static str,
    log: EventLog,
    import_child: bool,
}

impl Module for LifecycleModule {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        if !self.import_child {
            return vec![];
        }

        vec![Box::new(LifecycleModule {
            name: "import",
            log: self.log.clone(),
            import_child: false,
        })]
    }

    fn providers(&self, _injector: &Injector) {
        push_event(&self.log, format!("providers:{}", self.name));
    }

    fn on_start(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        let log = self.log.clone();
        let name = self.name.to_string();
        Box::pin(async move {
            push_event(&log, format!("on_start:{}", name));
            Ok(())
        })
    }

    fn on_stop(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        let log = self.log.clone();
        let name = self.name.to_string();
        Box::pin(async move {
            push_event(&log, format!("on_stop:{}", name));
            Ok(())
        })
    }
}

struct FailingLifecycleModule;

impl Module for FailingLifecycleModule {
    fn on_start(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        Box::pin(async {
            Err(Error::module_lifecycle_failed(
                "FailingLifecycleModule",
                "on_start",
                "intentional test failure",
            ))
        })
    }
}

/// Module whose on_stop fails; used for shutdown aggregation tests.
struct FailingShutdownModule;

impl Module for FailingShutdownModule {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        vec![Box::new(FailingShutdownImport)]
    }

    fn providers(&self, _injector: &Injector) {}

    fn on_stop(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        Box::pin(async {
            Err(Error::module_lifecycle_failed(
                "FailingShutdownModule",
                "on_stop",
                "root_stop_failed",
            ))
        })
    }
}

struct FailingShutdownImport;

impl Module for FailingShutdownImport {
    fn providers(&self, _injector: &Injector) {}

    fn on_stop(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        Box::pin(async {
            Err(Error::module_lifecycle_failed(
                "FailingShutdownImport",
                "on_stop",
                "import_stop_failed",
            ))
        })
    }
}

/// Module whose on_start fails (root + import); used for parallel bootstrap aggregation tests.
struct FailingBootstrapModule;

impl Module for FailingBootstrapModule {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        vec![Box::new(FailingBootstrapImport)]
    }

    fn providers(&self, _injector: &Injector) {}

    fn on_start(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        Box::pin(async {
            Err(Error::module_lifecycle_failed(
                "FailingBootstrapModule",
                "on_start",
                "root_start_failed",
            ))
        })
    }
}

struct FailingBootstrapImport;

impl Module for FailingBootstrapImport {
    fn providers(&self, _injector: &Injector) {}

    fn on_start(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        Box::pin(async {
            Err(Error::module_lifecycle_failed(
                "FailingBootstrapImport",
                "on_start",
                "import_start_failed",
            ))
        })
    }
}

/// Import succeeds, root fails on_start; used for rollback tests.
struct RollbackTestModule {
    log: EventLog,
}

impl Module for RollbackTestModule {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        vec![Box::new(RollbackTestImport {
            log: self.log.clone(),
        })]
    }

    fn providers(&self, _injector: &Injector) {
        push_event(&self.log, "providers:root".to_string());
    }

    fn on_start(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        let log = self.log.clone();
        Box::pin(async move {
            push_event(&log, "on_start:root".to_string());
            Err(Error::module_lifecycle_failed(
                "RollbackTestModule",
                "on_start",
                "root_start_failed",
            ))
        })
    }
}

struct RollbackTestImport {
    log: EventLog,
}

impl Module for RollbackTestImport {
    fn providers(&self, _injector: &Injector) {
        push_event(&self.log, "providers:import".to_string());
    }

    fn on_start(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        let log = self.log.clone();
        Box::pin(async move {
            push_event(&log, "on_start:import".to_string());
            Ok(())
        })
    }

    fn on_stop(&self, _injector: Shared<Injector>) -> ModuleLifecycleFuture {
        let log = self.log.clone();
        Box::pin(async move {
            push_event(&log, "on_stop:import".to_string());
            Ok(())
        })
    }
}

// NestedImportModule with conditional thread safety
#[cfg(not(feature = "thread-safe"))]
struct NestedImportModule {
    counter: Rc<RefCell<usize>>,
    depth: usize,
}

#[cfg(not(feature = "thread-safe"))]
impl Module for NestedImportModule {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        if self.depth > 0 {
            vec![Box::new(NestedImportModule {
                counter: self.counter.clone(),
                depth: self.depth - 1,
            })]
        } else {
            vec![]
        }
    }

    fn providers(&self, _injector: &Injector) {
        *self.counter.borrow_mut() += 1;
    }
}

#[cfg(feature = "thread-safe")]
struct NestedImportModule {
    counter: Arc<Mutex<usize>>,
    depth: usize,
}

#[cfg(feature = "thread-safe")]
impl Module for NestedImportModule {
    fn imports(&self) -> Vec<Box<dyn Module>> {
        if self.depth > 0 {
            vec![Box::new(NestedImportModule {
                counter: self.counter.clone(),
                depth: self.depth - 1,
            })]
        } else {
            vec![]
        }
    }

    fn providers(&self, _injector: &Injector) {
        *self.counter.lock().unwrap() += 1;
    }
}
