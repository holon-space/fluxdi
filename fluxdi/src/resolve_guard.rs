use std::any::TypeId;
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::{Error, ErrorKind};
#[cfg(feature = "tracing")]
use crate::observability::EVENT_CIRCULAR_DEPENDENCY;

#[cfg(feature = "tracing")]
use tracing::debug;

/// The resolves from a top-level resolve down to the one in progress.
///
/// A type that appears twice on one path is a cycle. Concurrent sibling
/// resolves extend their common parent path separately, so they never see
/// each other; waits between them are the in-flight cells' concern.
type ResolutionPath = Option<Arc<PathNode>>;

struct PathNode {
    type_id: TypeId,
    parent: ResolutionPath,
}

thread_local! {
    /// The path of the resolve being run on this thread.
    static RESOLUTION_PATH: Cell<ResolutionPath> = const { Cell::new(None) };
}

fn extend_current_path(type_id: TypeId) -> Result<Arc<PathNode>, Error> {
    let parent = crate::future_local::current(&RESOLUTION_PATH);
    let mut ancestors = std::iter::successors(parent.as_deref(), |node| node.parent.as_deref());
    if let Some(_cycle_length) = ancestors.position(|node| node.type_id == type_id) {
        #[cfg(feature = "tracing")]
        debug!(
            event = EVENT_CIRCULAR_DEPENDENCY,
            type_id = ?type_id,
            cycle_length = _cycle_length + 1,
            "Circular dependency detected during resolve"
        );
        return Err(Error::new(
            ErrorKind::CircularDependency,
            format!(
                "Circular dependency detected while resolving type_id: {:?}",
                type_id
            ),
        ));
    }
    Ok(Arc::new(PathNode { type_id, parent }))
}

/// Puts a synchronous resolve of `type_id` on the current path until dropped.
///
/// Not `Send`: it must drop on the thread, and within the poll, that pushed it.
pub struct ResolveGuard {
    node: Arc<PathNode>,
    _not_send: PhantomData<*const ()>,
}

impl ResolveGuard {
    pub fn push(type_id: TypeId) -> Result<Self, Error> {
        let node = extend_current_path(type_id)?;
        RESOLUTION_PATH.with(|path| path.set(Some(node.clone())));
        Ok(Self {
            node,
            _not_send: PhantomData,
        })
    }
}

impl Drop for ResolveGuard {
    fn drop(&mut self) {
        RESOLUTION_PATH.with(|path| {
            let current = path.take();
            assert!(
                current.is_some_and(|current| Arc::ptr_eq(&current, &self.node)),
                "resolve guards drop in the reverse order of their pushes"
            );
            path.set(self.node.parent.clone());
        });
    }
}

/// Runs the async resolve of `type_id` on the path of the resolve that polls
/// it first, extended by `type_id`.
#[cfg(feature = "async-factory")]
pub(crate) async fn resolving<R>(
    type_id: TypeId,
    resolve: impl std::future::Future<Output = Result<R, Error>>,
) -> Result<R, Error> {
    let path = extend_current_path(type_id)?;
    crate::future_local::WithLocal::new(&RESOLUTION_PATH, path, resolve).await
}

/// Runs the async resolve of `type_id` on a new path that starts with it,
/// whatever path the thread polling it is on.
#[cfg(feature = "live")]
pub(crate) async fn resolving_on_new_path<R>(
    type_id: TypeId,
    resolve: impl std::future::Future<Output = Result<R, Error>>,
) -> Result<R, Error> {
    let path = Arc::new(PathNode {
        type_id,
        parent: None,
    });
    crate::future_local::WithLocal::new(&RESOLUTION_PATH, path, resolve).await
}

#[cfg(test)]
mod tests {
    use crate::{Error, ErrorKind, Injector, Provider, Shared};
    use std::sync::{Arc, Mutex};

    struct A;
    struct B;

    #[test]
    fn a_sync_resolve_that_reaches_its_own_type_is_a_cycle() {
        let inner = Arc::new(Mutex::new(None));
        let injector = Injector::root();
        injector.provide::<A>(Provider::transient(|inj: &Injector| {
            inj.try_resolve::<B>().unwrap();
            Shared::new(A)
        }));
        injector.provide::<B>(Provider::transient({
            let inner = inner.clone();
            move |inj: &Injector| {
                *inner.lock().unwrap() = inj.try_resolve::<A>().err().map(|e| e.kind);
                Shared::new(B)
            }
        }));

        injector.try_resolve::<A>().unwrap();
        assert_eq!(*inner.lock().unwrap(), Some(ErrorKind::CircularDependency));
        assert!(
            crate::future_local::current(&super::RESOLUTION_PATH).is_none(),
            "the path outlived its resolves"
        );
    }

    #[cfg(feature = "async-factory")]
    #[test]
    fn an_async_resolve_that_reaches_its_own_type_is_a_cycle() {
        let injector = Injector::root();
        injector.provide::<A>(Provider::transient_try_async(|inj: Injector| async move {
            inj.try_resolve_async::<B>().await?;
            Ok::<_, Error>(Shared::new(A))
        }));
        injector.provide::<B>(Provider::transient_try_async(|inj: Injector| async move {
            inj.try_resolve_async::<A>().await?;
            Ok::<_, Error>(Shared::new(B))
        }));

        let err = futures::executor::block_on(injector.try_resolve_async::<A>())
            .err()
            .expect("A -> B -> A resolved");

        let kinds: Vec<ErrorKind> =
            std::iter::successors(Some(&err as &(dyn std::error::Error + 'static)), |e| {
                e.source()
            })
            .filter_map(|e| e.downcast_ref::<Error>().map(|e| e.kind.clone()))
            .collect();
        assert_eq!(kinds.last(), Some(&ErrorKind::CircularDependency), "{err}");
    }
}
