use super::*;
use crate::{ErrorKind, Provider, Shared};

use futures::channel::oneshot;
use futures::future::{FutureExt, Shared as SharedFuture};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const HANG_LIMIT: Duration = Duration::from_secs(5);

struct Svc(usize);

/// Opens once; every waiter sees the opening.
struct Latch {
    open: Mutex<Option<oneshot::Sender<()>>>,
    opened: SharedFuture<oneshot::Receiver<()>>,
}

impl Latch {
    fn new() -> Arc<Self> {
        let (open, opened) = oneshot::channel();
        Arc::new(Self {
            open: Mutex::new(Some(open)),
            opened: opened.shared(),
        })
    }

    fn open(&self) {
        if let Some(open) = self.open.lock().unwrap().take() {
            open.send(()).unwrap();
        }
    }

    async fn wait(&self) {
        self.opened.clone().await.expect("latch dropped unopened");
    }
}

/// Runs `future` on a single thread and fails the test when it does not finish.
fn within<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(HANG_LIMIT, future)
                .await
                .expect("resolution hung")
        })
}

/// Factory whose run `n` (1-based) waits for `gates[n - 1]`.
fn gated_provider(scope: Scope, runs: Arc<AtomicUsize>, gates: Vec<Arc<Latch>>) -> Provider<Svc> {
    let factory = move |_: Injector| {
        let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
        let gate = gates[run - 1].clone();
        async move {
            gate.wait().await;
            Shared::new(Svc(run))
        }
    };
    match scope {
        Scope::Root => Provider::root_async(factory),
        Scope::Scoped => Provider::scoped_async(factory),
        other => panic!("no gated provider for scope {other}"),
    }
}

fn chain_has_kind(err: &Error, kind: ErrorKind) -> bool {
    std::iter::successors(Some(err as &(dyn std::error::Error + 'static)), |e| {
        e.source()
    })
    .any(|e| e.downcast_ref::<Error>().is_some_and(|e| e.kind == kind))
}

#[test]
fn concurrent_resolves_in_one_task_share_one_factory_run() {
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Latch::new();
    let injector = Injector::root();
    injector.provide::<Svc>(gated_provider(
        Scope::Root,
        runs.clone(),
        vec![gate.clone(), Latch::new()],
    ));

    let (first, second, ()) = within(async {
        futures::join!(
            injector.try_resolve_async::<Svc>(),
            injector.try_resolve_async::<Svc>(),
            async { gate.open() },
        )
    });

    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(runs.load(Ordering::SeqCst), 1, "the factory ran twice");
    assert!(
        Shared::ptr_eq(&first, &second),
        "two instances of a singleton"
    );
}

#[cfg(feature = "thread-safe")]
#[test]
fn concurrent_resolves_on_two_threads_share_one_factory_run() {
    use std::sync::mpsc;
    use std::time::Instant;

    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Latch::new();
    let injector = Injector::root();
    injector.provide::<Svc>(gated_provider(
        Scope::Root,
        runs.clone(),
        vec![gate.clone(), gate.clone()],
    ));

    let (results, received) = mpsc::channel();
    let resolve = |injector: Injector, results: mpsc::Sender<_>| {
        std::thread::spawn(move || {
            results
                .send(futures::executor::block_on(
                    injector.try_resolve_async::<Svc>(),
                ))
                .unwrap()
        })
    };

    resolve(injector.clone(), results.clone());
    let deadline = Instant::now() + HANG_LIMIT;
    while runs.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "first factory run never started");
        std::thread::yield_now();
    }
    resolve(injector.clone(), results);
    // Gives the second resolve time to reach the factory, should it run it.
    std::thread::sleep(Duration::from_millis(100));
    gate.open();

    let first = received.recv_timeout(HANG_LIMIT).unwrap().unwrap();
    let second = received.recv_timeout(HANG_LIMIT).unwrap().unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 1, "the factory ran twice");
    assert!(
        Shared::ptr_eq(&first, &second),
        "two instances of a singleton"
    );
}

#[test]
fn every_scope_gets_its_own_cell() {
    let runs = Arc::new(AtomicUsize::new(0));
    let gates: Vec<_> = (0..4).map(|_| Latch::new()).collect();
    let root = Injector::root();
    root.provide::<Svc>(gated_provider(Scope::Scoped, runs.clone(), gates.clone()));

    within(async {
        let first_scope = root.create_scope();
        let second_scope = root.create_scope();
        let (first, second, ()) = futures::join!(
            first_scope.try_resolve_async::<Svc>(),
            second_scope.try_resolve_async::<Svc>(),
            async {
                gates[0].open();
                gates[1].open();
            },
        );
        let (first, second) = (first.unwrap(), second.unwrap());
        assert_eq!(runs.load(Ordering::SeqCst), 2);
        assert!(!Shared::ptr_eq(&first, &second));

        let dropped_scope = root.create_scope();
        let mut abandoned = Box::pin(dropped_scope.try_resolve_async::<Svc>());
        assert!(futures::poll!(&mut abandoned).is_pending());
        drop(abandoned);
        drop(dropped_scope);
        assert_eq!(runs.load(Ordering::SeqCst), 3);
        assert_eq!(root.inner.cells.in_flight(), 0);

        let new_scope = root.create_scope();
        gates[2].open();
        gates[3].open();
        let fresh = new_scope.try_resolve_async::<Svc>().await.unwrap();
        assert_eq!(fresh.0, 4, "the new scope reused the dropped scope's cell");
        let cached = new_scope.try_resolve_async::<Svc>().await.unwrap();
        assert!(Shared::ptr_eq(&fresh, &cached));
        assert_eq!(runs.load(Ordering::SeqCst), 4);
        assert_eq!(root.inner.cells.in_flight(), 0);
    });
}

struct A;
struct B;

/// `A`'s factory resolves `B` and `B`'s factory resolves `A`, each only after
/// both factories have started, so each resolve finds the other in flight.
/// `A` resolves `B` only once `a_proceeds` opens.
fn provide_cross_waiting_pair(injector: &Injector, a_proceeds: Arc<Latch>) {
    let a_started = Latch::new();
    let b_started = Latch::new();
    injector.provide::<A>(Provider::root_try_async({
        let (a_started, b_started) = (a_started.clone(), b_started.clone());
        move |inj: Injector| {
            let (a_started, b_started) = (a_started.clone(), b_started.clone());
            let a_proceeds = a_proceeds.clone();
            async move {
                a_started.open();
                b_started.wait().await;
                a_proceeds.wait().await;
                inj.try_resolve_async::<B>().await?;
                Ok::<_, Error>(Shared::new(A))
            }
        }
    }));
    injector.provide::<B>(Provider::root_try_async(move |inj: Injector| {
        let (a_started, b_started) = (a_started.clone(), b_started.clone());
        async move {
            b_started.open();
            a_started.wait().await;
            inj.try_resolve_async::<A>().await?;
            Ok::<_, Error>(Shared::new(B))
        }
    }));
}

fn assert_cycle_reported(a: Result<Shared<A>, Error>, b: Result<Shared<B>, Error>) {
    let (Err(a), Err(b)) = (a, b) else {
        panic!("a cross wait between two factories resolved");
    };
    assert!(chain_has_kind(&a, ErrorKind::CircularDependency), "A: {a}");
    assert!(chain_has_kind(&b, ErrorKind::CircularDependency), "B: {b}");
}

/// `B`'s factory joins `A` before `A`'s factory resolves `B`; `A`'s chain has
/// not seen `B`, so only the wait-for graph sees the cycle.
#[test]
fn cross_join_in_one_task_is_a_circular_dependency() {
    let injector = Injector::root();
    let a_proceeds = Latch::new();
    provide_cross_waiting_pair(&injector, a_proceeds.clone());

    let (a, b, ()) = within(async {
        futures::join!(
            injector.try_resolve_async::<A>(),
            injector.try_resolve_async::<B>(),
            async { a_proceeds.open() },
        )
    });

    assert_cycle_reported(a, b);
}

#[cfg(feature = "thread-safe")]
#[test]
fn cross_join_between_two_tasks_is_a_circular_dependency() {
    let injector = Injector::root();
    let a_proceeds = Latch::new();
    a_proceeds.open();
    provide_cross_waiting_pair(&injector, a_proceeds);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap();
    let (a, b) = runtime.block_on(async {
        let a = tokio::spawn({
            let injector = injector.clone();
            async move { injector.try_resolve_async::<A>().await }
        });
        let b = tokio::spawn({
            let injector = injector.clone();
            async move { injector.try_resolve_async::<B>().await }
        });
        tokio::time::timeout(HANG_LIMIT, async { (a.await.unwrap(), b.await.unwrap()) })
            .await
            .expect("cross join hung")
    });

    assert_cycle_reported(a, b);
}

#[test]
fn failed_run_is_retried_by_the_next_resolve_and_joiners_see_the_failure() {
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Latch::new();
    let injector = Injector::root();
    injector.provide::<Svc>(Provider::root_try_async({
        let (runs, gate) = (runs.clone(), gate.clone());
        move |_: Injector| {
            let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
            let gate = gate.clone();
            async move {
                gate.wait().await;
                if run == 1 {
                    return Err("first run fails".into());
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Shared::new(Svc(run)))
            }
        }
    }));

    within(async {
        let (first, second, ()) = futures::join!(
            injector.try_resolve_async::<Svc>(),
            injector.try_resolve_async::<Svc>(),
            async { gate.open() },
        );
        for err in [first.err().unwrap(), second.err().unwrap()] {
            assert_eq!(err.kind, ErrorKind::FactoryFailed);
            assert!(err.message.contains("first run fails"), "{}", err.message);
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        let retried = injector.try_resolve_async::<Svc>().await.unwrap();
        assert_eq!(retried.0, 2);
    });
}

/// A run that completes inside its parent's poll must not wake the parent
/// through a one-shot waker: a nested `block_on` on the same thread takes that
/// wake-up, and the parent's real one then finds no waker to reach.
#[cfg(feature = "thread-safe")]
#[test]
fn a_nested_block_on_does_not_lose_the_wake_up_of_a_run() {
    use std::sync::mpsc;

    let injector = Injector::root();
    injector.provide_into_set::<usize>(Provider::root(|_| Shared::new(1)));
    injector.provide_into_set::<usize>(Provider::root(|_| {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(tokio::time::sleep(Duration::from_millis(10)))
        });
        Shared::new(2)
    }));
    injector.provide::<Svc>(Provider::root_async(|inj: Injector| async move {
        let members = inj.try_resolve_all_async::<usize>().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        Shared::new(Svc(members.len()))
    }));

    let (result, received) = mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .build()
            .unwrap();
        result
            .send(runtime.block_on(injector.try_resolve_async::<Svc>()))
            .unwrap();
    });

    let svc = received
        .recv_timeout(HANG_LIMIT)
        .expect("the run's wake-up was lost")
        .unwrap();
    assert_eq!(svc.0, 2);
}

#[test]
fn a_joiner_takes_over_the_run_of_a_dropped_driver() {
    let runs = Arc::new(AtomicUsize::new(0));
    let gates = vec![Latch::new(), Latch::new()];
    let injector = Injector::root();
    injector.provide::<Svc>(gated_provider(Scope::Root, runs.clone(), gates.clone()));

    within(async {
        let mut driver = Box::pin(injector.try_resolve_async::<Svc>());
        assert!(futures::poll!(&mut driver).is_pending());
        let mut joiner = Box::pin(injector.try_resolve_async::<Svc>());
        assert!(futures::poll!(&mut joiner).is_pending());
        assert_eq!(runs.load(Ordering::SeqCst), 1, "the joiner ran the factory");

        drop(driver);
        gates[1].open();
        let svc = joiner.await.unwrap();
        assert_eq!(svc.0, 2);
        assert_eq!(runs.load(Ordering::SeqCst), 2);
        assert_eq!(injector.inner.cells.in_flight(), 0);
        let cached = injector.try_resolve_async::<Svc>().await.unwrap();
        assert!(Shared::ptr_eq(&svc, &cached));
    });
}

struct Outer;
struct Inner(usize);

#[test]
fn concurrent_resolves_of_one_type_inside_a_factory_share_one_run() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<Inner>(Provider::root_async({
        let runs = runs.clone();
        move |_: Injector| {
            let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                tokio::task::yield_now().await;
                Shared::new(Inner(run))
            }
        }
    }));
    injector.provide::<Outer>(Provider::root_try_async(|inj: Injector| async move {
        let (first, second) = futures::join!(
            inj.try_resolve_async::<Inner>(),
            inj.try_resolve_async::<Inner>(),
        );
        let (first, second) = (first?, second?);
        assert!(
            Shared::ptr_eq(&first, &second),
            "two instances of a singleton"
        );
        Ok::<_, Error>(Shared::new(Outer))
    }));

    within(injector.try_resolve_async::<Outer>()).unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 1, "the factory ran twice");
}

#[test]
fn concurrent_resolves_sharing_a_dependency_inside_a_factory_share_its_run() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<Svc>(Provider::root_async({
        let runs = runs.clone();
        move |_: Injector| {
            let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                tokio::task::yield_now().await;
                Shared::new(Svc(run))
            }
        }
    }));
    let via_svc = |inj: Injector| async move {
        let svc = inj.try_resolve_async::<Svc>().await?;
        Ok::<_, Error>(Shared::new(Inner(svc.0)))
    };
    injector.provide::<Inner>(Provider::root_try_async(via_svc));
    injector.provide_named::<Inner>("other", Provider::root_try_async(via_svc));
    injector.provide::<Outer>(Provider::root_try_async(|inj: Injector| async move {
        let (first, second) = futures::join!(
            inj.try_resolve_async::<Inner>(),
            inj.try_resolve_named_async::<Inner>("other"),
        );
        let (first, second) = (first?, second?);
        assert_eq!(first.0, second.0, "the two branches saw different Svc runs");
        Ok::<_, Error>(Shared::new(Outer))
    }));

    within(injector.try_resolve_async::<Outer>()).unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 1, "Svc's factory ran twice");
}

trait Named: Send + Sync {
    fn name(&self) -> usize;
}

impl Named for Svc {
    fn name(&self) -> usize {
        self.0
    }
}

async fn resolve_through_a_borrowed_injector(injector: &Injector) -> usize {
    match injector.optional_resolve_async::<dyn Named>().await {
        Some(named) => named.name(),
        None => injector.resolve_async::<Svc>().await.0,
    }
}

#[test]
fn a_factory_may_await_a_resolve_through_a_borrowed_injector() {
    let injector = Injector::root();
    injector.provide::<dyn Named>(Provider::root_async(|_| async {
        Shared::new(Svc(1)) as Shared<dyn Named>
    }));
    injector.provide::<usize>(Provider::root_async(|resolver| async move {
        let first = resolver.resolve_async::<dyn Named>().await.name();
        Shared::new(first + resolve_through_a_borrowed_injector(&resolver).await)
    }));
    assert_eq!(*within(injector.resolve_async::<usize>()), 2);
}
