use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::timeout;

use crate::live::{Generational, LivePublisher};
use crate::{Completeness, Error, ErrorKind, Injector, LiveOutcome, LiveState, Provider, Shared};

const HANG: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct Db(&'static str);

trait Source: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &'static str;
}

#[derive(Debug)]
struct NamedSource(&'static str);

impl Source for NamedSource {
    fn name(&self) -> &'static str {
        self.0
    }
}

fn gated_db(gate: Arc<Notify>, runs: Arc<AtomicUsize>) -> Provider<Db> {
    Provider::root_async(move |_| {
        let gate = gate.clone();
        let runs = runs.clone();
        async move {
            runs.fetch_add(1, Ordering::SeqCst);
            gate.notified().await;
            Shared::new(Db("ready"))
        }
    })
}

fn gated_source(name: &'static str, gate: Arc<Notify>) -> Provider<dyn Source> {
    Provider::root_async(move |_| {
        let gate = gate.clone();
        async move {
            gate.notified().await;
            Shared::new(NamedSource(name)) as Shared<dyn Source>
        }
    })
}

async fn explode() -> Shared<Db> {
    panic!("disk unreadable")
}

fn error_kinds(error: &Error) -> Vec<ErrorKind> {
    std::iter::successors(Some(error as &(dyn std::error::Error + 'static)), |e| {
        e.source()
    })
    .filter_map(|e| e.downcast_ref::<Error>().map(|e| e.kind.clone()))
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn resolve_live_returns_pending_before_the_producer_finishes() {
    let gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(gate.clone(), Arc::new(AtomicUsize::new(0))));

    let live = injector.resolve_live::<Db>();
    assert!(matches!(live.state().value, LiveState::Pending));

    gate.notify_one();
    let db = timeout(HANG, live.ready()).await.unwrap().unwrap();
    assert_eq!(db.value.0, "ready");
    assert!(matches!(live.state().value, LiveState::Ready(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_starts_on_first_resolve_and_runs_once() {
    let gate = Arc::new(Notify::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(gate.clone(), runs.clone()));

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "registration started the producer"
    );

    let first = injector.resolve_live::<Db>();
    let second = injector.resolve_live::<Db>();
    let hard = tokio::spawn({
        let injector = injector.clone();
        async move { injector.try_resolve_async::<Db>().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    gate.notify_one();
    let from_first = timeout(HANG, first.ready()).await.unwrap().unwrap();
    let from_second = second.ready().await.unwrap();
    let from_hard = timeout(HANG, hard).await.unwrap().unwrap().unwrap();
    assert!(Shared::ptr_eq(&from_first.value, &from_second.value));
    assert!(Shared::ptr_eq(&from_first.value, &from_hard));
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn live_set_grows_as_members_become_ready() {
    let gates: Vec<Arc<Notify>> = (0..3).map(|_| Arc::new(Notify::new())).collect();
    let injector = Injector::root();
    for (name, gate) in ["org", "loro", "mcp"].into_iter().zip(&gates) {
        injector.provide_into_set::<dyn Source>(gated_source(name, gate.clone()));
    }

    let mut set = injector.resolve_all_live::<dyn Source>();
    assert_eq!(set.members().len(), 3);
    assert!(set.ready_members().is_empty());

    let mut seen = Vec::new();
    for index in [2, 0, 1] {
        gates[index].notify_one();
        timeout(HANG, set.changed()).await.unwrap();
        let names: Vec<&str> = set.ready_members().iter().map(|s| s.value.name()).collect();
        seen.push(names);
    }
    assert_eq!(
        seen,
        vec![vec!["mcp"], vec!["org", "mcp"], vec!["org", "loro", "mcp"]]
    );
    assert_eq!(
        timeout(HANG, set.complete()).await.unwrap().unwrap().len(),
        3
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_producer_reaches_every_observer() {
    let injector = Injector::root();
    injector.provide::<Db>(Provider::root_async(|_| explode()));
    injector.provide::<String>(Provider::root_async(|inj: Injector| async move {
        let db = inj.resolve_live::<Db>().ready().await.unwrap();
        Shared::new(db.value.0.to_string())
    }));
    injector.provide_into_set::<dyn Source>(Provider::root_async(|inj: Injector| async move {
        inj.resolve_live::<Db>().ready().await.unwrap();
        Shared::new(NamedSource("needs db")) as Shared<dyn Source>
    }));
    injector.provide_into_set::<dyn Source>(Provider::root(|_| {
        Shared::new(NamedSource("fine")) as Shared<dyn Source>
    }));

    let db = injector.resolve_live::<Db>();
    let error = timeout(HANG, db.ready()).await.unwrap().unwrap_err();
    assert_eq!(error.kind, ErrorKind::LiveProducerFailed);
    assert!(
        error.message.contains("disk unreadable"),
        "{}",
        error.message
    );
    assert!(matches!(db.state().value, LiveState::Failed(_)));

    let hard = timeout(HANG, injector.try_resolve_async::<Db>())
        .await
        .unwrap();
    assert!(hard.unwrap_err().message.contains("disk unreadable"));

    let dependent = injector.resolve_live::<String>();
    let error = timeout(HANG, dependent.ready()).await.unwrap().unwrap_err();
    assert!(
        error.message.contains("disk unreadable"),
        "{}",
        error.message
    );

    let set = injector.resolve_all_live::<dyn Source>();
    let error = timeout(HANG, set.complete()).await.unwrap().unwrap_err();
    assert!(
        error.message.contains("disk unreadable"),
        "{}",
        error.message
    );
    let fine: Vec<&str> = set.ready_members().iter().map(|s| s.value.name()).collect();
    assert_eq!(fine, vec!["fine"]);

    let hard_set = timeout(HANG, injector.try_resolve_all_async::<dyn Source>())
        .await
        .unwrap();
    assert!(hard_set.unwrap_err().message.contains("disk unreadable"));
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cancels_a_running_producer() {
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let injector = Injector::root();
    let flag = dropped.clone();
    injector.provide::<Db>(Provider::root_async(move |_| {
        let held = DropFlag(flag.clone());
        async move {
            let _held = held;
            std::future::pending::<()>().await;
            Shared::new(Db("never"))
        }
    }));

    let live = injector.resolve_live::<Db>();
    tokio::time::sleep(Duration::from_millis(20)).await;
    injector.shutdown_live();

    let error = timeout(HANG, live.ready()).await.unwrap().unwrap_err();
    assert_eq!(error.kind, ErrorKind::LiveProducerCancelled);
    assert!(
        dropped.load(Ordering::SeqCst),
        "the producer future outlived shutdown"
    );
    let hard = timeout(HANG, injector.try_resolve_async::<Db>())
        .await
        .expect("a hard resolve after shutdown hung");
    assert_eq!(hard.unwrap_err().kind, ErrorKind::LiveProducerCancelled);
}

#[tokio::test(flavor = "multi_thread")]
async fn producers_waiting_on_each_other_fail_instead_of_hanging() {
    #[derive(Debug)]
    struct A;
    struct B;
    let injector = Injector::root();
    injector.provide::<A>(Provider::root_async(|inj: Injector| async move {
        inj.resolve_live::<B>().ready().await.unwrap();
        Shared::new(A)
    }));
    injector.provide::<B>(Provider::root_async(|inj: Injector| async move {
        inj.try_resolve_async::<A>().await.unwrap();
        Shared::new(B)
    }));

    let error = timeout(HANG, injector.resolve_live::<A>().ready())
        .await
        .expect("a wait cycle between live producers hung")
        .unwrap_err();
    assert!(error.message.contains("Circular"), "{}", error.message);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cycle_through_a_spawned_live_run_is_a_circular_dependency() {
    struct A;
    struct B;
    let injector = Injector::root();
    injector.provide::<A>(Provider::root_try_async(|inj: Injector| async move {
        inj.try_resolve_live::<B>()?.ready().await?;
        Ok::<_, Error>(Shared::new(A))
    }));
    injector.provide::<B>(Provider::root_try_async(|inj: Injector| async move {
        inj.try_resolve_async::<A>().await?;
        Ok::<_, Error>(Shared::new(B))
    }));

    let error = timeout(HANG, injector.try_resolve_async::<A>())
        .await
        .expect("A waits on B's spawned run, which joins A's run: hung")
        .err()
        .expect("A -> live B -> A resolved");
    assert!(
        error_kinds(&error).contains(&ErrorKind::CircularDependency),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_edge_nobody_waits_on_breaks_a_cycle() {
    struct A;
    struct B;
    let injector = Injector::root();
    injector.provide::<A>(Provider::root_try_async(|inj: Injector| async move {
        inj.try_resolve_live::<B>()?;
        Ok::<_, Error>(Shared::new(A))
    }));
    injector.provide::<B>(Provider::root_try_async(|inj: Injector| async move {
        inj.try_resolve_async::<A>().await?;
        Ok::<_, Error>(Shared::new(B))
    }));

    timeout(HANG, injector.try_resolve_async::<A>())
        .await
        .unwrap()
        .unwrap();
    timeout(HANG, injector.resolve_live::<B>().ready())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn live_report_times_every_producer() {
    let gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(gate.clone(), Arc::new(AtomicUsize::new(0))));
    injector.provide::<String>(Provider::root_async(|_| async {
        tokio::time::sleep(Duration::from_millis(30)).await;
        Shared::new("slow".to_string())
    }));

    let db = injector.resolve_live::<Db>();
    let slow = injector.resolve_live::<String>();
    timeout(HANG, slow.ready()).await.unwrap().unwrap();

    let report = injector.live_report();
    let string_row = report
        .iter()
        .find(|row| row.type_name.ends_with("String"))
        .unwrap();
    assert!(matches!(string_row.outcome, LiveOutcome::Ready));
    assert!(string_row.elapsed >= Duration::from_millis(30));
    let db_row = report
        .iter()
        .find(|row| row.type_name.ends_with("Db"))
        .unwrap();
    assert!(matches!(db_row.outcome, LiveOutcome::Running));

    gate.notify_one();
    timeout(HANG, db.ready()).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn report_changed_follows_producers_without_polling() {
    let gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(gate.clone(), Arc::new(AtomicUsize::new(0))));

    let mut changes = injector.report_changed();
    let _db = injector.resolve_live::<Db>();
    let started = timeout(HANG, changes.changed()).await.unwrap();
    assert!(matches!(started[0].outcome, LiveOutcome::Running));

    gate.notify_one();
    let finished = timeout(HANG, changes.changed()).await.unwrap();
    assert!(matches!(finished[0].outcome, LiveOutcome::Ready));
}

#[test]
fn resolve_live_from_a_thread_without_a_runtime_uses_the_held_handle() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .unwrap();
    let injector = Injector::root_with_runtime(runtime.handle().clone());
    injector.provide::<Db>(Provider::root_async(|_| async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Shared::new(Db("from the held runtime"))
    }));

    let ui_thread = std::thread::spawn(move || {
        let live = injector.resolve_live::<Db>();
        futures::executor::block_on(live.ready()).unwrap();
        live.state().value
    });
    let state = ui_thread.join().unwrap();
    assert!(
        matches!(&state, LiveState::Ready(db) if db.0 == "from the held runtime"),
        "{state:?}"
    );
}

#[test]
fn resolve_live_without_any_runtime_is_an_error() {
    let injector = Injector::root();
    injector.provide::<Db>(Provider::root_async(|_| async { Shared::new(Db("never")) }));

    let error = std::thread::spawn(move || injector.try_resolve_live::<Db>().err())
        .join()
        .unwrap()
        .expect("resolved live without a runtime");
    assert_eq!(error.kind, ErrorKind::LiveRuntimeMissing);
}

#[test]
fn a_cached_value_resolves_live_without_any_runtime() {
    let injector = Injector::root();
    injector.provide::<Db>(Provider::root(|_| Shared::new(Db("cached"))));
    let hard = injector.try_resolve::<Db>().unwrap();

    let live = injector
        .try_resolve_live::<Db>()
        .expect("a cached value needs no producer, so no runtime");
    assert!(matches!(live.state().value, LiveState::Ready(db) if Shared::ptr_eq(&db, &hard)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cached_value_gives_a_ready_cell_without_a_producer() {
    let injector = Injector::root();
    injector.provide::<Db>(Provider::root_async(|_| async {
        Shared::new(Db("cached"))
    }));
    let hard = injector.try_resolve_async::<Db>().await.unwrap();

    let live = injector.resolve_live::<Db>();
    assert!(matches!(live.state().value, LiveState::Ready(db) if Shared::ptr_eq(&db, &hard)));
    assert!(injector.live_report().is_empty());
}

#[test]
fn a_transient_provider_cannot_be_resolved_live() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let injector = Injector::root_with_runtime(runtime.handle().clone());
    injector.provide::<Db>(Provider::transient_async(|_| async {
        Shared::new(Db("fresh"))
    }));
    let error = injector.try_resolve_live::<Db>().err().unwrap();
    assert_eq!(error.kind, ErrorKind::LiveRequiresCachedScope);
}

/// Fails on run 1; run 2 waits for `gate`, then succeeds.
fn flaky_db(gate: Arc<Notify>, runs: Arc<AtomicUsize>) -> Provider<Db> {
    Provider::root_try_async(move |_| {
        let gate = gate.clone();
        let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
        async move {
            if run == 1 {
                return Err(std::io::Error::other("connection refused"));
            }
            gate.notified().await;
            Ok::<_, std::io::Error>(Shared::new(Db("reconnected")))
        }
    })
}

fn label<T: ?Sized>(state: &Generational<LiveState<T>>) -> (u64, &'static str) {
    let name = match &state.value {
        LiveState::Pending => "pending",
        LiveState::Partial(_) => "partial",
        LiveState::Ready(_) => "ready",
        LiveState::Failed(_) => "failed",
        _ => unreachable!("no other live state exists"),
    };
    (state.generation.get(), name)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_after_a_failure_publishes_the_next_generation() {
    let gate = Arc::new(Notify::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<Db>(flaky_db(gate.clone(), runs.clone()));

    let live = injector.resolve_live::<Db>();
    let mut observer = live.clone();
    let mut seen = vec![label(&live.state())];
    seen.push(label(&timeout(HANG, observer.changed()).await.unwrap()));
    let failed = live.state().generation;
    assert!(live.is_current(failed));

    let restarted = live.restart().unwrap();
    assert_eq!(restarted, failed.next());
    seen.push(label(&timeout(HANG, observer.changed()).await.unwrap()));
    let hard = tokio::spawn({
        let injector = injector.clone();
        async move { injector.try_resolve_async::<Db>().await }
    });
    gate.notify_one();
    seen.push(label(&timeout(HANG, observer.changed()).await.unwrap()));

    assert_eq!(
        seen,
        vec![(1, "pending"), (1, "failed"), (2, "pending"), (2, "ready")]
    );
    let ready = timeout(HANG, live.ready()).await.unwrap().unwrap();
    assert_eq!(ready.generation, restarted);
    assert_eq!(ready.value.0, "reconnected");
    assert!(!live.is_current(failed));
    assert!(live.is_current(restarted));
    let hard = timeout(HANG, hard).await.unwrap().unwrap().unwrap();
    assert!(Shared::ptr_eq(&hard, &ready.value));
    assert_eq!(runs.load(Ordering::SeqCst), 2);

    let report: Vec<(u64, bool)> = injector
        .live_report()
        .iter()
        .map(|row| {
            (
                row.generation.get(),
                matches!(row.outcome, LiveOutcome::Ready),
            )
        })
        .collect();
    assert_eq!(report, vec![(1, false), (2, true)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_while_the_generation_runs_is_refused() {
    let gate = Arc::new(Notify::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(gate.clone(), runs.clone()));

    let live = injector.resolve_live::<Db>();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let error = live.restart().expect_err("restarted a running generation");
    assert_eq!(error.kind, ErrorKind::LiveRestartWhileRunning);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "two producers ran for one cell"
    );
    assert_eq!(injector.live_report().len(), 1);

    gate.notify_one();
    let first = timeout(HANG, live.ready()).await.unwrap().unwrap();
    assert_eq!(first.generation.get(), 1);

    let second = live.restart().expect("a ready generation is terminal");
    gate.notify_one();
    let ready = timeout(HANG, live.ready()).await.unwrap().unwrap();
    assert_eq!(ready.generation, second);
    assert!(!Shared::ptr_eq(&first.value, &ready.value));
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cache_holds_the_last_ready_generation() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<Db>(Provider::root_try_async({
        let runs = runs.clone();
        move |_| {
            let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                match run {
                    1 => Ok(Shared::new(Db("first"))),
                    2 => Err(std::io::Error::other("connection lost")),
                    _ => Ok(Shared::new(Db("third"))),
                }
            }
        }
    }));

    let live = injector.resolve_live::<Db>();
    let first = timeout(HANG, live.ready()).await.unwrap().unwrap();

    live.restart().unwrap();
    let error = timeout(HANG, live.ready()).await.unwrap().unwrap_err();
    assert!(
        error.message.contains("connection lost"),
        "{}",
        error.message
    );
    let cached = injector.try_resolve::<Db>().unwrap();
    assert!(Shared::ptr_eq(&cached, &first.value));

    live.restart().unwrap();
    let third = timeout(HANG, live.ready()).await.unwrap().unwrap();
    assert_eq!(third.generation.get(), 3);
    let cached = injector.try_resolve::<Db>().unwrap();
    assert!(Shared::ptr_eq(&cached, &third.value));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_set_slot_restarts_in_place() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(Provider::root(|_| {
        Shared::new(NamedSource("org")) as Shared<dyn Source>
    }));
    injector.provide_into_set::<dyn Source>(Provider::root_try_async({
        let runs = runs.clone();
        move |_| {
            let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if run == 1 {
                    return Err(std::io::Error::other("mcp unreachable"));
                }
                Ok::<_, std::io::Error>(Shared::new(NamedSource("mcp")) as Shared<dyn Source>)
            }
        }
    }));

    let set = injector.resolve_all_live::<dyn Source>();
    timeout(HANG, set.complete()).await.unwrap().unwrap_err();
    assert_eq!(
        set.members().iter().map(label).collect::<Vec<_>>(),
        vec![(1, "ready"), (1, "failed")]
    );

    assert_eq!(set.restart(1).unwrap().get(), 2);
    let members = timeout(HANG, set.complete()).await.unwrap().unwrap();
    let names: Vec<(u64, &str)> = members
        .iter()
        .map(|member| (member.generation.get(), member.value.name()))
        .collect();
    assert_eq!(names, vec![(1, "org"), (2, "mcp")]);
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_live_ends_live_production_for_good() {
    let injector = Injector::root();
    injector.provide::<Db>(Provider::root_async(|_| async {
        std::future::pending::<()>().await;
        Shared::new(Db("never"))
    }));
    injector.provide::<String>(Provider::root_async(|_| async {
        Shared::new("never started".to_string())
    }));
    injector.provide::<u32>(Provider::root(|_| Shared::new(7)));
    injector.try_resolve::<u32>().unwrap();

    let live = injector.resolve_live::<Db>();
    injector.shutdown_live();
    let error = timeout(HANG, live.ready()).await.unwrap().unwrap_err();
    assert_eq!(error.kind, ErrorKind::LiveProducerCancelled);

    let error = live.restart().expect_err("restarted after shutdown_live");
    assert_eq!(error.kind, ErrorKind::LiveProducerCancelled);
    assert_eq!(label(&live.state()), (1, "failed"));
    let error = injector
        .try_resolve_live::<String>()
        .err()
        .expect("started a producer after shutdown_live");
    assert_eq!(error.kind, ErrorKind::LiveProducerCancelled);
    let hard = timeout(HANG, injector.try_resolve_async::<String>())
        .await
        .unwrap()
        .expect("a refused live resolve broke the hard resolve");
    assert_eq!(*hard, "never started");
    assert_eq!(injector.live_report().len(), 1);
    let cached = injector.try_resolve_live::<u32>().unwrap();
    assert_eq!(label(&cached.state()), (1, "ready"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_waiter_that_missed_a_generation_waits_for_the_next() {
    let gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide::<Db>(flaky_db(gate.clone(), Arc::new(AtomicUsize::new(0))));

    let live = injector.resolve_live::<Db>();
    let mut waiter = Box::pin(live.ready());
    assert!(futures::poll!(&mut waiter).is_pending());

    let mut observer = live.clone();
    assert_eq!(
        label(&timeout(HANG, observer.changed()).await.unwrap()),
        (1, "failed")
    );
    live.restart().unwrap();
    gate.notify_one();

    let ready = timeout(HANG, waiter).await.unwrap().unwrap();
    assert_eq!(ready.generation.get(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hard_resolve_during_a_restart_returns_the_last_ready_generation() {
    let gate = Arc::new(Notify::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(gate.clone(), runs.clone()));

    let live = injector.resolve_live::<Db>();
    gate.notify_one();
    let first = timeout(HANG, live.ready()).await.unwrap().unwrap();
    live.restart().unwrap();

    let hard = timeout(HANG, injector.try_resolve_async::<Db>())
        .await
        .expect("a hard resolve waited for the restarted generation")
        .unwrap();
    assert!(Shared::ptr_eq(&hard, &first.value));
    assert!(Shared::ptr_eq(
        &injector.try_resolve::<Db>().unwrap(),
        &first.value
    ));
    assert_eq!(label(&live.state()), (2, "pending"));

    gate.notify_one();
    let second = timeout(HANG, live.ready()).await.unwrap().unwrap();
    let hard = injector.try_resolve_async::<Db>().await.unwrap();
    assert!(Shared::ptr_eq(&hard, &second.value));
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_set_restart_of_a_missing_slot_is_an_error() {
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(Provider::root(|_| {
        Shared::new(NamedSource("org")) as Shared<dyn Source>
    }));
    let set = injector.resolve_all_live::<dyn Source>();

    let error = set.restart(7).expect_err("restarted a slot the set lacks");
    assert_eq!(error.kind, ErrorKind::LiveSlotOutOfRange);
    assert!(
        error.message.contains("slot 7") && error.message.contains("1 members"),
        "{}",
        error.message
    );
}

/// Publishes two pages as partials, then the complete value; each step waits
/// for `gate`.
fn paged_db(gate: Arc<Notify>) -> Provider<Db> {
    Provider::root_live(move |_, publisher: LivePublisher<Db>| {
        let gate = gate.clone();
        async move {
            for page in ["page 1", "page 2"] {
                gate.notified().await;
                publisher.partial(Shared::new(Db(page)));
            }
            gate.notified().await;
            Ok::<_, std::io::Error>(Shared::new(Db("complete")))
        }
    })
}

type Seen = (
    u64,
    &'static str,
    Option<&'static str>,
    Option<Completeness>,
);

fn seen(state: &Generational<LiveState<Db>>) -> Seen {
    let (generation, name) = label(state);
    (
        generation,
        name,
        state.value.value().map(|db| db.0),
        state.value.completeness(),
    )
}

fn panic_text(panic: Box<dyn std::any::Any + Send>) -> String {
    match panic.downcast::<String>() {
        Ok(text) => *text,
        Err(panic) => panic
            .downcast::<&'static str>()
            .map(|text| text.to_string())
            .expect("a panic with a text payload"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_observer_sees_every_partial_then_the_complete_value() {
    let gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide::<Db>(paged_db(gate.clone()));

    let live = injector.resolve_live::<Db>();
    let mut observer = live.clone();
    let mut states = vec![seen(&live.state())];
    for _ in 0..3 {
        gate.notify_one();
        states.push(seen(&timeout(HANG, observer.changed()).await.unwrap()));
    }
    assert_eq!(
        states,
        vec![
            (1, "pending", None, None),
            (1, "partial", Some("page 1"), Some(Completeness::Partial)),
            (1, "partial", Some("page 2"), Some(Completeness::Partial)),
            (1, "ready", Some("complete"), Some(Completeness::Complete)),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn neither_ready_nor_a_hard_resolve_returns_a_partial() {
    let gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide::<Db>(paged_db(gate.clone()));

    let live = injector.resolve_live::<Db>();
    let mut observer = live.clone();
    let mut waiter = Box::pin(live.ready());
    let mut hard = Box::pin(injector.try_resolve_async::<Db>());
    for _ in 0..2 {
        gate.notify_one();
        let state = timeout(HANG, observer.changed()).await.unwrap();
        assert_eq!(label(&state).1, "partial");
        assert!(
            futures::poll!(&mut waiter).is_pending(),
            "ready() returned a partial"
        );
        assert!(
            futures::poll!(&mut hard).is_pending(),
            "a hard resolve returned a partial"
        );
    }

    gate.notify_one();
    let ready = timeout(HANG, waiter).await.unwrap().unwrap();
    assert_eq!((ready.generation.get(), ready.value.0), (1, "complete"));
    let hard = timeout(HANG, hard).await.unwrap().unwrap();
    assert!(Shared::ptr_eq(&hard, &ready.value));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hard_resolve_without_a_live_handle_returns_the_complete_value() {
    let injector = Injector::root();
    injector.provide::<Db>(Provider::root_live(
        |_, publisher: LivePublisher<Db>| async move {
            publisher.partial(Shared::new(Db("page 1")));
            Ok::<_, std::io::Error>(Shared::new(Db("complete")))
        },
    ));

    let hard = timeout(HANG, injector.try_resolve_async::<Db>())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hard.0, "complete");
    let live = injector.resolve_live::<Db>();
    assert_eq!(seen(&live.state()).2, Some("complete"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_set_member_publishes_partials_in_its_slot() {
    let gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(Provider::root(|_| {
        Shared::new(NamedSource("org")) as Shared<dyn Source>
    }));
    injector.provide_into_set::<dyn Source>(Provider::root_live({
        let gate = gate.clone();
        move |_, publisher: LivePublisher<dyn Source>| {
            let gate = gate.clone();
            async move {
                publisher.partial(Shared::new(NamedSource("mcp page 1")) as Shared<dyn Source>);
                gate.notified().await;
                Ok::<_, std::io::Error>(Shared::new(NamedSource("mcp")) as Shared<dyn Source>)
            }
        }
    }));

    let mut set = injector.resolve_all_live::<dyn Source>();
    let partial = timeout(HANG, async {
        loop {
            let members = set.changed().await;
            if let LiveState::Partial(source) = &members[1].value {
                return source.name();
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(partial, "mcp page 1");
    assert_eq!(set.ready_members().len(), 1);

    gate.notify_one();
    let names: Vec<&str> = timeout(HANG, set.complete())
        .await
        .unwrap()
        .unwrap()
        .iter()
        .map(|member| member.value.name())
        .collect();
    assert_eq!(names, vec!["org", "mcp"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_publisher_cannot_write_into_the_next_generation() {
    let gate = Arc::new(Notify::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let escaped = Arc::new(std::sync::Mutex::new(Vec::new()));
    let injector = Injector::root();
    injector.provide::<Db>(Provider::root_live({
        let (gate, runs, escaped) = (gate.clone(), runs.clone(), escaped.clone());
        move |_, publisher: LivePublisher<Db>| {
            let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
            let (gate, escaped) = (gate.clone(), escaped.clone());
            async move {
                if run == 1 {
                    escaped.lock().unwrap().push(publisher);
                    return Ok::<_, std::io::Error>(Shared::new(Db("first")));
                }
                gate.notified().await;
                Ok(Shared::new(Db("second")))
            }
        }
    }));

    let live = injector.resolve_live::<Db>();
    timeout(HANG, live.ready()).await.unwrap().unwrap();
    live.restart().unwrap();
    assert_eq!(label(&live.state()), (2, "pending"));

    let stale = escaped.lock().unwrap().pop().unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        stale.partial(Shared::new(Db("stale")))
    }))
    .expect_err("a generation-1 publisher published into generation 2");
    let text = panic_text(panic);
    assert!(text.contains("generation 1"), "{text}");
    assert_eq!(label(&live.state()), (2, "pending"));

    gate.notify_one();
    let ready = timeout(HANG, live.ready()).await.unwrap().unwrap();
    assert_eq!((ready.generation.get(), ready.value.0), (2, "second"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publish_after_the_producer_ended_is_a_programming_error() {
    let escaped = Arc::new(std::sync::Mutex::new(Vec::new()));
    let escaping_db = |forever: bool| {
        let escaped = escaped.clone();
        Provider::root_live(move |_, publisher: LivePublisher<Db>| {
            let escaped = escaped.clone();
            async move {
                escaped.lock().unwrap().push(publisher);
                if forever {
                    std::future::pending::<()>().await;
                }
                Ok::<_, std::io::Error>(Shared::new(Db("complete")))
            }
        })
    };
    let finished = Injector::root();
    finished.provide::<Db>(escaping_db(false));
    let cancelled = Injector::root();
    cancelled.provide::<Db>(escaping_db(true));

    let ready = finished.resolve_live::<Db>();
    timeout(HANG, ready.ready()).await.unwrap().unwrap();
    let running = cancelled.resolve_live::<Db>();
    timeout(HANG, async {
        while escaped.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancelled.shutdown_live();
    let error = timeout(HANG, running.ready()).await.unwrap().unwrap_err();
    assert_eq!(error.kind, ErrorKind::LiveProducerCancelled);

    for (publisher, live) in escaped.lock().unwrap().drain(..).zip([&ready, &running]) {
        let before = label(&live.state());
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            publisher.partial(Shared::new(Db("late")))
        }))
        .expect_err("published after the producer ended");
        assert!(panic_text(panic).contains("ended"));
        assert_eq!(label(&live.state()), before);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn decorators_wrap_partials_and_values_of_a_live_provider() {
    let gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide::<String>(
        Provider::root_live({
            let gate = gate.clone();
            move |_, publisher: LivePublisher<String>| {
                let gate = gate.clone();
                async move {
                    publisher.partial(Shared::new("page".to_string()));
                    gate.notified().await;
                    Ok::<_, std::io::Error>(Shared::new("complete".to_string()))
                }
            }
        })
        .with_decorator(|inner| Shared::new(format!("[{inner}]")))
        .with_decorator(|inner| Shared::new(format!("({inner})"))),
    );

    let mut live = injector.resolve_live::<String>();
    let partial = timeout(HANG, async {
        loop {
            if let LiveState::Partial(value) = live.state().value {
                return value;
            }
            live.changed().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(partial.as_str(), "([page])");

    gate.notify_one();
    let ready = timeout(HANG, live.ready()).await.unwrap().unwrap();
    assert_eq!(ready.value.as_str(), "([complete])");
    let hard = injector.try_resolve_async::<String>().await.unwrap();
    assert_eq!(hard.as_str(), "([complete])");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_decorator_wraps_a_live_provider_resolved_hard() {
    let injector = Injector::root();
    injector.provide::<String>(
        Provider::root_live(|_, _: LivePublisher<String>| async {
            Ok::<_, std::io::Error>(Shared::new("complete".to_string()))
        })
        .with_decorator(|inner| Shared::new(format!("[{inner}]"))),
    );

    let hard = injector.try_resolve_async::<String>().await.unwrap();
    assert_eq!(hard.as_str(), "[complete]");
}

fn ready_source(name: &'static str) -> Provider<dyn Source> {
    Provider::root_async(
        move |_| async move { Shared::new(NamedSource(name)) as Shared<dyn Source> },
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_provided_after_the_set_resolves_gets_a_slot_and_starts() {
    let gates: Vec<Arc<Notify>> = (0..3).map(|_| Arc::new(Notify::new())).collect();
    let injector = Injector::root();
    for (name, gate) in ["org", "loro"].into_iter().zip(&gates) {
        injector.provide_into_set::<dyn Source>(gated_source(name, gate.clone()));
    }
    let mut set = injector.resolve_all_live::<dyn Source>();
    assert_eq!(set.members().len(), 2);

    injector.provide_into_set::<dyn Source>(gated_source("mcp", gates[2].clone()));
    let members = timeout(HANG, set.changed())
        .await
        .expect("the subscriber did not see the new slot");
    assert_eq!(
        members.iter().map(label).collect::<Vec<_>>(),
        vec![(1, "pending"); 3]
    );
    assert_eq!(set.members().len(), 3);

    gates[2].notify_one();
    let members = timeout(HANG, set.changed()).await.unwrap();
    assert_eq!(
        members.iter().map(label).collect::<Vec<_>>(),
        vec![(1, "pending"), (1, "pending"), (1, "ready")]
    );
    let names: Vec<&str> = set.ready_members().iter().map(|s| s.value.name()).collect();
    assert_eq!(names, vec!["mcp"]);
    assert!(
        injector
            .live_report()
            .iter()
            .any(|timing| timing.member == Some(2))
    );

    gates[0].notify_one();
    gates[1].notify_one();
    let names: Vec<&str> = timeout(HANG, set.complete())
        .await
        .unwrap()
        .unwrap()
        .iter()
        .map(|member| member.value.name())
        .collect();
    assert_eq!(names, vec!["org", "loro", "mcp"]);
    let hard = injector
        .try_resolve_all_async::<dyn Source>()
        .await
        .unwrap();
    assert_eq!(hard.len(), 3);
    assert_eq!(injector.resolve_all_live::<dyn Source>().members().len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_slot_restarts_in_place() {
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(ready_source("org"));
    let set = injector.resolve_all_live::<dyn Source>();
    injector.provide_into_set::<dyn Source>(Provider::root_try_async({
        let runs = runs.clone();
        move |_| {
            let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if run == 1 {
                    return Err(std::io::Error::other("mcp unreachable"));
                }
                Ok::<_, std::io::Error>(Shared::new(NamedSource("mcp")) as Shared<dyn Source>)
            }
        }
    }));
    timeout(HANG, set.complete()).await.unwrap().unwrap_err();
    assert_eq!(
        set.members().iter().map(label).collect::<Vec<_>>(),
        vec![(1, "ready"), (1, "failed")]
    );

    assert_eq!(set.restart(1).unwrap().get(), 2);
    let members = timeout(HANG, set.complete()).await.unwrap().unwrap();
    let names: Vec<(u64, &str)> = members
        .iter()
        .map(|member| (member.generation.get(), member.value.name()))
        .collect();
    assert_eq!(names, vec![(1, "org"), (2, "mcp")]);
}

#[tokio::test(flavor = "multi_thread")]
async fn complete_covers_the_slots_present_when_it_is_called() {
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(ready_source("org"));
    let set = injector.resolve_all_live::<dyn Source>();
    let complete = set.complete();

    injector.provide_into_set::<dyn Source>(gated_source("mcp", Arc::new(Notify::new())));
    assert_eq!(timeout(HANG, complete).await.unwrap().unwrap().len(), 1);
    assert!(
        timeout(Duration::from_millis(50), set.complete())
            .await
            .is_err(),
        "complete() returned before the late member was terminal"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_provided_after_shutdown_live_gets_a_cancelled_slot() {
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(ready_source("org"));
    let mut set = injector.resolve_all_live::<dyn Source>();
    timeout(HANG, set.complete()).await.unwrap().unwrap();
    injector.shutdown_live();
    let producers = injector.live_report().len();

    injector.provide_into_set::<dyn Source>(ready_source("mcp"));
    let members = timeout(HANG, async {
        loop {
            let members = set.changed().await;
            if members.len() == 2 {
                return members;
            }
        }
    })
    .await
    .expect("the subscriber did not see the new slot");
    assert_eq!(
        members.iter().map(label).collect::<Vec<_>>(),
        vec![(1, "ready"), (1, "failed")]
    );
    let LiveState::Failed(error) = &members[1].value else {
        unreachable!("checked above")
    };
    assert_eq!(error.kind, ErrorKind::LiveProducerCancelled);
    let error = set.restart(1).expect_err("restarted after shutdown_live");
    assert_eq!(error.kind, ErrorKind::LiveProducerCancelled);
    assert_eq!(injector.live_report().len(), producers);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_provided_after_shutdown_live_resolves_hard_as_without_a_live_set() {
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(ready_source("org"));
    let set = injector.resolve_all_live::<dyn Source>();
    timeout(HANG, set.complete()).await.unwrap().unwrap();
    injector.shutdown_live();
    injector.provide_into_set::<dyn Source>(ready_source("mcp"));
    assert_eq!(label(&set.members()[1]), (1, "failed"));

    for _ in 0..2 {
        let hard = timeout(HANG, injector.try_resolve_all_async::<dyn Source>())
            .await
            .expect("a hard resolve after shutdown hung")
            .expect("a member added after shutdown_live broke the hard set resolve");
        let names: Vec<&str> = hard.iter().map(|source| source.name()).collect();
        assert_eq!(names, vec!["org", "mcp"]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_live_set_grows_from_its_first_member() {
    let injector = Injector::root();
    let mut set = injector
        .try_resolve_all_live::<dyn Source>()
        .expect("a live set of no members was refused");
    assert!(set.members().is_empty());
    let complete = timeout(HANG, set.complete())
        .await
        .expect("complete() on an empty set waited")
        .unwrap();
    assert!(complete.is_empty());
    assert!(
        timeout(Duration::from_millis(50), set.changed())
            .await
            .is_err(),
        "changed() on an empty set returned without a new slot"
    );
    let hard = injector
        .try_resolve_all_async::<dyn Source>()
        .await
        .expect_err("a hard resolve of no members succeeded");
    assert_eq!(hard.kind, ErrorKind::ServiceNotProvided);

    injector.provide_into_set::<dyn Source>(ready_source("mcp"));
    let members = timeout(HANG, set.changed())
        .await
        .expect("the subscriber did not see the first slot");
    assert_eq!(members.len(), 1);
    let names: Vec<&str> = timeout(HANG, set.complete())
        .await
        .unwrap()
        .unwrap()
        .iter()
        .map(|member| member.value.name())
        .collect();
    assert_eq!(names, vec!["mcp"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transient_member_cannot_join_an_observed_set() {
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(ready_source("org"));
    let set = injector.resolve_all_live::<dyn Source>();

    let error = injector
        .try_provide_into_set::<dyn Source>(Provider::transient(|_| {
            Shared::new(NamedSource("scratch")) as Shared<dyn Source>
        }))
        .expect_err("a transient member joined a live set");
    assert_eq!(error.kind, ErrorKind::LiveRequiresCachedScope);
    assert_eq!(set.members().len(), 1);
    let hard = injector
        .try_resolve_all_async::<dyn Source>()
        .await
        .unwrap();
    assert_eq!(hard.len(), 1);
}

#[test]
fn concurrent_registrations_while_sets_resolve_and_follow_lose_no_slot() {
    const REGISTRARS: usize = 4;
    const PER_REGISTRAR: usize = 32;
    const RESOLVERS: usize = 4;
    const SCOPES_PER_RESOLVER: usize = 8;
    const MEMBERS: usize = 1 + REGISTRARS * PER_REGISTRAR;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let injector = Injector::root_with_runtime(runtime.handle().clone());
    injector.provide_into_set::<dyn Source>(ready_source("first"));
    let mut followed = injector.resolve_all_live::<dyn Source>();
    let follower = runtime.spawn(async move {
        loop {
            let members = followed.changed().await;
            let ready = members
                .iter()
                .filter(|member| matches!(member.value, LiveState::Ready(_)))
                .count();
            if ready == MEMBERS {
                return members.len();
            }
        }
    });

    let start = Arc::new(std::sync::Barrier::new(REGISTRARS + RESOLVERS));
    let registrars: Vec<_> = (0..REGISTRARS)
        .map(|registrar| {
            let injector = injector.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                for index in 0..PER_REGISTRAR {
                    let name: &'static str =
                        Box::leak(format!("member {registrar}.{index}").into_boxed_str());
                    injector.provide_into_set::<dyn Source>(ready_source(name));
                }
            })
        })
        .collect();
    let resolvers: Vec<_> = (0..RESOLVERS)
        .map(|_| {
            let injector = injector.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                (0..SCOPES_PER_RESOLVER)
                    .map(|_| {
                        let scope = injector.create_scope();
                        let set = scope.resolve_all_live::<dyn Source>();
                        (scope, set)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    for registrar in registrars {
        registrar.join().unwrap();
    }
    let sets: Vec<_> = resolvers
        .into_iter()
        .flat_map(|resolver| resolver.join().unwrap())
        .collect();

    for (_scope, set) in &sets {
        let names: std::collections::HashSet<_> = runtime
            .block_on(async { timeout(HANG, set.complete()).await })
            .expect("a set member never became ready")
            .unwrap()
            .iter()
            .map(|member| member.value.name())
            .collect();
        assert_eq!(set.members().len(), MEMBERS);
        assert_eq!(names.len(), MEMBERS, "a slot is missing or duplicated");
    }
    let followed_len = runtime
        .block_on(async { timeout(HANG, follower).await })
        .expect("the follower never saw every member ready")
        .unwrap();
    assert_eq!(followed_len, MEMBERS);
    let hard = runtime
        .block_on(injector.try_resolve_all_async::<dyn Source>())
        .unwrap();
    assert_eq!(hard.len(), MEMBERS);
}

/// Waits on `X` once `gate` opens, then joins its set as `name`.
fn waits_on<X: Send + Sync + 'static>(
    name: &'static str,
    gate: Arc<Notify>,
) -> Provider<dyn Source> {
    Provider::root_try_async(move |inj: Injector| {
        let gate = gate.clone();
        async move {
            gate.notified().await;
            inj.try_resolve_live::<X>()?.ready().await?;
            Ok::<_, Error>(Shared::new(NamedSource(name)) as Shared<dyn Source>)
        }
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn completing_a_set_whose_member_waits_on_the_caller_is_a_cycle() {
    #[derive(Debug)]
    struct X;
    let open = Arc::new(Notify::new());
    open.notify_one();
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(waits_on::<X>("m", open));
    injector.provide::<X>(Provider::root_try_async(|inj: Injector| async move {
        inj.resolve_all_live::<dyn Source>().complete().await?;
        Ok::<_, Error>(Shared::new(X))
    }));

    let error = timeout(HANG, injector.resolve_live::<X>().ready())
        .await
        .expect("X awaits complete() of a set whose member waits on X: hung")
        .unwrap_err();
    assert!(
        error_kinds(&error).contains(&ErrorKind::CircularDependency),
        "{error}"
    );
    assert_eq!(injector.recorded_waits(), vec![]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cycle_refused_on_a_member_fails_complete_while_other_members_still_run() {
    #[derive(Debug)]
    struct X;
    let org = Arc::new(Notify::new());
    let m = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(gated_source("org", org.clone()));
    injector.provide_into_set::<dyn Source>(waits_on::<X>("m", m.clone()));
    injector.provide::<X>(Provider::root_try_async(move |inj: Injector| {
        let m = m.clone();
        async move {
            let mut complete = Box::pin(inj.resolve_all_live::<dyn Source>().complete());
            assert!(futures::poll!(&mut complete).is_pending());
            m.notify_one();
            complete.await?;
            Ok::<_, Error>(Shared::new(X))
        }
    }));

    let set = injector.resolve_all_live::<dyn Source>();
    let error = timeout(HANG, injector.resolve_live::<X>().ready())
        .await
        .expect("X completes a set whose member m waits on X, while org runs: hung")
        .unwrap_err();
    let cycle = std::iter::successors(Some(&error as &(dyn std::error::Error + 'static)), |e| {
        e.source()
    })
    .filter_map(|e| e.downcast_ref::<Error>())
    .find(|e| e.kind == ErrorKind::CircularDependency)
    .unwrap_or_else(|| panic!("X failed without a cycle: {error}"));
    assert!(
        cycle.message.contains(std::any::type_name::<X>())
            && cycle.message.contains(std::any::type_name::<dyn Source>()),
        "{cycle}"
    );
    assert_eq!(label(&set.members()[0]), (1, "pending"));
}

/// Slot 0 ends generation 1 while `complete()` waits on slot 1, then
/// restarts; `poll_between` polls `complete()` between the end and the
/// restart.
async fn complete_across_a_restart(poll_between: bool) -> Vec<(u64, &'static str)> {
    let first = Arc::new(Notify::new());
    let blocker = Arc::new(Notify::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(Provider::root_async({
        let first = first.clone();
        move |_| {
            let first = first.clone();
            let run = runs.fetch_add(1, Ordering::SeqCst);
            async move {
                if run == 0 {
                    first.notified().await;
                    Shared::new(NamedSource("gen1")) as Shared<dyn Source>
                } else {
                    std::future::pending().await
                }
            }
        }
    }));
    injector.provide_into_set::<dyn Source>(gated_source("blocker", blocker.clone()));
    let mut set = injector.resolve_all_live::<dyn Source>();
    let mut complete = Box::pin(set.complete());
    assert!(futures::poll!(&mut complete).is_pending());

    first.notify_one();
    timeout(HANG, async {
        while label(&set.changed().await[0]) != (1, "ready") {}
    })
    .await
    .expect("slot 0 never ended generation 1");
    if poll_between {
        assert!(futures::poll!(&mut complete).is_pending());
    }
    assert_eq!(set.restart(0).unwrap().get(), 2);
    assert_eq!(label(&set.members()[0]), (2, "pending"));
    blocker.notify_one();

    timeout(HANG, complete)
        .await
        .expect("complete() waited for a generation started after the call")
        .unwrap()
        .iter()
        .map(|member| (member.generation.get(), member.value.name()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn complete_returns_the_generation_each_member_had_at_the_call() {
    assert_eq!(
        complete_across_a_restart(false).await,
        vec![(1, "gen1"), (1, "blocker")]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn complete_polled_before_a_restart_returns_the_same_generation() {
    assert_eq!(
        complete_across_a_restart(true).await,
        vec![(1, "gen1"), (1, "blocker")]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_set_completes_at_once_without_a_wait() {
    struct Probe {
        completed_at_once: Option<usize>,
        waits_before: Vec<(&'static str, &'static str)>,
        waits_after: Vec<(&'static str, &'static str)>,
    }
    let injector = Injector::root();
    injector.provide::<Probe>(Provider::root_try_async(|inj: Injector| async move {
        let waits_before = inj.recorded_waits();
        let completed_at_once =
            futures::FutureExt::now_or_never(inj.resolve_all_live::<dyn Source>().complete())
                .transpose()?
                .map(|members| members.len());
        Ok::<_, Error>(Shared::new(Probe {
            completed_at_once,
            waits_before,
            waits_after: inj.recorded_waits(),
        }))
    }));

    let probe = timeout(HANG, injector.resolve_live::<Probe>().ready())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(probe.value.completed_at_once, Some(0));
    assert_eq!(probe.value.waits_after, probe.value.waits_before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_complete_leaves_no_wait_behind() {
    struct X {
        waits_while_pending: Vec<(&'static str, &'static str)>,
        waits_after_drop: Vec<(&'static str, &'static str)>,
    }
    let m = Arc::new(Notify::new());
    let x_gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(waits_on::<X>("m", m.clone()));
    injector.provide::<X>(Provider::root_try_async({
        let x_gate = x_gate.clone();
        move |inj: Injector| {
            let m = m.clone();
            let x_gate = x_gate.clone();
            async move {
                let mut complete = Box::pin(inj.resolve_all_live::<dyn Source>().complete());
                assert!(futures::poll!(&mut complete).is_pending());
                let waits_while_pending = inj.recorded_waits();
                drop(complete);
                let waits_after_drop = inj.recorded_waits();
                m.notify_one();
                x_gate.notified().await;
                Ok::<_, Error>(Shared::new(X {
                    waits_while_pending,
                    waits_after_drop,
                }))
            }
        }
    }));

    let set = injector.resolve_all_live::<dyn Source>();
    let x = injector.resolve_live::<X>();
    let m_waits_on_x = (
        std::any::type_name::<dyn Source>(),
        std::any::type_name::<X>(),
    );
    let x_waits_on_m = (m_waits_on_x.1, m_waits_on_x.0);
    timeout(HANG, async {
        while !injector.recorded_waits().contains(&m_waits_on_x)
            && label(&set.members()[0]) == (1, "pending")
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("m neither waited on X nor failed");
    x_gate.notify_one();

    let x = timeout(HANG, x.ready()).await.unwrap().unwrap();
    let names: Vec<&str> = timeout(HANG, set.complete())
        .await
        .unwrap()
        .expect("m's wait on X was refused by the dropped complete()'s edge")
        .iter()
        .map(|member| member.value.name())
        .collect();
    assert_eq!(names, vec!["m"]);
    assert!(x.value.waits_while_pending.contains(&x_waits_on_m));
    assert!(!x.value.waits_after_drop.contains(&x_waits_on_m));
}

/// A live cell's receiver offers no `wait_for` outside `live/channel.rs`, so
/// this scan covers the raw waits that remain.
#[test]
fn every_wait_for_a_run_to_end_records_a_wait_edge() {
    let mut raw_waits = 0;
    for (file, source) in [
        ("live.rs", include_str!("../live.rs")),
        ("live/channel.rs", include_str!("channel.rs")),
        ("injector/cells.rs", include_str!("../injector/cells.rs")),
        (
            "injector/live_cells.rs",
            include_str!("../injector/live_cells.rs"),
        ),
    ] {
        let code = source.split("#[cfg(test)]\nmod tests").next().unwrap();
        for (number, line) in code.lines().enumerate() {
            for raw in [".wait_for(", "Join {"] {
                if let Some(at) = line.find(raw) {
                    raw_waits += 1;
                    assert!(
                        line[..at].contains(".wait("),
                        "{file}:{}: `{raw}` is not awaited through WaitEdge::wait: {line}",
                        number + 1
                    );
                }
            }
        }
    }
    assert_eq!(raw_waits, 2, "the scan no longer sees the run-end waits");
}
