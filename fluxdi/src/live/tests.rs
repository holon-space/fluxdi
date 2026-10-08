use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::timeout;

use crate::live::Generational;
use crate::{Error, ErrorKind, Injector, LiveOutcome, LiveState, Provider, Shared};

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
