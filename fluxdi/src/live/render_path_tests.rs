use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::timeout;

use super::{LiveState, is_terminal, non_blocking_scope, non_blocking_section};
use crate::{Error, ErrorKind, Injector, Provider, Shared};

const HANG: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct Db;

trait Source: Send + Sync {}

struct Member;

impl Source for Member {}

fn gated_db(gate: Arc<Notify>) -> Provider<Db> {
    Provider::root_async(move |_| {
        let gate = gate.clone();
        async move {
            gate.notified().await;
            Shared::new(Db)
        }
    })
}

fn gated_member(gate: Arc<Notify>) -> Provider<dyn Source> {
    Provider::root_async(move |_| {
        let gate = gate.clone();
        async move {
            gate.notified().await;
            Shared::new(Member) as Shared<dyn Source>
        }
    })
}

fn assert_refused(error: &Error, type_name: &str) {
    assert_eq!(error.kind, ErrorKind::LiveWaitOnRenderPath, "{error}");
    assert!(error.message.contains(type_name), "{error}");
}

fn ui_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn ready_on_a_pending_cell_inside_a_non_blocking_scope_is_refused() {
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(Arc::new(Notify::new())));
    let live = injector.resolve_live::<Db>();

    let error = timeout(HANG, non_blocking_scope(live.ready()))
        .await
        .expect("ready() in a non-blocking scope waited for the producer")
        .unwrap_err();
    assert_refused(&error, "Db");
    assert!(matches!(live.state().value, LiveState::Pending));
}

#[tokio::test(flavor = "multi_thread")]
async fn complete_inside_a_non_blocking_scope_is_refused() {
    let injector = Injector::root();
    injector.provide_into_set::<dyn Source>(gated_member(Arc::new(Notify::new())));
    let set = injector.resolve_all_live::<dyn Source>();

    let error = timeout(HANG, non_blocking_scope(set.complete()))
        .await
        .expect("complete() in a non-blocking scope waited for the members")
        .err()
        .expect("complete() in a non-blocking scope returned members");
    assert_refused(&error, "Source");
}

#[test]
fn waits_inside_a_non_blocking_section_on_a_plain_thread_are_refused() {
    let runtime = ui_runtime();
    let handle = runtime.handle().clone();
    let injector = Injector::root_with_runtime(handle.clone());
    injector.provide::<Db>(gated_db(Arc::new(Notify::new())));
    injector.provide_into_set::<dyn Source>(gated_member(Arc::new(Notify::new())));

    let ui_thread = std::thread::spawn(move || {
        let live = injector.resolve_live::<Db>();
        let set = injector.resolve_all_live::<dyn Source>();
        non_blocking_section(|| {
            let ready = handle.block_on(async { timeout(HANG, live.ready()).await });
            let complete = handle.block_on(async { timeout(HANG, set.complete()).await });
            (ready.map(|r| r.err()), complete.map(|c| c.err()))
        })
    });
    let (ready, complete) = ui_thread.join().unwrap();
    let ready = ready.expect("ready() in a non-blocking section waited for the producer");
    assert_refused(&ready.expect("ready() in a section returned a value"), "Db");
    let complete = complete.expect("complete() in a non-blocking section waited");
    assert_refused(
        &complete.expect("complete() in a section returned"),
        "Source",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn waits_on_finished_producers_inside_a_non_blocking_scope_are_refused() {
    let injector = Injector::root();
    injector.provide::<Db>(Provider::root_async(|_| async { Shared::new(Db) }));
    injector.provide_into_set::<dyn Source>(Provider::root_async(|_| async {
        Shared::new(Member) as Shared<dyn Source>
    }));
    let live = injector.resolve_live::<Db>();
    let set = injector.resolve_all_live::<dyn Source>();
    timeout(HANG, live.ready()).await.unwrap().unwrap();
    timeout(HANG, set.complete()).await.unwrap().unwrap();

    let error = non_blocking_scope(live.ready()).await.unwrap_err();
    assert_refused(&error, "Db");
    let error = non_blocking_scope(set.complete()).await.err().unwrap();
    assert_refused(&error, "Source");
}

#[tokio::test(flavor = "multi_thread")]
async fn state_and_changed_work_inside_a_non_blocking_scope() {
    let gate = Arc::new(Notify::new());
    let member_gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(gate.clone()));
    injector.provide_into_set::<dyn Source>(gated_member(member_gate.clone()));
    let mut live = injector.resolve_live::<Db>();
    let mut set = injector.resolve_all_live::<dyn Source>();

    let (before, after, members) = timeout(
        HANG,
        non_blocking_scope(async {
            let before = live.state().value;
            gate.notify_one();
            let after = live.changed().await.value;
            member_gate.notify_one();
            let members = set.changed().await;
            (before, after, members)
        }),
    )
    .await
    .expect("changed() in a non-blocking scope did not follow the producer");
    assert!(matches!(before, LiveState::Pending));
    assert!(matches!(after, LiveState::Ready(_)));
    assert!(matches!(members[0].value, LiveState::Ready(_)));
}

#[test]
fn state_and_changed_work_inside_a_non_blocking_section_on_a_plain_thread() {
    let runtime = ui_runtime();
    let handle = runtime.handle().clone();
    let injector = Injector::root_with_runtime(handle.clone());
    let gate = Arc::new(Notify::new());
    injector.provide::<Db>(gated_db(gate.clone()));

    let ui_thread = std::thread::spawn(move || {
        let mut live = injector.resolve_live::<Db>();
        non_blocking_section(|| {
            let before = live.state().value;
            gate.notify_one();
            let after = handle.block_on(async { timeout(HANG, live.changed()).await });
            (before, after.map(|after| after.value))
        })
    });
    let (before, after) = ui_thread.join().unwrap();
    assert!(matches!(before, LiveState::Pending));
    let after = after.expect("changed() in a non-blocking section did not follow the producer");
    assert!(matches!(after, LiveState::Ready(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_guard_covers_only_what_runs_inside_the_scope_or_section() {
    let gate = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(gate.clone()));
    let live = injector.resolve_live::<Db>();
    gate.notify_one();

    let (inside, beside) = timeout(HANG, async {
        tokio::join!(non_blocking_scope(live.ready()), live.ready())
    })
    .await
    .expect("ready() in a non-blocking scope waited for the producer");
    assert_refused(&inside.unwrap_err(), "Db");
    beside.expect("a sibling future of a non-blocking scope was refused");

    non_blocking_section(|| ());
    timeout(HANG, live.ready())
        .await
        .unwrap()
        .expect("a wait after a non-blocking section ended was refused");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hard_resolve_inside_a_non_blocking_scope_is_refused_only_when_it_would_wait() {
    struct Config;
    let db_gate = Arc::new(Notify::new());
    let config_gate = Arc::new(Notify::new());
    let config_started = Arc::new(Notify::new());
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(db_gate.clone()));
    injector.provide::<Config>(Provider::root_async({
        let config_gate = config_gate.clone();
        let config_started = config_started.clone();
        move |_| {
            let config_gate = config_gate.clone();
            let config_started = config_started.clone();
            async move {
                config_started.notify_one();
                config_gate.notified().await;
                Shared::new(Config)
            }
        }
    }));
    let live = injector.resolve_live::<Db>();
    let driver = tokio::spawn({
        let injector = injector.clone();
        async move { injector.try_resolve_async::<Config>().await.map(|_| ()) }
    });
    timeout(HANG, config_started.notified()).await.unwrap();

    let error = timeout(HANG, non_blocking_scope(injector.try_resolve_async::<Db>()))
        .await
        .expect("a hard resolve of a pending live cell in a non-blocking scope waited")
        .err()
        .unwrap();
    assert_refused(&error, "Db");
    let error = timeout(
        HANG,
        non_blocking_scope(injector.try_resolve_async::<Config>()),
    )
    .await
    .expect("a hard resolve joining another resolve's run in a non-blocking scope waited")
    .err()
    .unwrap();
    assert_refused(&error, "Config");

    db_gate.notify_one();
    config_gate.notify_one();
    timeout(HANG, live.ready()).await.unwrap().unwrap();
    timeout(HANG, driver).await.unwrap().unwrap().unwrap();
    non_blocking_scope(async {
        injector.try_resolve_async::<Db>().await.unwrap();
        injector.try_resolve_async::<Config>().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_factory_refused_inside_a_non_blocking_scope_keeps_the_refusal_kind() {
    struct Composite;
    struct Outer;
    let injector = Injector::root();
    injector.provide::<Db>(gated_db(Arc::new(Notify::new())));
    injector.provide::<Composite>(Provider::root_try_async(|inj: Injector| async move {
        inj.try_resolve_live::<Db>()?.ready().await?;
        Ok::<_, Error>(Shared::new(Composite))
    }));
    injector.provide::<Outer>(Provider::root_try_async(|inj: Injector| async move {
        inj.try_resolve_async::<Composite>().await?;
        Ok::<_, Error>(Shared::new(Outer))
    }));
    injector.resolve_live::<Db>();

    for error in [
        timeout(
            HANG,
            non_blocking_scope(injector.try_resolve_async::<Composite>()),
        )
        .await
        .unwrap()
        .err()
        .unwrap(),
        timeout(
            HANG,
            non_blocking_scope(injector.try_resolve_async::<Outer>()),
        )
        .await
        .unwrap()
        .err()
        .unwrap(),
    ] {
        assert_refused(&error, "Db");
        assert!(error.message.contains("Composite"), "{error}");
    }
}

#[test]
fn a_producer_polled_inside_a_non_blocking_section_may_wait() {
    #[derive(Debug)]
    struct A;
    #[derive(Debug)]
    struct B;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let injector = Injector::root_with_runtime(runtime.handle().clone());
    injector.provide::<A>(Provider::root_async(|_| async { Shared::new(A) }));
    injector.provide::<B>(Provider::root_try_async(|inj: Injector| async move {
        inj.try_resolve_live::<A>()?.ready().await?;
        Ok::<_, Error>(Shared::new(B))
    }));

    let state = non_blocking_section(|| {
        runtime.block_on(async {
            let mut b = injector.resolve_live::<B>();
            timeout(HANG, async {
                loop {
                    let state = b.changed().await.value;
                    if is_terminal(&state) {
                        return state;
                    }
                }
            })
            .await
        })
    })
    .expect("B's producer did not end");
    assert!(matches!(state, LiveState::Ready(_)), "{state:?}");
}
