#![cfg(feature = "live")]

use std::time::Duration;

use fluxdi::live::non_blocking_scope;
use fluxdi::{ErrorKind, Injector, Provider, Shared};

struct Db;

#[tokio::test(flavor = "multi_thread")]
async fn a_wait_on_a_non_blocking_path_panics_in_debug_builds_and_fails_in_release_builds() {
    let injector = Injector::root();
    injector.provide::<Db>(Provider::root_async(|_| {
        std::future::pending::<Shared<Db>>()
    }));
    let live = injector.resolve_live::<Db>();

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::spawn(non_blocking_scope(
            async move { live.ready().await.map(|_| ()) },
        )),
    )
    .await
    .expect("ready() in a non-blocking scope waited for the producer");

    if cfg!(debug_assertions) {
        let panic = outcome.unwrap_err().into_panic();
        let message = panic
            .downcast_ref::<String>()
            .expect("the guard panics with a formatted message");
        assert!(
            message.contains("non-blocking path") && message.contains("Db"),
            "{message}"
        );
    } else {
        let error = outcome.unwrap().unwrap_err();
        assert_eq!(error.kind, ErrorKind::LiveWaitOnRenderPath, "{error}");
    }
}
