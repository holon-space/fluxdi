#![cfg(feature = "logging")]

use fluxdi::{Injector, Provider, Shared, init_logging, try_init_logging};

const CHILD: &str = "FLUXDI_INIT_LOGGING_TWICE";

#[test]
fn initializes_logging_and_emits_fluxdi_events() {
    try_init_logging().expect("logging subscriber should initialize");

    let injector = Injector::root();
    injector.provide::<String>(Provider::transient(|_| Shared::new("value".to_string())));

    let value = injector.try_resolve::<String>().unwrap();
    assert_eq!(value.as_str(), "value");
}

/// Runs itself in a child process, whose stderr the parent reads.
#[test]
fn init_logging_reports_a_failed_initialization_on_stderr() {
    if std::env::var_os(CHILD).is_some() {
        init_logging();
        init_logging();
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "init_logging_reports_a_failed_initialization_on_stderr",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "child failed: {stderr}");
    assert_eq!(
        stderr
            .matches("fluxdi: logging was not initialized:")
            .count(),
        1,
        "{stderr}"
    );
}
