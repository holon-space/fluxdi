//! Observability identifiers and helpers used by FluxDI instrumentation.
//!
//! This module keeps span/event/metric names stable and provides optional
//! integrations for tracing backends.

#[cfg(feature = "metrics")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "metrics")]
use std::time::Duration;
#[cfg(feature = "logging")]
use tracing_subscriber::EnvFilter;

/// Span name emitted during provider registration.
pub const SPAN_PROVIDE: &str = "fluxdi.provide";

/// Span name emitted during service resolution.
pub const SPAN_RESOLVE: &str = "fluxdi.resolve";

/// Span name emitted while executing a provider factory.
pub const SPAN_FACTORY_EXECUTE: &str = "fluxdi.factory.execute";

/// Event name emitted when a circular dependency is detected.
pub const EVENT_CIRCULAR_DEPENDENCY: &str = "fluxdi.circular_dependency";

/// Metric name: provider registration attempts.
#[cfg(feature = "metrics")]
pub const METRIC_PROVIDE_ATTEMPTS_TOTAL: &str = "fluxdi_provide_attempts_total";
/// Metric name: successful provider registrations.
#[cfg(feature = "metrics")]
pub const METRIC_PROVIDE_SUCCESS_TOTAL: &str = "fluxdi_provide_success_total";
/// Metric name: failed provider registrations.
#[cfg(feature = "metrics")]
pub const METRIC_PROVIDE_FAILURES_TOTAL: &str = "fluxdi_provide_failures_total";
/// Metric name: resolve attempts.
#[cfg(feature = "metrics")]
pub const METRIC_RESOLVE_ATTEMPTS_TOTAL: &str = "fluxdi_resolve_attempts_total";
/// Metric name: successful resolves.
#[cfg(feature = "metrics")]
pub const METRIC_RESOLVE_SUCCESS_TOTAL: &str = "fluxdi_resolve_success_total";
/// Metric name: failed resolves.
#[cfg(feature = "metrics")]
pub const METRIC_RESOLVE_FAILURES_TOTAL: &str = "fluxdi_resolve_failures_total";
/// Metric name: resolve cache hits.
#[cfg(feature = "metrics")]
pub const METRIC_RESOLVE_CACHE_HITS_TOTAL: &str = "fluxdi_resolve_cache_hits_total";
/// Metric name: resolve cache misses.
#[cfg(feature = "metrics")]
pub const METRIC_RESOLVE_CACHE_MISSES_TOTAL: &str = "fluxdi_resolve_cache_misses_total";
/// Metric name: factory executions.
#[cfg(feature = "metrics")]
pub const METRIC_FACTORY_EXECUTIONS_TOTAL: &str = "fluxdi_factory_executions_total";
/// Metric name: cumulative resolve duration in seconds.
#[cfg(feature = "metrics")]
pub const METRIC_RESOLVE_DURATION_SECONDS_TOTAL: &str = "fluxdi_resolve_duration_seconds_total";
/// Metric name: resolve duration sample count.
#[cfg(feature = "metrics")]
pub const METRIC_RESOLVE_DURATION_SAMPLES_TOTAL: &str = "fluxdi_resolve_duration_samples_total";

/// Snapshot of injector metrics counters.
#[cfg(feature = "metrics")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub provide_attempts_total: u64,
    pub provide_success_total: u64,
    pub provide_failures_total: u64,
    pub resolve_attempts_total: u64,
    pub resolve_success_total: u64,
    pub resolve_failures_total: u64,
    pub resolve_cache_hits_total: u64,
    pub resolve_cache_misses_total: u64,
    pub factory_executions_total: u64,
    pub resolve_duration_ns_total: u64,
    pub resolve_duration_samples_total: u64,
}

#[cfg(feature = "metrics")]
impl MetricsSnapshot {
    /// Renders metrics in Prometheus text exposition format.
    #[cfg(feature = "prometheus")]
    pub fn to_prometheus(self) -> String {
        let mut output = String::new();

        append_counter(
            &mut output,
            METRIC_PROVIDE_ATTEMPTS_TOTAL,
            "Total number of provider registration attempts.",
            self.provide_attempts_total,
        );
        append_counter(
            &mut output,
            METRIC_PROVIDE_SUCCESS_TOTAL,
            "Total number of successful provider registrations.",
            self.provide_success_total,
        );
        append_counter(
            &mut output,
            METRIC_PROVIDE_FAILURES_TOTAL,
            "Total number of failed provider registrations.",
            self.provide_failures_total,
        );
        append_counter(
            &mut output,
            METRIC_RESOLVE_ATTEMPTS_TOTAL,
            "Total number of resolve attempts.",
            self.resolve_attempts_total,
        );
        append_counter(
            &mut output,
            METRIC_RESOLVE_SUCCESS_TOTAL,
            "Total number of successful resolves.",
            self.resolve_success_total,
        );
        append_counter(
            &mut output,
            METRIC_RESOLVE_FAILURES_TOTAL,
            "Total number of failed resolves.",
            self.resolve_failures_total,
        );
        append_counter(
            &mut output,
            METRIC_RESOLVE_CACHE_HITS_TOTAL,
            "Total number of resolve cache hits.",
            self.resolve_cache_hits_total,
        );
        append_counter(
            &mut output,
            METRIC_RESOLVE_CACHE_MISSES_TOTAL,
            "Total number of resolve cache misses.",
            self.resolve_cache_misses_total,
        );
        append_counter(
            &mut output,
            METRIC_FACTORY_EXECUTIONS_TOTAL,
            "Total number of provider factory executions.",
            self.factory_executions_total,
        );

        let resolve_seconds_total = self.resolve_duration_ns_total as f64 / 1_000_000_000.0;
        append_counter_float(
            &mut output,
            METRIC_RESOLVE_DURATION_SECONDS_TOTAL,
            "Total time spent resolving services in seconds.",
            resolve_seconds_total,
        );
        append_counter(
            &mut output,
            METRIC_RESOLVE_DURATION_SAMPLES_TOTAL,
            "Total number of resolve duration samples recorded.",
            self.resolve_duration_samples_total,
        );

        output
    }
}

#[cfg(all(feature = "metrics", feature = "prometheus"))]
fn append_counter(output: &mut String, metric_name: &str, help: &str, value: u64) {
    output.push_str("# HELP ");
    output.push_str(metric_name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');

    output.push_str("# TYPE ");
    output.push_str(metric_name);
    output.push_str(" counter\n");

    output.push_str(metric_name);
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

#[cfg(all(feature = "metrics", feature = "prometheus"))]
fn append_counter_float(output: &mut String, metric_name: &str, help: &str, value: f64) {
    output.push_str("# HELP ");
    output.push_str(metric_name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');

    output.push_str("# TYPE ");
    output.push_str(metric_name);
    output.push_str(" counter\n");

    output.push_str(metric_name);
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

#[cfg(feature = "metrics")]
#[derive(Debug, Default)]
pub(crate) struct MetricsState {
    provide_attempts_total: AtomicU64,
    provide_success_total: AtomicU64,
    provide_failures_total: AtomicU64,
    resolve_attempts_total: AtomicU64,
    resolve_success_total: AtomicU64,
    resolve_failures_total: AtomicU64,
    resolve_cache_hits_total: AtomicU64,
    resolve_cache_misses_total: AtomicU64,
    factory_executions_total: AtomicU64,
    resolve_duration_ns_total: AtomicU64,
    resolve_duration_samples_total: AtomicU64,
}

#[cfg(feature = "metrics")]
impl MetricsState {
    pub(crate) fn record_provide_attempt(&self) {
        self.provide_attempts_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_provide_success(&self) {
        self.provide_success_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_provide_failure(&self) {
        self.provide_failures_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_resolve_attempt(&self) {
        self.resolve_attempts_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_resolve_success(&self, elapsed: Duration) {
        self.resolve_success_total.fetch_add(1, Ordering::Relaxed);
        self.resolve_duration_ns_total
            .fetch_add(duration_to_nanos_u64(elapsed), Ordering::Relaxed);
        self.resolve_duration_samples_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_resolve_failure(&self, elapsed: Duration) {
        self.resolve_failures_total.fetch_add(1, Ordering::Relaxed);
        self.resolve_duration_ns_total
            .fetch_add(duration_to_nanos_u64(elapsed), Ordering::Relaxed);
        self.resolve_duration_samples_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_resolve_cache_hit(&self) {
        self.resolve_cache_hits_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_resolve_cache_miss(&self) {
        self.resolve_cache_misses_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_factory_execution(&self) {
        self.factory_executions_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            provide_attempts_total: self.provide_attempts_total.load(Ordering::Relaxed),
            provide_success_total: self.provide_success_total.load(Ordering::Relaxed),
            provide_failures_total: self.provide_failures_total.load(Ordering::Relaxed),
            resolve_attempts_total: self.resolve_attempts_total.load(Ordering::Relaxed),
            resolve_success_total: self.resolve_success_total.load(Ordering::Relaxed),
            resolve_failures_total: self.resolve_failures_total.load(Ordering::Relaxed),
            resolve_cache_hits_total: self.resolve_cache_hits_total.load(Ordering::Relaxed),
            resolve_cache_misses_total: self.resolve_cache_misses_total.load(Ordering::Relaxed),
            factory_executions_total: self.factory_executions_total.load(Ordering::Relaxed),
            resolve_duration_ns_total: self.resolve_duration_ns_total.load(Ordering::Relaxed),
            resolve_duration_samples_total: self
                .resolve_duration_samples_total
                .load(Ordering::Relaxed),
        }
    }

    #[cfg(feature = "prometheus")]
    pub(crate) fn to_prometheus(&self) -> String {
        self.snapshot().to_prometheus()
    }
}

#[cfg(feature = "metrics")]
fn duration_to_nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(feature = "logging")]
fn default_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Initializes a default text logging subscriber for FluxDI tracing events.
///
/// This helper is available when the `logging` feature is enabled.
/// It reads filters from `RUST_LOG` and falls back to `info` when the
/// environment variable is not set.
#[cfg(feature = "logging")]
pub fn try_init_logging() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt()
        .with_env_filter(default_env_filter())
        .try_init()
}

/// Initializes logging; a failure, e.g. a global subscriber that is already
/// set, is reported on stderr and does not fail the caller.
///
/// Use [`try_init_logging`] if you need to handle initialization failures.
#[cfg(feature = "logging")]
pub fn init_logging() {
    if let Err(err) = try_init_logging() {
        eprintln!("fluxdi: logging was not initialized: {err}");
    }
}

#[cfg(feature = "opentelemetry")]
mod opentelemetry;

#[cfg(feature = "opentelemetry")]
pub use opentelemetry::{opentelemetry_layer, try_init_opentelemetry};
