# Changelog

All notable changes to FluxDI are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Performance docs (`docs/src/performance.md`) with feature impact and benchmark commands
- **Epic H (continued)**: `BootstrapOptions::with_parallel_start()` for concurrent module `on_start` execution
- Bootstrap lifecycle error aggregation: `Error::bootstrap_aggregate()` when multiple modules fail during parallel startup
- Rollback policy: on bootstrap failure, `on_stop` is called on already-started modules (sequential and parallel)
- Graceful shutdown timeout: with `ShutdownOptions::with_timeout`, all modules' `on_stop` are attempted within the time budget; no partial abort

- `Error::module_lifecycle_failed_with_source()`: a lifecycle error whose `source()` is the hook's error
- `try_optional_resolve`, `try_optional_resolve_named`, `try_optional_resolve_all` and their `_async` forms: `Ok(None)` only when no provider is registered; every other failure is returned
- `Error::module_name()`: the module whose lifecycle hook failed; for an aggregate, the module of its first failure

### Changed

- Bootstrap and shutdown keep a module's `configure`/`on_start`/`on_stop` error as the lifecycle error's `source()`; the message is unchanged. `bootstrap_aggregate`/`shutdown_aggregate` keep the first failure as `source()`
- A failed `on_stop` during bootstrap rollback is reported in the bootstrap error after the start failure, instead of being discarded
- Lifecycle errors name the failing module's concrete type (`Module::type_name`) instead of `dyn Module`
- A panic in a factory (sync or async, hard or live) is a failure of that resolve: `ErrorKind::FactoryFailed` with a `FactoryPanic` source, and nothing is cached. With `panic = "abort"` the process still aborts
- fluxdi compiles with default features
- `optional_resolve*` is deprecated: it turns every failure, including a failed factory, into `None`; use `try_optional_resolve*`
- **Performance**: Cache-hit resolve skips `ResolveGuard` (no recursion); ~67% faster cached resolve (~20 ns → ~12 ns default)

## [1.2.2] - 2026-03-21

### Added

- **Epic H (partial)**: `BootstrapOptions` and `ShutdownOptions` for lifecycle control
- Optional bootstrap and shutdown timeouts via `lifecycle` feature
- `Application::bootstrap_with_options()` and `Application::shutdown_with_options()`
- Structured shutdown error aggregation: when multiple modules fail during `on_stop()`, all modules are attempted and a single aggregated error is returned
- `Error::shutdown_aggregate()` for combining multiple shutdown failures
- Production Patterns documentation (`docs/src/production-patterns.md`) for long-running servers
- **Epic I**: `Provider::with_decorator()` for service wrapping (logging, caching, retry)
- `examples/decorator` demonstrating logging and caching decorators
- `docs/src/decorators.md` documentation
- `bench_resolve_with_decorator` and `bench_resolve_decorator_baseline` benchmarks for decorator overhead

## [1.2.1] - 2026-03-21

### Changed

- Unified documentation version references to 1.2.1 across README, mdBook, and examples
- Fixed encoding issues in `fluxdi/README.md` (corrupted emoji in section headers)

### Added

- **Epic A completion**: Macro vs manual provider parity documentation
- New `examples/injectable-macro` demonstrating `#[derive(Injectable)]` as an alternative to manual provider closures
- "Manual vs Macro Parity" section in `docs/derive-macros.md` with side-by-side comparison
- Injectable macro example entry in main README and `docs/examples.md`

### Fixed

- Section headers `## Examples`, `## Usage Guide`, `### Architectural Patterns`, `### Developer Experience` display correctly

## [1.2.0]

### Added

- `Module::configure()` as the preferred registration hook (returns `Result<(), Error>`)
- `#[derive(Injectable)]` via `fluxdi-macros` crate (macros feature)
- Named bindings (`provide_named`, `resolve_named`) for multiple implementations of one trait
- Multi-binding (`provide_into_set`, `resolve_all`) for plugin pipelines
- Graph tooling (`dependency_graph()`, `validate_graph()`, DOT/Mermaid export)
- Scoped context (`create_scope()`, `Provider::scoped`)
- Testing override API (`try_override_provider`)
- Error suggestions for `ServiceNotProvided`, `AsyncFactoryRequiresAsyncResolve`, circular dependency
- Axum and Actix-web integration extractors (`Resolved<T>`)
- Module lifecycle hooks (`on_start`, `on_stop`) with async support
- `Application::bootstrap()` and `Application::shutdown()` for async lifecycle

## [1.1.0]

### Added

- Initial release with core DI functionality
- Transient, singleton (root), and module-scoped lifetimes
- Injector, Provider, Shared types
- Module system with imports
- Circular dependency detection
- Resource limits (Policy, Limits)
- Async factory support
- Metrics and Prometheus export
- Logging and tracing integration

[Unreleased]: https://github.com/DaiYuANg/fluxdi/compare/v1.2.2...HEAD
[1.2.2]: https://github.com/DaiYuANg/fluxdi/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/DaiYuANg/fluxdi/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/DaiYuANg/fluxdi/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/DaiYuANg/fluxdi/releases/tag/v1.1.0
