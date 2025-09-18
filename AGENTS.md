# Repository Guidelines

## Project Structure & Module Organization
- `Cargo.toml` defines the workspace with `crates/driftdb-core` (storage engine), `crates/driftdb-cli` (user CLI), and `crates/driftdb-admin` (operations tooling).
- Root `src/lib.rs` hosts the workspace prelude and test-only helpers; integration fixtures live in `test-db/`.
- `examples/` contains demo flows referenced by `make demo`; `benches/` holds Criterion microbenchmarks.
- Consult `CONFIGURATION.md` and `ARCHITECTURAL_REVIEW.md` when altering deployment knobs or internal interfaces.

## Build, Test, and Development Commands
- `make build` (or `cargo build --release`) produces optimized binaries in `target/release`.
- `make test` wraps `cargo test --all`; scope with `cargo test -p driftdb-core -- --ignored` for long invariants.
- `make bench` / `cargo bench --all` executes Criterion suites; run after `make build` for stable baselines.
- `make fmt`, `make clippy`, and `make ci` cover formatting, linting (`-D warnings`), and the full pre-PR pipeline.

## Coding Style & Naming Conventions
- Enforce Rust 2021 defaults via `cargo fmt` before committing; check clippy and fix warnings proactively.
- Favor `tracing` spans over ad-hoc logging; keep public APIs documented with rustdoc comments.
- Use snake_case for modules/functions, CamelCase for types, and SCREAMING_SNAKE_CASE for constants; describe feature flags in `Cargo.toml`.

## Testing Guidelines
- Unit tests colocate with code under `#[cfg(test)]` modules (e.g., `crates/driftdb-core/src/transaction.rs`); workspace-level suites sit in `crates/driftdb-core/src/tests.rs`.
- Property tests rely on `proptest`; pin seeds when reproducing failures and clean them before merging.
- Before opening a PR, run `cargo test --all-features` and rerun targeted tests against storage or replication modules.
- Place new benchmarks in `benches/` behind `criterion_group!` guards to control runtime.

## Commit & Pull Request Guidelines
- Follow the conventional prefixes in history (`feat:`, `fix:`, `docs:`, `refactor:`) and keep subjects ≤72 chars in imperative mood.
- PRs should describe behavior changes, list verification steps (`make ci` minimum), and link issues or ADRs; include CLI output or screenshots for admin UX tweaks.
- Tag engine and CLI maintainers on cross-crate changes and update `ADMIN_GUIDE.md` when storage or operational procedures change.

## Security & Configuration Tips
- Never commit secrets or live data; mirror patterns in `CONFIGURATION.md` for sample values.
- When adjusting encryption or replication defaults, document new flags in `crates/driftdb-cli/src/config.rs` comments and refresh `README.md`.
