# Supercluster compatibility checks

This test harness compiles the actual core `supercluster/mod.rs` and its tests
directly. It does not copy the implementation or depend on the full core's MLT
reference-decoder test dependencies, which require newer Cargo.

```sh
cargo +1.77.2 test --manifest-path tools/supercluster-compat/Cargo.toml --locked
cargo +1.77.2 test --manifest-path tools/supercluster-compat/Cargo.toml --locked --release
```

The JSON parser's `float_roundtrip` feature is test-only. Golden fixtures live
under `src/rust/freestiler-core/tests/fixtures/`; tests need no Node installation.
Do not loosen bit-exact assertions on a new platform without diagnosing whether
the discrepancy comes from projection/libm, fixture parsing or the algorithm.

The default-feature core library is checked separately:

```sh
cargo +1.77.2 build --manifest-path src/rust/freestiler-core/Cargo.toml --locked
```

The full core **test** graph currently needs a newer toolchain because its MLT
reference decoder pulls in Edition-2024 code. That is not a failure of the
default-feature production library's Rust 1.77.2 build.

## Publishing the CI check

The workflow is currently local and uncommitted, with no remote job queued. To
make it runnable in a pull request, publish these files together:

- `.github/workflows/supercluster-parity.yml`
- this directory's `Cargo.toml` and `Cargo.lock`
- `src/rust/freestiler-core/src/supercluster/`, including tests and upstream notices
- `src/rust/freestiler-core/tests/fixtures/supercluster-8.0.1.json.gz`

The workflow then tests macOS, Linux and Windows with stable and 1.77.2 compilers
in debug and release. A local YAML file is not evidence that any remote check
has started. Manual `workflow_dispatch` additionally requires the workflow on
the default branch. This harness is excluded from the R source package.
