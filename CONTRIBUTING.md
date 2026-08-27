# Contributing to Starkstream DNA

Starkstream DNA is developed as an open-source Rust project. Before starting a
substantial change, open or review an issue in the
[starkstream-dna repository](https://github.com/software-mansion-labs/starkstream-dna/issues)
so its scope and compatibility impact are clear.

## Clone and enter the development environment

```sh
git clone git@github.com:software-mansion-labs/starkstream-dna.git
cd starkstream-dna
nix develop --accept-flake-config
```

The Nix shell provides the pinned Rust toolchain, native libraries, and protobuf
compiler. A system Rust installation can be used, but contributors are still
expected to run the Nix checks that CI uses.

## Build, format, and test

During development, use Cargo for quick feedback:

```sh
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --lib
```

Before requesting review, run:

```sh
cargo fmt --all --check
nix flake check -L --accept-flake-config
nix build .#unit-tests -L --accept-flake-config
nix build .#integration-tests-archive -L --accept-flake-config
nix develop .#integration --accept-flake-config -c run-integration-tests
nix build .#all-crates -L --accept-flake-config
```

Integration tests run Etcd and object-store containers and therefore require a
working Docker daemon.

## Compatibility expectations

Read [docs/compatibility.md](docs/compatibility.md) before changing protobufs,
configuration variables, metrics, or storage keys. Internal Rust packages may
evolve, but deployed wire and state contracts require an explicit migration
plan. The workspace is intentionally Starknet-only; introducing another chain
implementation or a published Rust SDK requires a separate design decision.

## Commits and pull requests

Keep commits focused and use a short subsystem prefix when one is useful, for
example:

```text
starknet: handle pending block update
common: add canonical segment regression test
```

A pull request should explain why the change is needed, identify compatibility
risks, and list the exact verification commands run. Do not include editor or
machine-local files.

## Releases

The legacy release-branch and Quay process has been retired. Maintainers release
the immutable Starknet runtime using the manual workflow documented in
[RELEASE.md](RELEASE.md). Ordinary contributions must not trigger a release or
modify the infrastructure repository.
