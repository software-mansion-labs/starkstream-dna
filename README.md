# Starkstream DNA

Starkstream DNA is a high-performance Starknet data server. It ingests Starknet
blocks, maintains a reorg-aware canonical history, and exposes filtered data over
the DNA gRPC streaming protocol.

This repository contains the Starknet server and its internal Rust crates. EVM,
Beacon Chain, and the former Rust client SDK are not part of this distribution.
The TypeScript SDK is maintained separately in
[apibara-typescript-sdk](https://github.com/software-mansion-labs/apibara-typescript-sdk)
while its Starkstream migration is completed.

> [!NOTE]
> Documentation is being rebuilt. Until it launches, this repository's README,
> crate rustdoc, and protobuf definitions are the canonical technical reference.

## Architecture

The server reads Starknet data through JSON-RPC and WebSocket providers, turns
it into indexed fragments, stores canonical and compacted segments in an object
store, and serves filtered streams over gRPC. Etcd coordinates ingestion state,
leases, and mutable pointers; persisted block data remains in the configured
object store.

The workspace is split into these private packages:

| Package | Responsibility |
|---|---|
| `starkstream-dna-starknet` | Starknet provider, ingestion, filters, CLI, and production binary |
| `starkstream-dna-common` | Reorg-aware ingestion, storage, compaction, and gRPC server infrastructure |
| `starkstream-dna-protocol` | Generated DNA stream and Starknet protobuf types used by the server |
| `starkstream-dna-etcd` | Etcd client, locking, key/value, and watch helpers |
| `starkstream-dna-observability` | Tracing, metrics, and OpenTelemetry helpers |
| `starkstream-dna-benchmark` | Starknet stream benchmark utility |

See [Architecture](docs/architecture.md) for the component flow and
[Compatibility](docs/compatibility.md) for the identifiers that deliberately
retain DNA naming.

## Development

The repository uses stable Rust and provides a Nix development environment with
the required native dependencies and protobuf compiler:

```sh
nix develop --accept-flake-config
cargo check --workspace --all-targets
```

Run the same build and test boundaries used by CI:

```sh
nix flake check -L --accept-flake-config
nix build .#unit-tests -L --accept-flake-config
nix build .#integration-tests-archive -L --accept-flake-config
nix develop .#integration --accept-flake-config -c run-integration-tests
nix build .#all-crates -L --accept-flake-config
```

Integration tests require access to a Docker daemon for their Etcd and
object-store test containers.

Build the Starknet binary with Nix:

```sh
nix build .#dna-starknet -L --accept-flake-config
./result/bin/starkstream-dna-starknet start --help
```

## Compatibility

The repository and Rust package names changed, but deployed protocol and state
contracts did not. In particular, `dna.v2.stream`, `starknet.v2`, `DNA_*` and
`STARKNET_*` configuration, `dna_*` metrics, persisted keys and object prefixes,
the `dna-starknet` image repository, and the `dna-starknet-release` event remain
stable. The package and repository migration is summarized in
[Compatibility](docs/compatibility.md); see [Architecture](docs/architecture.md)
for the resulting Starknet-only component layout.

## Releases

Images are built from `main`, published immutably to the dedicated
`dna-starknet` Artifact Registry repository, and promoted by digest through the
infrastructure repository. This repository does not deploy to Kubernetes.
Maintainers should follow [RELEASE.md](RELEASE.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, tests, and pull-request
guidance.

## History and license

Starkstream DNA builds on the original Apibara DNA codebase and preserves its
contributors' work and attribution. The project is licensed under the
[Apache License 2.0](LICENSE.txt).
