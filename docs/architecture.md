# Architecture

Starkstream DNA is a Starknet-only data server built from generic ingestion and
streaming infrastructure plus a Starknet implementation.

## Data flow

```text
Starknet JSON-RPC / WebSocket
            |
            v
   Starknet provider and ingestion
            |
            v
  Canonical chain and reorg detection
            |
            +----> Etcd: leases, state, mutable pointers
            |
            v
  Indexed fragments and segments
            |
            +----> Object store: canonical and compacted data
            |
            v
  DNA gRPC filtering and streaming
            |
            v
          Clients
```

## Package boundaries

- `starknet/` translates Starknet provider responses into DNA fragments,
  implements filters, and assembles the production CLI.
- `common/` owns chain-independent ingestion, canonical-chain recovery,
  compaction, object-store access, filtering infrastructure, and the gRPC
  server. Its `ChainSupport` abstraction remains generic so these concerns stay
  testable and separate from Starknet RPC details.
- `protocol/` compiles the common DNA stream and Starknet protobuf definitions.
  It is an internal server dependency even though the removed Rust SDK also used
  it historically.
- `etcd/` owns distributed coordination primitives. `etcd-dbg/` is the related
  diagnostic CLI.
- `observability/` standardizes tracing, metrics, and OpenTelemetry setup.
- `benchmark/` exercises Starknet streams and reports throughput statistics.

## Storage responsibilities

Etcd stores coordination state and pointers; bulk chain data lives in the
configured AWS S3-compatible, Azure Blob, or Google Cloud Storage object store.
The local file cache accelerates object access. Compaction rewrites immutable
segments without changing the public stream protocol.

Names used in persisted keys and object paths are deployment contracts. See
[Compatibility](compatibility.md) before modifying them.
