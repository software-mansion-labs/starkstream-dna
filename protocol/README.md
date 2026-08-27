# Starkstream DNA protocol types

This private crate compiles the protobuf messages used by the Starkstream DNA
server:

- `dna.v2.stream` defines the stable gRPC streaming service;
- `starknet.v2` defines Starknet block and filter messages.

It is server infrastructure, not a supported Rust SDK distribution. Consumers
should use the maintained TypeScript SDK or generate a client from the protobuf
definitions. Package names, services, methods, field numbers, and wire types are
compatibility-sensitive; see [Compatibility](../docs/compatibility.md).

## License

Copyright 2025 GNC Labs Limited

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE.txt).
