# Compatibility contract

The Starkstream migration changes repository, Rust package, module, and binary
names. It intentionally does not rename deployed protocol, configuration,
telemetry, state, or release identifiers.

## Stable interfaces

Changes to the following require a dedicated compatibility and rollout plan:

- protobuf packages `dna.v2.stream` and `starknet.v2`;
- the `DnaStream` service, its method names, message names, existing field
  numbers, and wire types;
- `DNA_*` and `STARKNET_*` environment variables and existing CLI options;
- `dna_*` metric names and labels consumed by dashboards or alerts;
- existing Etcd keys and prefixes, including ingestion and options state;
- existing object-store paths, including canonical/recent and segment data;
- the Nix output and Artifact Registry repository/image name `dna-starknet`;
- the repository dispatch event `dna-starknet-release` and its payload fields.

“DNA” remains the name of the protocol and operational surface. These names are
not leftover branding.

## Internal interfaces

The `starkstream-dna-*` Cargo package, Rust import, and executable names are
internal build interfaces. All workspace packages have publishing disabled, so
they are not a supported crates.io distribution.

The production container now starts `starkstream-dna-starknet`, but the image
repository and its runtime configuration stay unchanged. Existing deployments
promoted by digest do not require a state migration.

## Client compatibility

The Rust SDK formerly located in this repository is discontinued. The
TypeScript SDK is maintained in a separate repository and communicates through
the stable protobuf service. Do not change protobuf wire identities as part of a
repository or package rename.
