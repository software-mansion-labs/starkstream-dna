# Starkstream DNA observability

This private crate contains the shared tracing, metrics, and OpenTelemetry setup
used by Starkstream DNA binaries.

Call `init_opentelemetry` during process startup and use the provided request and
metric helpers to keep telemetry behavior consistent across the server tools.
