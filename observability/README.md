# Starkstream DNA observability

This private crate contains the shared tracing, metrics, and OpenTelemetry setup
used by Starkstream DNA binaries.

Call `init_opentelemetry` during process startup and use the provided request and
metric helpers to keep telemetry behavior consistent across the server tools.

## GCP deployment

Set `RUST_LOG_FORMAT=gcp` to enable structured logs and panic reporting.
Service identity comes from `OTEL_SERVICE_NAME` or `service.name` in
`OTEL_RESOURCE_ATTRIBUTES`; set `SERVICE_VERSION` to the deployed Git SHA.

Trace links require `GOOGLE_CLOUD_PROJECT`, `OTEL_SDK_DISABLED=false`, and
`OTEL_EXPORTER_OTLP_ENDPOINT` pointing to a collector exporting to Cloud Trace.
