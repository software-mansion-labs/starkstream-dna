//! Cloud Logging output, and the Error Reporting path built on top of it.
//!
//! Cloud Error Reporting has no ingestion endpoint of its own: it reads what
//! the process already writes to Cloud Logging. An entry is picked up as an
//! error when it is `ERROR` or above and either contains a stack trace in a
//! language Error Reporting parses - Rust is not one - or carries the
//! `ReportedErrorEvent` type marker. This module takes the marker route, so the
//! whole transport is stdout: the logging agent parses a single-line JSON
//! payload into `jsonPayload` and reads the reserved field names out of it.
//!
//! Two pieces:
//!
//!   - [`format`] - the tracing layer. Maps levels onto Cloud Logging
//!     severities and stamps error events with the marker. A custom
//!     `FormatEvent` is needed because `tracing_subscriber`'s JSON output emits
//!     `level`, which Cloud Logging ignores - only `severity` is read, so
//!     entries would otherwise land at DEFAULT severity and never reach Error
//!     Reporting.
//!   - [`panic`] - the panic hook. The standard library's writes an
//!     unstructured multi-line block to stderr, which the agent records as
//!     several unrelated text entries at no particular severity.
//!
//! Selected with `RUST_LOG_FORMAT=gcp`.

mod format;
mod panic;

use std::sync::OnceLock;

pub use format::layer;
pub use panic::init_panic_hook;

/// Identifies this process to Error Reporting, which groups by service and
/// tracks whether a group regressed across versions.
pub struct ServiceContext {
    pub service: String,
    pub version: String,
}

static SERVICE_CONTEXT: OnceLock<ServiceContext> = OnceLock::new();

/// The GCP project, used to link a log entry to its trace. Read once at init
/// because the formatter runs on every event.
static PROJECT_ID: OnceLock<Option<String>> = OnceLock::new();

/// Record what this binary reports itself as. Called from `init_opentelemetry`
/// with the crate name and version.
///
/// The service name follows the otel resolution order, so a deployment that
/// already names itself for traces is named the same way for errors. That
/// matters here: the crate name is the chain binary - `apibara-starknet` - and
/// every network and role shares it, so falling back to it would collapse
/// mainnet, testnet, ingestion and serving into a single Error Reporting
/// service.
///
/// `SERVICE_VERSION` lets a deploy report the image tag it was built from. The
/// crate version rarely changes between deploys, so on its own it cannot
/// attribute a regression to a release.
pub fn set_service_context(service: impl Into<String>, version: impl Into<String>) {
    let service = std::env::var("OTEL_SERVICE_NAME")
        .ok()
        .or_else(service_name_from_resource_attributes)
        .unwrap_or_else(|| service.into());
    let version = std::env::var("SERVICE_VERSION").unwrap_or_else(|_| version.into());

    let _ = SERVICE_CONTEXT.set(ServiceContext { service, version });
    let _ = PROJECT_ID.set(std::env::var("GOOGLE_CLOUD_PROJECT").ok());
}

/// `service.name` out of `OTEL_RESOURCE_ATTRIBUTES`, which is the W3C Baggage
/// list format: comma-separated `key=value` pairs.
///
/// This is where the deployment actually sets the name - `OTEL_SERVICE_NAME` is
/// the shorthand that overrides it, per the otel spec.
fn service_name_from_resource_attributes() -> Option<String> {
    let attributes = std::env::var("OTEL_RESOURCE_ATTRIBUTES").ok()?;

    attributes
        .split(',')
        .filter_map(|attribute| attribute.split_once('='))
        .find(|(key, _)| key.trim() == "service.name")
        .map(|(_, value)| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub fn service_context() -> &'static ServiceContext {
    SERVICE_CONTEXT.get_or_init(|| ServiceContext {
        service: env!("CARGO_PKG_NAME").to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// The `logging.googleapis.com/trace` value for a trace id, or `None` when the
/// project is unknown - the field is a resource name, so a bare trace id in it
/// links nowhere.
pub(crate) fn trace_field(trace_id: &str) -> Option<String> {
    PROJECT_ID
        .get()
        .and_then(Option::as_ref)
        .map(|project_id| format!("projects/{project_id}/traces/{trace_id}"))
}

#[cfg(test)]
mod tests {
    use super::service_name_from_resource_attributes;

    #[test]
    fn the_service_name_comes_from_the_resource_attributes() {
        // What the terraform serving module actually sets. Without this the
        // name falls back to the crate - `apibara-starknet` - which every
        // network and role shares.
        std::env::set_var(
            "OTEL_RESOURCE_ATTRIBUTES",
            "service.name=dna-starknet-mainnet-serving,network=starknet-mainnet,role=serving",
        );
        assert_eq!(
            service_name_from_resource_attributes().as_deref(),
            Some("dna-starknet-mainnet-serving")
        );

        // A resource without a name has nothing to report, so the caller falls
        // back rather than reporting a network as the service.
        std::env::set_var("OTEL_RESOURCE_ATTRIBUTES", "network=starknet-mainnet");
        assert_eq!(service_name_from_resource_attributes(), None);

        std::env::set_var("OTEL_RESOURCE_ATTRIBUTES", "service.name=");
        assert_eq!(service_name_from_resource_attributes(), None);

        std::env::remove_var("OTEL_RESOURCE_ATTRIBUTES");
        assert_eq!(service_name_from_resource_attributes(), None);
    }
}
