//! The tracing layer that writes Cloud Logging structured entries to stdout.
//!
//! The output is one JSON object per line. Field names are not arbitrary - the
//! logging agent and Error Reporting both read specific keys:
//!
//!   severity                                 LogEntry.severity
//!   message                                  the entry payload, and the text
//!                                            Error Reporting groups on
//!   time                                     LogEntry.timestamp
//!   logging.googleapis.com/sourceLocation    LogEntry.sourceLocation
//!   logging.googleapis.com/trace             LogEntry.trace
//!   logging.googleapis.com/spanId            LogEntry.spanId
//!   @type + serviceContext + context         ReportedErrorEvent
//!
//! Anything else lands in `jsonPayload` under its own name and is queryable in
//! the Logs Explorer, which is what makes the grouping caveat below workable.
//!
//! On grouping: Error Reporting has no Rust stack trace to work with and falls
//! back to the message text plus `context.reportLocation`, so
//! `error!("failed for {cursor}")` fans out into one group per cursor. Keeping
//! the varying part in a structured field - `error!(%cursor, "failed")` -
//! groups on the constant text and keeps the cursor queryable.

use std::fmt;

use serde_json::{Map, Value};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_opentelemetry::OtelData;
use tracing_subscriber::{
    fmt::{format::JsonFields, FmtContext, FormatEvent, FormattedFields},
    registry::LookupSpan,
};

use super::service_context;

/// The marker that promotes an entry from "a log line" to "an error Error
/// Reporting tracks". Without it an entry needs a stack trace in a language
/// Error Reporting parses, which Rust's is not.
pub(super) const REPORTED_ERROR_EVENT_TYPE: &str =
    "type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent";

/// Build the layer. Pair it with `JsonFields` so span fields arrive as JSON this
/// formatter can merge rather than as a display string it would have to parse.
pub fn layer<S>() -> tracing_subscriber::fmt::Layer<S, JsonFields, CloudLoggingFormat>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer()
        .json()
        .event_format(CloudLoggingFormat)
}

/// Maps a tracing level onto the `LogSeverity` enum Cloud Logging expects.
/// TRACE has no distinct severity - DEBUG is the floor - so the two collapse.
fn severity(level: &Level) -> &'static str {
    match *level {
        Level::TRACE | Level::DEBUG => "DEBUG",
        Level::INFO => "INFO",
        Level::WARN => "WARNING",
        Level::ERROR => "ERROR",
    }
}

pub struct CloudLoggingFormat;

impl<S> FormatEvent<S, JsonFields> for CloudLoggingFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, JsonFields>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let mut entry = Map::new();

        // Fields from the enclosing spans first, then the event's own, then the
        // reserved keys. Later writes win, so a stray user field named
        // `severity` cannot corrupt the entry - it is simply shadowed.
        if let Some(leaf) = ctx.lookup_current() {
            for span in leaf.scope().from_root() {
                let extensions = span.extensions();
                if let Some(fields) = extensions.get::<FormattedFields<JsonFields>>() {
                    if let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(fields) {
                        entry.extend(fields);
                    }
                }
            }

            // Trace correlation, when the otel layer is running. It is the only
            // thing that puts an `OtelData` in the span extensions, so this is
            // simply absent when `OTEL_SDK_DISABLED` is set.
            let extensions = leaf.extensions();
            if let Some(otel) = extensions.get::<OtelData>() {
                if let Some(trace) = otel
                    .trace_id()
                    .filter(|trace_id| trace_id.to_string() != "00000000000000000000000000000000")
                    .and_then(|trace_id| super::trace_field(&trace_id.to_string()))
                {
                    entry.insert("logging.googleapis.com/trace".into(), trace.into());
                    if let Some(span_id) = otel.span_id() {
                        entry.insert(
                            "logging.googleapis.com/spanId".into(),
                            span_id.to_string().into(),
                        );
                    }
                }
            }
        }

        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let message = visitor.message.unwrap_or_default();
        entry.extend(visitor.fields);

        entry.insert("severity".into(), severity(meta.level()).into());
        entry.insert("message".into(), message.into());
        entry.insert("target".into(), meta.target().into());

        // A clock failure here would mean losing the line entirely, and a log
        // line without a timestamp is more useful than no log line: the agent
        // stamps its own receive time when `time` is absent.
        if let Ok(time) = OffsetDateTime::now_utc().format(&Rfc3339) {
            entry.insert("time".into(), time.into());
        }

        // Cloud Logging wants the line number as a string here. It wants it as a
        // number in `context.reportLocation` below - the two structures are
        // specified by different APIs and genuinely disagree.
        if let Some(file) = meta.file() {
            let mut location = Map::new();
            location.insert("file".into(), file.into());
            if let Some(line) = meta.line() {
                location.insert("line".into(), line.to_string().into());
            }
            location.insert("function".into(), meta.target().into());
            entry.insert(
                "logging.googleapis.com/sourceLocation".into(),
                Value::Object(location),
            );
        }

        if *meta.level() == Level::ERROR {
            decorate_as_reported_error(&mut entry, meta.file(), meta.line(), meta.target());
        }

        // `Value`'s Display is compact, so this is the single line the logging
        // agent needs to parse the payload as JSON rather than as text.
        writeln!(writer, "{}", Value::Object(entry))
    }
}

/// Add the fields that make an entry a `ReportedErrorEvent`.
///
/// Shared with the panic hook, which builds its entry by hand rather than
/// through the tracing layer.
pub(super) fn decorate_as_reported_error(
    entry: &mut Map<String, Value>,
    file: Option<&str>,
    line: Option<u32>,
    function: &str,
) {
    let service = service_context();
    entry.insert("@type".into(), REPORTED_ERROR_EVENT_TYPE.into());
    entry.insert(
        "serviceContext".into(),
        serde_json::json!({
            "service": service.service,
            "version": service.version,
        }),
    );

    // With no parsable stack trace this is what Error Reporting groups on, so
    // it is the difference between one group per call site and one group per
    // distinct interpolated message.
    let mut report_location = Map::new();
    report_location.insert("filePath".into(), file.unwrap_or("<unknown>").into());
    report_location.insert("lineNumber".into(), line.unwrap_or(0).into());
    report_location.insert("functionName".into(), function.into());

    let mut error_context = Map::new();
    error_context.insert("reportLocation".into(), Value::Object(report_location));

    entry.insert("context".into(), Value::Object(error_context));
}

/// Collects an event's fields, pulling `message` out as the entry payload and
/// leaving the rest as structured JSON.
#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    fields: Map<String, Value>,
}

impl FieldVisitor {
    fn record(&mut self, field: &Field, value: Value) {
        if field.name() == "message" {
            self.message = value.as_str().map(str::to_string);
        } else {
            self.fields.insert(field.name().to_string(), value);
        }
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record(field, format!("{value:?}").into());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value.into());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record(field, value.into());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record(field, value.into());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record(field, value.into());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record(field, value.into());
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    /// Collects whatever the layer writes so a test can assert on the JSON.
    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Buffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Emit through a real subscriber and hand back the parsed entry.
    fn capture(emit: impl FnOnce()) -> Value {
        let buffer = Buffer::default();
        let subscriber = tracing_subscriber::registry().with(layer().with_writer(buffer.clone()));

        tracing::subscriber::with_default(subscriber, emit);

        let written = buffer.0.lock().unwrap().clone();
        let written = String::from_utf8(written).expect("output is not utf-8");
        assert_eq!(
            written.lines().count(),
            1,
            "the logging agent parses one JSON object per line, got: {written}"
        );
        serde_json::from_str(&written).expect("output is not JSON")
    }

    #[test]
    fn info_event_is_a_plain_log_entry() {
        let entry = capture(|| tracing::info!(block_number = 42, "ingested block"));

        assert_eq!(entry["severity"], "INFO");
        assert_eq!(entry["message"], "ingested block");
        assert_eq!(entry["block_number"], 42);
        assert!(entry["time"].is_string());
        assert_eq!(
            entry["logging.googleapis.com/sourceLocation"]["file"],
            file!()
        );

        // Only errors are reported; an info entry carrying the marker would
        // create an Error Reporting group for every log line.
        assert!(entry.get("@type").is_none());
    }

    #[test]
    fn error_event_carries_the_reported_error_marker() {
        let entry = capture(|| tracing::error!("failed to ingest block"));

        assert_eq!(entry["severity"], "ERROR");
        assert_eq!(entry["@type"], REPORTED_ERROR_EVENT_TYPE);
        assert_eq!(entry["message"], "failed to ingest block");
        assert!(entry["serviceContext"]["service"].is_string());
        assert!(entry["serviceContext"]["version"].is_string());

        // What Error Reporting groups on when there is no parsable stack trace.
        let location = &entry["context"]["reportLocation"];
        assert_eq!(location["filePath"], file!());
        assert!(location["lineNumber"].as_u64().unwrap() > 0);
    }

    #[test]
    fn warnings_are_not_reported_as_errors() {
        let entry = capture(|| tracing::warn!("failed to refresh the chain view"));

        assert_eq!(entry["severity"], "WARNING");
        assert!(entry.get("@type").is_none());
    }

    #[test]
    fn span_fields_are_merged_into_the_entry() {
        let entry = capture(|| {
            let span = tracing::info_span!("ingest", block_number = 7);
            let _entered = span.enter();
            tracing::info!("started");
        });

        assert_eq!(entry["block_number"], 7);
        assert_eq!(entry["message"], "started");
    }

    #[test]
    fn reserved_keys_survive_a_colliding_field() {
        // A field named `severity` must not be able to downgrade an error, or a
        // caller could hide a failure from Error Reporting by accident.
        let entry = capture(|| tracing::error!(severity = "DEBUG", "boom"));

        assert_eq!(entry["severity"], "ERROR");
    }

    #[test]
    fn the_trace_field_is_omitted_without_a_project() {
        // A bare trace id in `logging.googleapis.com/trace` links nowhere, so
        // the field is left off rather than filled with something unusable.
        let entry = capture(|| tracing::info!("no trace"));

        assert!(entry.get("logging.googleapis.com/trace").is_none());
    }
}
