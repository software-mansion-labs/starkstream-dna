use apibara_observability::{Counter, Histogram, RequestMetrics, UpDownCounter};

#[derive(Debug, Clone)]
pub struct DataStreamMetrics {
    /// Number of client-facing data streams returned by the RPC service.
    pub active: UpDownCounter<i64>,
    /// Number of background workers serving data streams.
    pub worker_active: UpDownCounter<i64>,
    pub block_size: Histogram<u64>,
    pub fragment_size: Histogram<u64>,
    pub time_in_queue: Histogram<f64>,
    pub block_download: RequestMetrics,
    pub segment_download: RequestMetrics,
    pub segment_wait: RequestMetrics,
    pub group_download: RequestMetrics,
    pub group_wait: RequestMetrics,
    pub group_cache_hit: Counter<u64>,
}

impl Default for DataStreamMetrics {
    fn default() -> Self {
        let meter = apibara_observability::meter("dna_data_stream");

        Self {
            active: meter
                .i64_up_down_counter("dna.data_stream.active")
                .with_description("number of active client data streams")
                .with_unit("{connection}")
                .build(),
            worker_active: meter
                .i64_up_down_counter("dna.data_stream.worker_active")
                .with_description("number of active data stream workers")
                .with_unit("{connection}")
                .build(),
            block_size: meter
                .u64_histogram("dna.data_stream.block_size")
                .with_description("size (in bytes) of blocks sent to the client")
                .with_unit("By")
                .with_boundaries(vec![
                    1_000.0,
                    10_000.0,
                    100_000.0,
                    1_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    25_000_000.0,
                    50_000_000.0,
                    100_000_000.0,
                    1_000_000_000.0,
                ])
                .build(),
            fragment_size: meter
                .u64_histogram("dna.data_stream.fragment_size")
                .with_description("size (in bytes) of fragments sent to the client")
                .with_unit("By")
                .with_boundaries(vec![
                    1_000.0,
                    10_000.0,
                    100_000.0,
                    1_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    25_000_000.0,
                    50_000_000.0,
                    100_000_000.0,
                    1_000_000_000.0,
                ])
                .build(),
            time_in_queue: meter
                .f64_histogram("dna.data_stream.time_in_queue")
                .with_description("time (in seconds) spent in the prefetch queue")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 5.0, 10.0, 30.0,
                ])
                .build(),
            block_download: RequestMetrics::new_with_boundaries(
                "dna_data_stream",
                "dna.data_stream.block_download",
                vec![
                    0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 5.0, 10.0, 30.0,
                ],
            ),
            segment_download: RequestMetrics::new_with_boundaries(
                "dna_data_stream",
                "dna.data_stream.segment_download",
                vec![
                    0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 5.0, 10.0, 30.0,
                ],
            ),
            segment_wait: RequestMetrics::new_with_boundaries(
                "dna_data_stream",
                "dna.data_stream.segment_wait",
                vec![
                    0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 5.0, 10.0, 30.0,
                ],
            ),
            group_download: RequestMetrics::new_with_boundaries(
                "dna_data_stream",
                "dna.data_stream.group_download",
                vec![
                    0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 5.0, 10.0, 30.0,
                ],
            ),
            group_wait: RequestMetrics::new_with_boundaries(
                "dna_data_stream",
                "dna.data_stream.group_wait",
                vec![
                    0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 5.0, 10.0, 30.0,
                ],
            ),
            group_cache_hit: meter
                .u64_counter("dna.data_stream.group_cache_hit")
                .with_description("number of group cache hits")
                .build(),
        }
    }
}

/// Keeps an active metric balanced over the lifetime of the guarded value.
#[derive(Debug)]
pub(crate) struct ActiveStreamGuard {
    active: UpDownCounter<i64>,
}

impl ActiveStreamGuard {
    pub(crate) fn new(active: UpDownCounter<i64>) -> Self {
        active.add(1, &[]);
        Self { active }
    }
}

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        self.active.add(-1, &[]);
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        data::{AggregatedMetrics, MetricData},
        InMemoryMetricExporter, SdkMeterProvider,
    };

    use super::ActiveStreamGuard;

    #[test]
    fn active_stream_guard_balances_metric() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let active = provider
            .meter("active_stream_guard_test")
            .i64_up_down_counter("active")
            .build();

        let guard = ActiveStreamGuard::new(active);
        provider.force_flush().expect("metric flush should succeed");
        assert_eq!(metric_value(&exporter), 1);

        drop(guard);
        provider.force_flush().expect("metric flush should succeed");
        assert_eq!(metric_value(&exporter), 0);
    }

    fn metric_value(exporter: &InMemoryMetricExporter) -> i64 {
        let resource_metrics = exporter
            .get_finished_metrics()
            .expect("metrics should be available");
        let metric = resource_metrics
            .last()
            .and_then(|resource| resource.scope_metrics().next())
            .and_then(|scope| scope.metrics().find(|metric| metric.name() == "active"))
            .expect("active metric should exist");

        let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() else {
            panic!("active metric should be an i64 sum");
        };

        let value = sum
            .data_points()
            .next()
            .expect("active metric should have a data point")
            .value();
        value
    }
}
