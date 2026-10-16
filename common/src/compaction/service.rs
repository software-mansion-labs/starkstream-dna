use error_stack::{Result, ResultExt};
use futures::FutureExt;
use starkstream_dna_etcd::{EtcdClient, Lock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    block_store::{BlockStoreWriter, UncachedBlockStoreReader},
    chain_view::ChainView,
    compaction::{group::SegmentGroupService, prune::PruneService},
    ingestion::IngestionStateClient,
    object_store::ObjectStore,
};

use super::{error::CompactionError, metrics::CompactionMetrics, segment::SegmentService};

#[derive(Debug, Clone)]
pub struct CompactionServiceOptions {
    /// How many blocks in a single segment.
    pub segment_size: usize,
    /// How many segments in a single segment group.
    pub group_size: usize,
}

pub struct CompactionService {
    options: CompactionServiceOptions,
    block_store_reader: UncachedBlockStoreReader,
    block_store_writer: BlockStoreWriter,
    state_client: IngestionStateClient,
    chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
    metrics: CompactionMetrics,
}

impl CompactionService {
    pub fn new(
        etcd_client: EtcdClient,
        object_store: ObjectStore,
        chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
        options: CompactionServiceOptions,
        metrics: CompactionMetrics,
    ) -> Self {
        let block_store_reader = UncachedBlockStoreReader::new(object_store.clone());
        let block_store_writer = BlockStoreWriter::new(object_store);
        let state_client = IngestionStateClient::new(&etcd_client);

        Self {
            options,
            block_store_reader,
            block_store_writer,
            chain_view,
            state_client,
            metrics,
        }
    }

    pub async fn start(
        self,
        lock: &mut Lock,
        ct: CancellationToken,
    ) -> Result<(), CompactionError> {
        let chain_view = loop {
            lock.keep_alive().await.change_context(CompactionError)?;

            if let Some(chain_view) = self.chain_view.borrow().clone() {
                break chain_view;
            };

            let Some(_) = ct
                .run_until_cancelled(tokio::time::sleep(std::time::Duration::from_secs(5)))
                .await
            else {
                return Ok(());
            };
        };

        let segment_service = SegmentService::new(
            self.options.segment_size,
            chain_view.clone(),
            self.block_store_reader.clone(),
            self.block_store_writer.clone(),
            self.state_client.clone(),
            self.metrics.clone(),
        );

        let group_service = SegmentGroupService::new(
            self.options.segment_size,
            self.options.group_size,
            chain_view.clone(),
            self.block_store_reader.clone(),
            self.block_store_writer.clone(),
            self.state_client.clone(),
            self.metrics.clone(),
        );

        let prune_service = PruneService::new(
            self.options.segment_size,
            chain_view.clone(),
            self.block_store_writer.clone(),
            self.state_client.clone(),
            self.metrics.clone(),
        );

        let mut services = JoinSet::new();
        services.spawn(
            segment_service
                .start(ct.clone())
                .map(|result| ("segmentation", result)),
        );
        services.spawn(
            group_service
                .start(ct.clone())
                .map(|result| ("group service", result)),
        );
        services.spawn(
            prune_service
                .start(ct.clone())
                .map(|result| ("prune service", result)),
        );

        let lock_handle = lock_keep_alive_loop(lock, ct.clone());

        let result = tokio::select! {
            _ = ct.cancelled() => {
                Ok(())
            }
            lock = lock_handle => {
                info!("compaction lock loop terminated");
                lock.change_context(CompactionError)
            }
            Some(service) = services.join_next() => {
                match service {
                    Ok((name, result)) => {
                        info!("compaction {name} loop terminated");
                        result.change_context(CompactionError)
                    }
                    Err(err) => Err(err).change_context(CompactionError),
                }
            }
        };

        // The caller restarts compaction after an error. Wait for the remaining services to
        // stop so they don't keep running alongside the new ones.
        services.shutdown().await;

        result
    }
}

async fn lock_keep_alive_loop(
    lock: &mut Lock,
    ct: CancellationToken,
) -> Result<(), CompactionError> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = ct.cancelled() => {
                return Ok(());
            }
            _ = interval.tick() => {
                debug!("compaction: keep alive");
                lock.keep_alive().await.change_context(CompactionError)?;
            }
        }
    }
}

impl Default for CompactionServiceOptions {
    fn default() -> Self {
        Self {
            segment_size: 1_000,
            group_size: 100,
        }
    }
}
