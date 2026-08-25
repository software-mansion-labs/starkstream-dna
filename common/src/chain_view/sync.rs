use std::time::Duration;

use error_stack::{Result, ResultExt};
use futures::TryStreamExt;
use starkstream_dna_etcd::EtcdClient;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    chain::CanonicalChainSegment,
    chain_store::ChainStore,
    file_cache::FileCache,
    ingestion::{IngestionStateClient, IngestionStateUpdate},
    object_store::ObjectStore,
    options_store::OptionsStore,
};

use super::{error::ChainViewError, full::FullCanonicalChain, view::ChainView};

pub struct ChainViewSyncService {
    tx: tokio::sync::watch::Sender<Option<ChainView>>,
    etcd_client: EtcdClient,
    chain_store: ChainStore,
}

impl ChainViewSyncService {
    fn new(
        tx: tokio::sync::watch::Sender<Option<ChainView>>,
        chain_file_cache: FileCache,
        etcd_client: EtcdClient,
        object_store: ObjectStore,
    ) -> Self {
        let chain_store = ChainStore::new(object_store, chain_file_cache);
        Self {
            tx,
            etcd_client,
            chain_store,
        }
    }

    /// Read the recent canonical chain segment currently published by the ingester.
    ///
    /// Returns `None` when no segment is available yet, so callers can simply retry. That
    /// covers both an ingester that has not published anything and a pointer that has
    /// already been reclaimed by the cleanup job.
    async fn get_recent(
        &self,
        state_client: &mut IngestionStateClient,
    ) -> Result<Option<CanonicalChainSegment>, ChainViewError> {
        let Some(pointer) = state_client
            .get_recent()
            .await
            .change_context(ChainViewError)?
        else {
            return self.get_legacy_recent(state_client).await;
        };

        let recent = self
            .chain_store
            .get_recent_snapshot(&pointer)
            .await
            .change_context(ChainViewError)
            .attach_printable("failed to get recent canonical chain snapshot")?;

        if recent.is_none() {
            // The pointer references an object that no longer exists. It is likely
            // stale (superseded then reclaimed by cleanup); the caller retries to pick
            // up the pointer the ingester publishes next, rather than failing to start.
            warn!(
                key = %pointer.key,
                "chain_view: recent snapshot pointer references a missing object; retrying"
            );
        }

        Ok(recent)
    }

    /// Backwards-compatibility read for the legacy single-object recent layout.
    ///
    /// Returns the `canon/recent` segment when the legacy `ingestion/ingested` key is
    /// still present (i.e. an ingester that predates the pointer layout). Returns `None`
    /// once the ingester has migrated to the pointer-based layout.
    ///
    /// Deploy consumers before ingesters so this path is available during the transition.
    async fn get_legacy_recent(
        &self,
        state_client: &mut IngestionStateClient,
    ) -> Result<Option<CanonicalChainSegment>, ChainViewError> {
        if state_client
            .get_legacy_ingested()
            .await
            .change_context(ChainViewError)?
            .is_none()
        {
            return Ok(None);
        }

        let recent = self
            .chain_store
            .get_legacy_recent()
            .await
            .change_context(ChainViewError)
            .attach_printable("failed to get legacy recent canonical chain segment")?;

        if recent.is_some() {
            warn!("chain_view: using legacy recent canonical chain segment");
        }

        Ok(recent)
    }

    pub async fn start(self, ct: CancellationToken) -> Result<(), ChainViewError> {
        info!("chain_view: starting chain view sync service");
        let mut ingestion_state_client = IngestionStateClient::new(&self.etcd_client);
        // A second client used for recovery reads while `ingestion_state_client` is tied up
        // by the watch stream.
        let mut recovery_state_client = IngestionStateClient::new(&self.etcd_client);

        let starting_block = loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            let starting_block = ingestion_state_client
                .get_starting_block()
                .await
                .change_context(ChainViewError)?;

            if let Some(starting_block) = starting_block {
                break starting_block;
            }

            info!(
                step = "starting_block",
                "chain_view: waiting for ingestion to start"
            );
            tokio::time::sleep(Duration::from_secs(10)).await;
        };

        let finalized = loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            let finalized = ingestion_state_client
                .get_finalized()
                .await
                .change_context(ChainViewError)?;

            if let Some(finalized) = finalized {
                break finalized;
            }

            info!(
                step = "finalized_block",
                "chain_view: waiting for ingestion to start"
            );
            tokio::time::sleep(Duration::from_secs(10)).await;
        };

        let segmented = ingestion_state_client
            .get_segmented()
            .await
            .change_context(ChainViewError)?;

        let grouped = ingestion_state_client
            .get_grouped()
            .await
            .change_context(ChainViewError)?;

        let recent = loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            if let Some(recent) = self.get_recent(&mut ingestion_state_client).await? {
                break recent;
            }

            info!(
                step = "recent",
                "chain_view: waiting for ingestion to start"
            );
            tokio::time::sleep(Duration::from_secs(10)).await;
        };

        if ct.is_cancelled() {
            return Ok(());
        }

        let mut options_store = OptionsStore::new(&self.etcd_client);
        let chain_segment_size = options_store
            .get_chain_segment_size()
            .await
            .change_context(ChainViewError)
            .attach_printable("failed to get chain segment size options")?
            .ok_or(ChainViewError)
            .attach_printable("chain segment size option not found")?;

        let canonical_chain = FullCanonicalChain::initialize(
            self.chain_store.clone(),
            starting_block,
            chain_segment_size,
            recent,
        )
        .await?;

        let segment_size = options_store
            .get_segment_size()
            .await
            .change_context(ChainViewError)
            .attach_printable("failed to get segment size options")?
            .ok_or(ChainViewError)
            .attach_printable("segment size option not found")?;

        let group_size = options_store
            .get_group_size()
            .await
            .change_context(ChainViewError)
            .attach_printable("failed to get group size options")?
            .ok_or(ChainViewError)
            .attach_printable("group size option not found")?;

        let chain_view = ChainView::new(
            finalized,
            segmented,
            grouped,
            segment_size as u64,
            group_size as u64,
            canonical_chain,
        );

        chain_view.record_starting_metrics().await?;

        self.tx
            .send(Some(chain_view.clone()))
            .change_context(ChainViewError)?;

        info!("chain_view: initialized");

        if ct.is_cancelled() {
            return Ok(());
        }

        loop {
            let loop_result: Result<(), ChainViewError> = async {
                let state_changes = ingestion_state_client
                    .watch_changes(ct.clone())
                    .await
                    .change_context(ChainViewError)?;

                tokio::pin!(state_changes);

                info!("chain_view: streaming state changes");
                chain_view.record_is_up().await?;
                while let Some(update) = state_changes
                    .try_next()
                    .await
                    .change_context(ChainViewError)?
                {
                    if !update.is_pending() {
                        info!(update = ?update, "chain_view: sync update");
                    } else {
                        debug!(update = ?update, "chain_view: sync update");
                    }

                    match update {
                        IngestionStateUpdate::StartingBlock(block) => {
                            // The starting block should never be updated.
                            warn!(starting_block = block, "chain view starting block updated");
                        }
                        IngestionStateUpdate::Finalized(block) => {
                            chain_view.set_finalized_block(block).await;
                        }
                        IngestionStateUpdate::Segmented(block) => {
                            chain_view.set_segmented_block(block).await;
                        }
                        IngestionStateUpdate::Grouped(block) => {
                            chain_view.set_grouped_block(block).await;
                        }
                        IngestionStateUpdate::Pending(generation) => {
                            chain_view.set_pending_generation(generation).await;
                        }
                        IngestionStateUpdate::Recent(pointer) => {
                            let recent = match self
                                .chain_store
                                .get_recent_snapshot(&pointer)
                                .await
                                .change_context(ChainViewError)
                                .attach_printable("failed to get recent canonical chain snapshot")?
                            {
                                Some(recent) => Some(recent),
                                None => {
                                    // This pointer is likely stale: a newer snapshot has
                                    // superseded it and the old object was already reclaimed
                                    // by the cleanup job. Re-read the current pointer and use
                                    // that instead of tearing down the watch stream.
                                    warn!(
                                        key = %pointer.key,
                                        "chain_view: recent snapshot missing; re-reading current pointer"
                                    );
                                    match recovery_state_client
                                        .get_recent()
                                        .await
                                        .change_context(ChainViewError)?
                                    {
                                        Some(current) if current.key != pointer.key => self
                                            .chain_store
                                            .get_recent_snapshot(&current)
                                            .await
                                            .change_context(ChainViewError)
                                            .attach_printable(
                                                "failed to get recent canonical chain snapshot",
                                            )?,
                                        _ => None,
                                    }
                                }
                            };

                            let Some(recent) = recent else {
                                warn!(
                                    key = %pointer.key,
                                    "chain_view: skipping recent update; snapshot unavailable"
                                );
                                continue;
                            };
                            chain_view.set_recent(recent).await?;
                        }
                    }

                    self.tx
                        .send(Some(chain_view.clone()))
                        .change_context(ChainViewError)?;
                }

                Err(ChainViewError).attach_printable("chain view loop ended")
            }
            .await;

            if ct.is_cancelled() {
                return Ok(());
            }

            chain_view.record_is_down().await?;

            if let Err(inner_error) = loop_result {
                error!(error = ?inner_error, "chain_view: error");
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
            info!("chain_view: retrying chain view loop");
        }
    }
}

pub async fn chain_view_sync_loop(
    chain_file_cache: FileCache,
    etcd_client: EtcdClient,
    object_store: ObjectStore,
) -> Result<
    (
        tokio::sync::watch::Receiver<Option<ChainView>>,
        ChainViewSyncService,
    ),
    ChainViewError,
> {
    let (tx, rx) = tokio::sync::watch::channel(None);

    let sync_service = ChainViewSyncService::new(tx, chain_file_cache, etcd_client, object_store);

    Ok((rx, sync_service))
}
