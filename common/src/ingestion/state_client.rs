use apibara_etcd::{EtcdClient, KvClient, WatchClient};
use error_stack::{Result, ResultExt};
use futures::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::chain_store::RecentSegmentPointer;

pub static INGESTION_PREFIX_KEY: &str = "ingestion/";
pub static RECENT_KEY: &str = "ingestion/recent";
// Legacy key written by ingesters that predate the pointer-based recent layout. It held
// the object version of the single mutable `canon/recent` object. Kept only so the new
// code can read it (consumer fallback) and clean it up after migration.
pub static LEGACY_INGESTED_KEY: &str = "ingestion/ingested";
pub static PENDING_KEY: &str = "ingestion/pending";
pub static STARTING_BLOCK_KEY: &str = "ingestion/starting_block";
pub static FINALIZED_KEY: &str = "ingestion/finalized";
pub static SEGMENTED_KEY: &str = "ingestion/segmented";
pub static GROUPED_KEY: &str = "ingestion/grouped";

// Use a different prefix for pruned blocks to avoid overwhelming the compaction state store
pub static PRUNED_KEY: &str = "compaction/pruned";

#[derive(Debug)]
pub struct IngestionStateClientError;

#[derive(Clone)]
pub struct IngestionStateClient {
    kv_client: KvClient,
    watch_client: WatchClient,
}

#[derive(Clone)]
pub enum IngestionStateUpdate {
    StartingBlock(u64),
    Finalized(u64),
    Pending(Option<u64>),
    Segmented(u64),
    Grouped(u64),
    Recent(RecentSegmentPointer),
}

impl std::fmt::Debug for IngestionStateUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartingBlock(block) => f.debug_tuple("StartingBlock").field(block).finish(),
            Self::Finalized(block) => f.debug_tuple("Finalized").field(block).finish(),
            Self::Pending(generation) => f.debug_tuple("Pending").field(generation).finish(),
            Self::Segmented(block) => f.debug_tuple("Segmented").field(block).finish(),
            Self::Grouped(block) => f.debug_tuple("Grouped").field(block).finish(),
            Self::Recent(pointer) => f
                .debug_struct("Recent")
                .field("key", &pointer.key)
                .field("version", &pointer.version)
                .field("first_block", &pointer.first_block)
                .field("last_block", &pointer.last_block)
                .finish(),
        }
    }
}

impl IngestionStateClient {
    pub fn new(client: &EtcdClient) -> Self {
        let kv_client = client.kv_client();
        let watch_client = client.watch_client();

        Self {
            kv_client,
            watch_client,
        }
    }

    pub async fn watch_changes(
        &mut self,
        ct: CancellationToken,
    ) -> Result<
        impl Stream<Item = Result<IngestionStateUpdate, IngestionStateClientError>>,
        IngestionStateClientError,
    > {
        let (_watcher, stream) = self
            .watch_client
            .watch_prefix(INGESTION_PREFIX_KEY, ct)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to watch ingestion state")?;

        let changes = stream.flat_map(|response| {
            let response = match response {
                Err(err) => {
                    return futures::stream::iter(vec![
                        Err(err).change_context(IngestionStateClientError)
                    ]);
                }
                Ok(response) => response,
            };

            let changes = response
                .events()
                .iter()
                .filter_map(|event| {
                    let kv = event.kv()?;

                    match IngestionStateUpdate::from_kv(kv) {
                        Ok(Some(update)) => Some(Ok(update)),
                        Ok(None) => None,
                        Err(err) => Some(Err(err)),
                    }
                })
                .collect::<Vec<Result<IngestionStateUpdate, _>>>();
            futures::stream::iter(changes)
        });

        Ok(changes)
    }

    pub async fn get_starting_block(&mut self) -> Result<Option<u64>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(STARTING_BLOCK_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get starting block")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode starting block")?;

        let block = value
            .parse::<u64>()
            .change_context(IngestionStateClientError)
            .attach_printable("failed to parse starting block")?;

        Ok(Some(block))
    }

    pub async fn put_starting_block(
        &mut self,
        block: u64,
    ) -> Result<(), IngestionStateClientError> {
        let value = block.to_string();
        self.kv_client
            .put(STARTING_BLOCK_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put starting block")?;

        Ok(())
    }

    pub async fn get_finalized(&mut self) -> Result<Option<u64>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(FINALIZED_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get finalized block")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode finalized block")?;

        let block = value
            .parse::<u64>()
            .change_context(IngestionStateClientError)
            .attach_printable("failed to parse finalized block")?;

        Ok(Some(block))
    }

    pub async fn put_finalized(&mut self, block: u64) -> Result<(), IngestionStateClientError> {
        let value = block.to_string();
        self.kv_client
            .put(FINALIZED_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put finalized block")?;

        Ok(())
    }

    pub async fn get_recent(
        &mut self,
    ) -> Result<Option<RecentSegmentPointer>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(RECENT_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get recent canonical chain segment")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        RecentSegmentPointer::try_from(kv.value()).map(Some)
    }

    pub async fn put_recent(
        &mut self,
        pointer: &RecentSegmentPointer,
    ) -> Result<(), IngestionStateClientError> {
        let value: Vec<u8> = pointer.try_into()?;
        self.kv_client
            .put_and_delete(RECENT_KEY, value, PENDING_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put recent canonical chain segment")?;

        Ok(())
    }

    /// Read the legacy `ingestion/ingested` key, returning its raw value if present.
    ///
    /// Used as a backwards-compatibility fallback when the new `ingestion/recent` pointer
    /// is not yet published (e.g. a consumer running against an ingester that has not been
    /// upgraded). Presence is what matters; the value is the legacy object version.
    pub async fn get_legacy_ingested(
        &mut self,
    ) -> Result<Option<String>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(LEGACY_INGESTED_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get legacy ingested key")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode legacy ingested value")?;

        Ok(Some(value))
    }

    /// Delete the legacy `ingestion/ingested` key. Idempotent: deleting a missing key is a
    /// no-op in etcd.
    pub async fn delete_legacy_ingested(&mut self) -> Result<(), IngestionStateClientError> {
        self.kv_client
            .delete(LEGACY_INGESTED_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to delete legacy ingested key")?;

        Ok(())
    }

    pub async fn put_pending(&mut self, generation: u64) -> Result<(), IngestionStateClientError> {
        let value = generation.to_string();
        self.kv_client
            .put(PENDING_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put pending block generation")?;

        Ok(())
    }

    pub async fn get_segmented(&mut self) -> Result<Option<u64>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(SEGMENTED_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get segmented block")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode segmented block")?;

        let block = value
            .parse::<u64>()
            .change_context(IngestionStateClientError)
            .attach_printable("failed to parse segmented block")?;

        Ok(Some(block))
    }

    pub async fn put_segmented(&mut self, block: u64) -> Result<(), IngestionStateClientError> {
        let value = block.to_string();
        self.kv_client
            .put(SEGMENTED_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put segmented block")?;

        Ok(())
    }

    pub async fn get_grouped(&mut self) -> Result<Option<u64>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(GROUPED_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get grouped block")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode grouped block")?;

        let block = value
            .parse::<u64>()
            .change_context(IngestionStateClientError)
            .attach_printable("failed to parse grouped block")?;

        Ok(Some(block))
    }

    pub async fn put_grouped(&mut self, block: u64) -> Result<(), IngestionStateClientError> {
        let value = block.to_string();
        self.kv_client
            .put(GROUPED_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put grouped block")?;

        Ok(())
    }

    pub async fn get_pruned(&mut self) -> Result<Option<u64>, IngestionStateClientError> {
        let response = self
            .kv_client
            .get(PRUNED_KEY)
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to get pruned block")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode pruned block")?;

        let block = value
            .parse::<u64>()
            .change_context(IngestionStateClientError)
            .attach_printable("failed to parse pruned block")?;

        Ok(Some(block))
    }

    pub async fn put_pruned(&mut self, block: u64) -> Result<(), IngestionStateClientError> {
        let value = block.to_string();
        self.kv_client
            .put(PRUNED_KEY, value.as_bytes())
            .await
            .change_context(IngestionStateClientError)
            .attach_printable("failed to put pruned block")?;

        Ok(())
    }
}

impl error_stack::Context for IngestionStateClientError {}

impl std::fmt::Display for IngestionStateClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ingestion state client error")
    }
}

impl IngestionStateUpdate {
    pub fn is_pending(&self) -> bool {
        matches!(self, IngestionStateUpdate::Pending(_))
    }

    pub fn from_kv(kv: &etcd_client::KeyValue) -> Result<Option<Self>, IngestionStateClientError> {
        let key = String::from_utf8(kv.key().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode key")?;

        if key.ends_with(RECENT_KEY) {
            return RecentSegmentPointer::try_from(kv.value())
                .map(Self::Recent)
                .map(Some);
        }

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(IngestionStateClientError)
            .attach_printable("failed to decode value")?;

        if key.ends_with(STARTING_BLOCK_KEY) {
            let block = value
                .parse::<u64>()
                .change_context(IngestionStateClientError)
                .attach_printable("failed to parse starting block")?;
            Ok(Some(IngestionStateUpdate::StartingBlock(block)))
        } else if key.ends_with(FINALIZED_KEY) {
            let block = value
                .parse::<u64>()
                .change_context(IngestionStateClientError)
                .attach_printable("failed to parse finalized block")?;
            Ok(Some(IngestionStateUpdate::Finalized(block)))
        } else if key.ends_with(PENDING_KEY) {
            if value.is_empty() {
                Ok(Some(IngestionStateUpdate::Pending(None)))
            } else {
                let generation = value
                    .parse::<u64>()
                    .change_context(IngestionStateClientError)
                    .attach_printable("failed to parse pending block generation")?;
                Ok(Some(IngestionStateUpdate::Pending(Some(generation))))
            }
        } else if key.ends_with(SEGMENTED_KEY) {
            let block = value
                .parse::<u64>()
                .change_context(IngestionStateClientError)
                .attach_printable("failed to parse segmented block")?;
            Ok(Some(IngestionStateUpdate::Segmented(block)))
        } else if key.ends_with(GROUPED_KEY) {
            let block = value
                .parse::<u64>()
                .change_context(IngestionStateClientError)
                .attach_printable("failed to parse grouped block")?;
            Ok(Some(IngestionStateUpdate::Grouped(block)))
        } else {
            Ok(None)
        }
    }
}

impl TryFrom<&RecentSegmentPointer> for Vec<u8> {
    type Error = error_stack::Report<IngestionStateClientError>;

    fn try_from(pointer: &RecentSegmentPointer) -> std::result::Result<Self, Self::Error> {
        serde_json::to_vec(pointer)
            .change_context(IngestionStateClientError)
            .attach_printable("failed to serialize recent canonical chain pointer")
    }
}

impl TryFrom<&[u8]> for RecentSegmentPointer {
    type Error = error_stack::Report<IngestionStateClientError>;

    fn try_from(bytes: &[u8]) -> std::result::Result<Self, Self::Error> {
        serde_json::from_slice(bytes)
            .change_context(IngestionStateClientError)
            .attach_printable("failed to deserialize recent canonical chain pointer")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_pointer_serialization_roundtrip() {
        let pointer = RecentSegmentPointer {
            key: "canon/recent/z-0000000001-0000000042-01718123456789123456-000001".to_string(),
            version: "123456789".to_string(),
            first_block: 1,
            last_block: 42,
        };

        let encoded: Vec<u8> = (&pointer).try_into().unwrap();
        assert!(std::str::from_utf8(&encoded).is_ok());
        let decoded = RecentSegmentPointer::try_from(encoded.as_slice()).unwrap();

        assert_eq!(decoded, pointer);
    }
}

pub mod testing {
    use apibara_etcd::EtcdClient;
    use futures::Future;
    use testcontainers::{
        core::{wait::LogWaitStrategy, ContainerPort, WaitFor},
        ContainerAsync, Image,
    };

    pub struct EtcdServer;

    pub trait EtcdServerExt {
        fn etcd_client(&self) -> impl Future<Output = EtcdClient> + Send;
    }

    impl Image for EtcdServer {
        fn name(&self) -> &str {
            "gcr.io/etcd-development/etcd"
        }

        fn tag(&self) -> &str {
            "v3.5.33"
        }

        fn ready_conditions(&self) -> Vec<WaitFor> {
            vec![WaitFor::log(LogWaitStrategy::stdout_or_stderr(
                "serving client traffic insecurely",
            ))]
        }

        fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
            vec![
                "/usr/local/bin/etcd",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://0.0.0.0:2379",
            ]
        }

        fn expose_ports(&self) -> &[ContainerPort] {
            &[ContainerPort::Tcp(2379)]
        }
    }

    pub fn etcd_server_container() -> EtcdServer {
        EtcdServer
    }

    impl EtcdServerExt for ContainerAsync<EtcdServer> {
        async fn etcd_client(&self) -> EtcdClient {
            let port = self
                .get_host_port_ipv4(2379)
                .await
                .expect("Etcd port 2379 not found");

            let endpoint = format!("http://localhost:{port}");
            EtcdClient::connect(&[endpoint], Default::default())
                .await
                .expect("Etcd connection error")
        }
    }
}
