use std::sync::Arc;

use apibara_etcd::EtcdClient;
use bytes::Bytes;
use error_stack::{Result, ResultExt};
use foyer::HybridCacheBuilder;
use testcontainers::{runners::AsyncRunner, ContainerAsync};

use apibara_dna_common::{
    chain::{BlockInfo, CanonicalBlock, CanonicalChainSegment, CanonicalChainSegmentInfo},
    chain_store::ChainStore,
    file_cache::FileCache,
    fragment,
    ingestion::{
        state_client::testing::{etcd_server_container, EtcdServer, EtcdServerExt},
        BlockIngestion, IngestionError, IngestionMetrics, IngestionService,
        IngestionServiceOptions, IngestionStateClient,
    },
    object_store::{
        testing::{minio_container, MinIO, MinIOExt},
        AwsS3Client, GetOptions, ObjectStore, ObjectStoreOptions, PutOptions,
    },
    Cursor, Hash,
};
use testing::{BlockNumberOrTag, TestChain};
use tokio_util::sync::CancellationToken;

async fn get_test_head(provider: &std::sync::Arc<TestChain>) -> apibara_dna_common::Cursor {
    use apibara_dna_common::{Cursor, Hash};
    let header = provider.get_header(BlockNumberOrTag::Latest).await;
    let hash = Hash(header.hash.to_vec());
    Cursor::new(header.number, hash)
}

async fn init_minio() -> (ContainerAsync<MinIO>, ObjectStore) {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;
    let s3_client = AwsS3Client::new_from_config(config);

    let client = ObjectStore::new_s3(
        s3_client,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    (minio, client)
}

async fn init_etcd_server() -> (ContainerAsync<EtcdServer>, EtcdClient) {
    let etcd_server = etcd_server_container().start().await.unwrap();
    let mut etcd_client = etcd_server.etcd_client().await;

    let st = etcd_client.status().await.unwrap();
    println!("{:?}", st);

    (etcd_server, etcd_client)
}

async fn init_test_chain() -> Arc<TestChain> {
    Arc::new(TestChain::new())
}

async fn init_file_cache() -> FileCache {
    let general = HybridCacheBuilder::default()
        .memory(1024 * 1024)
        .storage()
        .build()
        .await
        .expect("failed to create file cache");
    let index = HybridCacheBuilder::default()
        .memory(1024 * 1024)
        .storage()
        .build()
        .await
        .expect("failed to create file cache");
    FileCache { general, index }
}

#[tokio::test]
async fn test_chain_keeps_finalized_cursor_monotonic_across_reorgs() {
    let test_chain = TestChain::new();

    test_chain.mine(90, 0).await;
    let snapshot = test_chain.snapshot().await;
    test_chain.mine(10, 0).await;

    let finalized = test_chain.get_header(BlockNumberOrTag::Finalized).await;
    assert_eq!(finalized.number, 36);

    test_chain.reorg(5).await;
    let finalized_after_reorg = test_chain.get_header(BlockNumberOrTag::Finalized).await;
    assert_eq!(finalized_after_reorg.number, finalized.number);
    assert_eq!(finalized_after_reorg.hash, finalized.hash);

    test_chain.revert(snapshot).await;
    test_chain.mine(5, 0).await;

    let latest_after_revert = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(latest_after_revert.number, 95);
    let finalized_after_revert = test_chain.get_header(BlockNumberOrTag::Finalized).await;
    assert_eq!(finalized_after_revert.number, finalized.number);
    assert_eq!(finalized_after_revert.hash, finalized.hash);
}

#[tokio::test]
async fn test_ingestion_initialize() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let test_chain = init_test_chain().await;

    let mut state_client = IngestionStateClient::new(&etcd_client);
    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: test_chain.clone(),
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        IngestionServiceOptions::default(),
        IngestionMetrics::default(),
    );

    test_chain.mine(100, 3).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    let ingest_state = starting_state.as_ingest().unwrap();

    assert_eq!(ingest_state.head.number, 100);

    let starting_block = state_client.get_starting_block().await.unwrap();
    assert_eq!(starting_block, Some(0));

    let finalized = state_client.get_finalized().await.unwrap();
    assert!(finalized.is_some());
    assert_eq!(ingest_state.finalized.number, finalized.unwrap());

    let recent = state_client.get_recent().await.unwrap().unwrap();
    assert_eq!(recent.last_block, 0);
    assert!(recent.key.starts_with("canon/recent/"));
}

#[tokio::test]
async fn test_ingestion_initialize_with_starting_block() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let test_chain = init_test_chain().await;

    let mut state_client = IngestionStateClient::new(&etcd_client);
    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: test_chain.clone(),
    };

    let options = IngestionServiceOptions {
        override_starting_block: Some(100),
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
        IngestionMetrics::default(),
    );

    test_chain.mine(200, 3).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 200);

    let starting_state = service.initialize().await.unwrap();
    let ingest_state = starting_state.as_ingest().unwrap();

    assert_eq!(ingest_state.head.number, 200);

    let starting_block = state_client.get_starting_block().await.unwrap();
    assert_eq!(starting_block, Some(100));

    let finalized = state_client.get_finalized().await.unwrap();
    assert!(finalized.is_some());
    assert_eq!(ingest_state.finalized.number, finalized.unwrap());

    let recent = state_client.get_recent().await.unwrap().unwrap();
    assert_eq!(recent.last_block, 100);
    assert!(recent.key.starts_with("canon/recent/"));
}

#[tokio::test]
async fn test_ingestion_migrates_legacy_recent_segment_once() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let test_chain = init_test_chain().await;

    let genesis = test_chain.get_header(BlockNumberOrTag::Number(0)).await;
    let genesis_hash = Hash(genesis.hash.to_vec());
    let genesis_cursor = Cursor::new(0, genesis_hash.clone());
    let legacy_segment = CanonicalChainSegment {
        previous_segment: None,
        info: CanonicalChainSegmentInfo {
            first_block: genesis_cursor.clone(),
            last_block: genesis_cursor,
        },
        canonical: vec![CanonicalBlock {
            hash: genesis_hash,
            reorgs: Default::default(),
        }],
        extra_reorgs: Vec::new(),
    };
    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&legacy_segment).unwrap();
    object_store
        .put(
            "canon/recent",
            Bytes::copy_from_slice(serialized.as_slice()),
            PutOptions::default(),
        )
        .await
        .unwrap();

    // Seed the legacy etcd pointer key, as a pre-pointer ingester would have written it.
    etcd_client
        .kv_client()
        .put("ingestion/ingested", b"legacy-version")
        .await
        .unwrap();

    let block_ingestion = TestBlockIngestion {
        provider: test_chain.clone(),
    };
    let mut service = IngestionService::new(
        block_ingestion.clone(),
        etcd_client.clone(),
        object_store.clone(),
        init_file_cache().await,
        IngestionServiceOptions::default(),
        IngestionMetrics::default(),
    );

    service.initialize().await.unwrap();

    let mut state_client = IngestionStateClient::new(&etcd_client);
    let pointer = state_client.get_recent().await.unwrap().unwrap();
    assert_eq!(pointer.first_block, 0);
    assert_eq!(pointer.last_block, 0);
    assert!(pointer.key.starts_with("canon/recent/"));

    // Migration removes the legacy `canon/recent` object and the `ingestion/ingested` key.
    assert!(object_store
        .get("canon/recent", GetOptions::default())
        .await
        .is_err());
    assert!(state_client.get_legacy_ingested().await.unwrap().is_none());

    // Restart restores from the new pointer, not the (now-removed) legacy object.
    let mut restarted_service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        init_file_cache().await,
        IngestionServiceOptions::default(),
        IngestionMetrics::default(),
    );

    restarted_service.initialize().await.unwrap();
}

#[tokio::test]
async fn test_ingestion_advances_as_head_changes() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let test_chain = init_test_chain().await;

    let mut state_client = IngestionStateClient::new(&etcd_client);
    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: test_chain.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 10,
        chain_segment_upload_offset_size: 1,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store.clone(),
        file_cache,
        options,
        IngestionMetrics::default(),
    );

    test_chain.mine(10, 3).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 10);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    // Nothing changed, so state is the same.
    let state = starting_state.take_ingest().unwrap();
    let prev_head = state.head.clone();
    let head = get_test_head(&test_chain).await;
    let state = service.tick_refresh_head(state, head).await.unwrap();
    let state = state.take_ingest().unwrap();
    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head, prev_head);

    // We need a lot of blocks to push the finalized block forward.
    test_chain.mine(100, 3).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 110);

    let head = Cursor::new(header.number, Hash(header.hash.to_vec()));
    let state = service.tick_refresh_head(state, head).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 5);
    assert_eq!(state.head.number, header.number);
    assert_eq!(state.finalized.number, 46);
    assert_eq!(state.queued_block_number, 5);

    let mut state = Some(state);
    for offset in 0..5 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        assert_eq!(next_state.queued_block_number, 5 + offset + 1);
        state = Some(next_state);
    }

    for _ in 0..4 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    let join_result = service.task_queue_next().await;
    service
        .tick_with_task_result(state.take().unwrap(), join_result)
        .await
        .unwrap()
        .take_ingest()
        .unwrap();

    let recent = state_client.get_recent().await.unwrap().unwrap();
    assert_eq!(recent.last_block, 10);
    let chain_store = ChainStore::new(object_store.clone(), init_file_cache().await);
    let recent_segment = chain_store
        .get_recent_snapshot(&recent)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recent_segment.info.last_block.number, 10);
}

#[tokio::test]
async fn test_ingestion_not_affected_by_reorg_after_ingested_block() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let test_chain = init_test_chain().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: test_chain.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 10,
        chain_segment_upload_offset_size: 1,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
        IngestionMetrics::default(),
    );

    test_chain.mine(100, 3).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let head = get_test_head(&test_chain).await;
    let state = service.tick_refresh_head(state, head).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();
    let prev_head = state.head.clone();
    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    test_chain.reorg(5).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let head = Cursor::new(header.number, Hash(header.hash.to_vec()));
    let state = service.tick_refresh_head(state, head).await.unwrap();
    let state = state.take_ingest().unwrap();
    assert_ne!(state.head, prev_head);
}

#[tokio::test]
async fn test_ingestion_detect_shrinking_reorg_on_head_refresh() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let test_chain = init_test_chain().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: test_chain.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 100,
        chain_segment_upload_offset_size: 10,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
        IngestionMetrics::default(),
    );

    test_chain.mine(90, 3).await;
    let snapshot_id = test_chain.snapshot().await;
    test_chain.mine(10, 3).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let head = get_test_head(&test_chain).await;
    let state = service.tick_refresh_head(state, head).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    let mut state = Some(state);
    for _ in 0..=100 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    let state = state.unwrap();
    assert_eq!(state.last_ingested.number, 100);
    assert_eq!(service.task_queue_len(), 0);

    test_chain.revert(snapshot_id).await;
    test_chain.mine(5, 13).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 95);

    let chain_segment_before_recovery = service.current_chain_segment().unwrap();
    assert_eq!(chain_segment_before_recovery.info.last_block.number, 100);
    let block_before_recovery = chain_segment_before_recovery.canonical(95).unwrap();

    let head = get_test_head(&test_chain).await;
    let state = service.tick_refresh_head(state, head).await.unwrap();
    assert!(state.is_recover());

    let state = state.take_recover().unwrap();
    let ct = CancellationToken::new();
    let state = service.tick_recover(state, ct).await.unwrap();

    let state = state.take_ingest().unwrap();
    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.last_ingested.number, 90);

    let chain_segment_after_recovery = service.current_chain_segment().unwrap();
    assert_eq!(chain_segment_after_recovery.info.last_block.number, 90);
    let action = chain_segment_after_recovery
        .reconnect(&block_before_recovery)
        .unwrap();
    let reconnect_cursor = action.as_offline_reorg_cursor().unwrap();
    assert_eq!(reconnect_cursor.number, 90);
}

#[tokio::test]
async fn test_ingestion_detect_shrinking_reorg_on_block_ingestion() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let test_chain = init_test_chain().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: test_chain.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 100,
        chain_segment_upload_offset_size: 10,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
        IngestionMetrics::default(),
    );

    test_chain.mine(90, 3).await;
    let snapshot_id = test_chain.snapshot().await;
    test_chain.mine(10, 3).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let head = get_test_head(&test_chain).await;
    let state = service.tick_refresh_head(state, head).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    let mut state = Some(state);
    for _ in 0..=100 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    let state = state.unwrap();
    assert_eq!(state.last_ingested.number, 100);
    assert_eq!(service.task_queue_len(), 0);

    test_chain.revert(snapshot_id).await;
    test_chain.mine(5, 13).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 95);

    // Simulate a task queued before the reorg happened.
    service.push_ingest_block_by_number(101);
    let join_result = service.task_queue_next().await;
    let state = service
        .tick_with_task_result(state, join_result)
        .await
        .unwrap();

    assert!(state.is_recover());

    let chain_segment_before_recovery = service.current_chain_segment().unwrap();
    assert_eq!(chain_segment_before_recovery.info.last_block.number, 100);
    let block_before_recovery = chain_segment_before_recovery.canonical(95).unwrap();

    let state = state.take_recover().unwrap();
    let ct = CancellationToken::new();
    let state = service.tick_recover(state, ct).await.unwrap();

    let state = state.take_ingest().unwrap();
    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.last_ingested.number, 90);

    let chain_segment_after_recovery = service.current_chain_segment().unwrap();
    assert_eq!(chain_segment_after_recovery.info.last_block.number, 90);
    let action = chain_segment_after_recovery
        .reconnect(&block_before_recovery)
        .unwrap();
    let reconnect_cursor = action.as_offline_reorg_cursor().unwrap();
    assert_eq!(reconnect_cursor.number, 90);
}

#[tokio::test]
async fn test_ingestion_detect_offline_reorg() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let test_chain = init_test_chain().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: test_chain.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 100,
        chain_segment_upload_offset_size: 10,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion.clone(),
        etcd_client.clone(),
        object_store.clone(),
        file_cache.clone(),
        options.clone(),
        IngestionMetrics::default(),
    );

    test_chain.mine(100, 3).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let head = get_test_head(&test_chain).await;
    let state = service.tick_refresh_head(state, head).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    let mut state = Some(state);
    for _ in 0..=100 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    test_chain.reorg(10).await;
    test_chain.mine(20, 7).await;

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
        IngestionMetrics::default(),
    );

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);
    assert!(starting_state.is_recover());

    let chain_segment_before_recovery = service.current_chain_segment().unwrap();
    assert_eq!(chain_segment_before_recovery.info.last_block.number, 100);
    let block_before_recovery = chain_segment_before_recovery.canonical(95).unwrap();

    let state = starting_state.take_recover().unwrap();
    let ct = CancellationToken::new();
    let state = service.tick_recover(state, ct).await.unwrap();

    let state = state.take_ingest().unwrap();
    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.last_ingested.number, 90);

    let chain_segment_after_recovery = service.current_chain_segment().unwrap();
    assert_eq!(chain_segment_after_recovery.info.last_block.number, 90);
    let action = chain_segment_after_recovery
        .reconnect(&block_before_recovery)
        .unwrap();
    let reconnect_cursor = action.as_offline_reorg_cursor().unwrap();
    assert_eq!(reconnect_cursor.number, 90);
}

#[tokio::test]
async fn test_ingestion_detect_reorg_on_head_refresh() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let test_chain = init_test_chain().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: test_chain.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 100,
        chain_segment_upload_offset_size: 10,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
        IngestionMetrics::default(),
    );

    test_chain.mine(100, 3).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let head = get_test_head(&test_chain).await;
    let state = service.tick_refresh_head(state, head).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    let mut state = Some(state);
    for _ in 0..=100 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    let state = state.unwrap();
    assert_eq!(state.last_ingested.number, 100);
    assert_eq!(service.task_queue_len(), 0);

    test_chain.reorg(10).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let head = Cursor::new(header.number, Hash(header.hash.to_vec()));
    let state = service.tick_refresh_head(state, head).await.unwrap();
    assert!(state.is_recover());

    let chain_segment_before_recovery = service.current_chain_segment().unwrap();
    assert_eq!(chain_segment_before_recovery.info.last_block.number, 100);
    let block_before_recovery = chain_segment_before_recovery.canonical(95).unwrap();

    let state = state.take_recover().unwrap();
    let ct = CancellationToken::new();
    let state = service.tick_recover(state, ct).await.unwrap();

    let state = state.take_ingest().unwrap();
    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.last_ingested.number, 90);

    let chain_segment_after_recovery = service.current_chain_segment().unwrap();
    assert_eq!(chain_segment_after_recovery.info.last_block.number, 90);
    let action = chain_segment_after_recovery
        .reconnect(&block_before_recovery)
        .unwrap();
    let reconnect_cursor = action.as_offline_reorg_cursor().unwrap();
    assert_eq!(reconnect_cursor.number, 90);
}

#[tokio::test]
async fn test_ingestion_detect_reorg_on_block_ingestion() {
    let (_minio, object_store) = init_minio().await;
    let (_etcd_server, etcd_client) = init_etcd_server().await;
    let test_chain = init_test_chain().await;

    let file_cache = init_file_cache().await;

    let block_ingestion = TestBlockIngestion {
        provider: test_chain.clone(),
    };

    let options = IngestionServiceOptions {
        chain_segment_size: 100,
        chain_segment_upload_offset_size: 10,
        max_concurrent_tasks: 5,
        ..Default::default()
    };

    let mut service = IngestionService::new(
        block_ingestion,
        etcd_client,
        object_store,
        file_cache,
        options,
        IngestionMetrics::default(),
    );

    test_chain.mine(100, 3).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 100);

    let starting_state = service.initialize().await.unwrap();
    assert_eq!(service.task_queue_len(), 0);

    let state = starting_state.take_ingest().unwrap();
    let head = get_test_head(&test_chain).await;
    let state = service.tick_refresh_head(state, head).await.unwrap();
    let state = state.take_ingest().unwrap();
    let state = service.tick_refresh_finalized(state).await.unwrap();
    let state = state.take_ingest().unwrap();

    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.head.number, 100);

    let mut state = Some(state);
    for _ in 0..=100 {
        let join_result = service.task_queue_next().await;
        let next_state = service
            .tick_with_task_result(state.take().unwrap(), join_result)
            .await
            .unwrap();
        let next_state = next_state.take_ingest().unwrap();
        state = Some(next_state);
    }

    let state = state.unwrap();
    assert_eq!(state.last_ingested.number, 100);
    assert_eq!(service.task_queue_len(), 0);

    test_chain.reorg(10).await;
    test_chain.mine(20, 7).await;
    let header = test_chain.get_header(BlockNumberOrTag::Latest).await;
    assert_eq!(header.number, 120);

    let head = Cursor::new(header.number, Hash(header.hash.to_vec()));
    let state = service.tick_refresh_head(state, head).await.unwrap();
    let state = state.take_ingest().unwrap();
    assert_eq!(service.task_queue_len(), 5);

    let join_result = service.task_queue_next().await;
    let state = service
        .tick_with_task_result(state, join_result)
        .await
        .unwrap();

    assert!(state.is_recover());

    let chain_segment_before_recovery = service.current_chain_segment().unwrap();
    assert_eq!(chain_segment_before_recovery.info.last_block.number, 100);
    let block_before_recovery = chain_segment_before_recovery.canonical(95).unwrap();

    let state = state.take_recover().unwrap();
    let ct = CancellationToken::new();
    let state = service.tick_recover(state, ct).await.unwrap();

    let state = state.take_ingest().unwrap();
    assert_eq!(service.task_queue_len(), 0);
    assert_eq!(state.last_ingested.number, 90);

    let chain_segment_after_recovery = service.current_chain_segment().unwrap();
    assert_eq!(chain_segment_after_recovery.info.last_block.number, 90);
    let action = chain_segment_after_recovery
        .reconnect(&block_before_recovery)
        .unwrap();
    let reconnect_cursor = action.as_offline_reorg_cursor().unwrap();
    assert_eq!(reconnect_cursor.number, 90);
}

#[derive(Clone)]
struct TestBlockIngestion {
    provider: Arc<TestChain>,
}

impl BlockIngestion for TestBlockIngestion {
    async fn get_head_cursor(&self) -> Result<Cursor, IngestionError> {
        let header = self.provider.get_header(BlockNumberOrTag::Latest).await;
        let hash = Hash(header.hash.to_vec());
        Ok(Cursor::new(header.number, hash))
    }

    async fn get_finalized_cursor(&self) -> Result<Cursor, IngestionError> {
        let header = self.provider.get_header(BlockNumberOrTag::Finalized).await;
        let hash = Hash(header.hash.to_vec());
        Ok(Cursor::new(header.number, hash))
    }

    async fn get_block_info_by_number(&self, number: u64) -> Result<BlockInfo, IngestionError> {
        let Some(header) = self
            .provider
            .get_maybe_header(BlockNumberOrTag::Number(number))
            .await
        else {
            return Err(IngestionError::BlockNotFound).attach_printable("missing block");
        };
        let hash = Hash(header.hash.to_vec());
        let parent_hash = Hash(header.parent_hash.to_vec());

        Ok(BlockInfo {
            number,
            hash,
            parent: parent_hash,
        })
    }

    async fn ingest_block_by_number(
        &self,
        number: u64,
    ) -> Result<(BlockInfo, fragment::Block), IngestionError> {
        let info = self.get_block_info_by_number(number).await?;

        let header = fragment::HeaderFragment {
            data: Vec::default(),
        };
        let index = fragment::IndexGroupFragment {
            indexes: Vec::default(),
        };
        let join = fragment::JoinGroupFragment {
            joins: Vec::default(),
        };

        let block = fragment::Block {
            header,
            index,
            join,
            body: Vec::default(),
        };

        Ok((info, block))
    }
}

pub mod testing {
    use tokio::sync::RwLock;

    const FINALIZATION_DEPTH: u64 = 64;

    #[derive(Clone, Copy)]
    pub enum BlockNumberOrTag {
        Latest,
        Finalized,
        Number(u64),
    }

    #[derive(Clone)]
    pub struct Header {
        pub number: u64,
        pub hash: Vec<u8>,
        pub parent_hash: Vec<u8>,
    }

    pub struct TestChainSnapshot {
        blocks: Vec<Header>,
    }

    struct TestChainState {
        blocks: Vec<Header>,
        next_hash_id: u64,
        finalized_number: u64,
    }

    pub struct TestChain {
        state: RwLock<TestChainState>,
    }

    impl Default for TestChain {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TestChain {
        pub fn new() -> Self {
            Self {
                state: RwLock::new(TestChainState {
                    blocks: vec![Header {
                        number: 0,
                        hash: Self::hash(0, 0),
                        parent_hash: vec![0; 32],
                    }],
                    next_hash_id: 1,
                    finalized_number: 0,
                }),
            }
        }

        fn hash(number: u64, id: u64) -> Vec<u8> {
            let mut hash = vec![0; 32];
            hash[..8].copy_from_slice(&number.to_be_bytes());
            hash[8..16].copy_from_slice(&id.to_be_bytes());
            hash
        }

        fn append_block(state: &mut TestChainState) {
            let parent = state.blocks.last().expect("genesis block exists");
            let number = parent.number + 1;
            let parent_hash = parent.hash.clone();
            let hash = Self::hash(number, state.next_hash_id);
            state.next_hash_id += 1;
            state.blocks.push(Header {
                number,
                hash,
                parent_hash,
            });
            state.finalized_number = state
                .finalized_number
                .max(number.saturating_sub(FINALIZATION_DEPTH));
        }

        pub async fn get_maybe_header(&self, block: BlockNumberOrTag) -> Option<Header> {
            let state = self.state.read().await;
            match block {
                BlockNumberOrTag::Latest => state.blocks.last().cloned(),
                BlockNumberOrTag::Finalized => {
                    state.blocks.get(state.finalized_number as usize).cloned()
                }
                BlockNumberOrTag::Number(number) => state.blocks.get(number as usize).cloned(),
            }
        }

        pub async fn get_header(&self, block: BlockNumberOrTag) -> Header {
            self.get_maybe_header(block)
                .await
                .expect("test chain header must exist")
        }

        pub async fn mine(&self, block_count: u64, _interval_sec: u64) {
            let mut state = self.state.write().await;
            for _ in 0..block_count {
                Self::append_block(&mut state);
            }
        }

        pub async fn reorg(&self, block_count: u64) {
            let mut state = self.state.write().await;
            assert!(block_count < state.blocks.len() as u64);
            let new_len = state.blocks.len() - block_count as usize;
            let fork_number = state.blocks[new_len - 1].number;
            assert!(
                fork_number >= state.finalized_number,
                "cannot reorg finalized blocks"
            );
            state.blocks.truncate(new_len);
            for _ in 0..block_count {
                Self::append_block(&mut state);
            }
        }

        pub async fn snapshot(&self) -> TestChainSnapshot {
            TestChainSnapshot {
                blocks: self.state.read().await.blocks.clone(),
            }
        }

        pub async fn revert(&self, snapshot: TestChainSnapshot) {
            let snapshot_head = snapshot.blocks.last().expect("genesis block exists").number;
            let mut state = self.state.write().await;
            assert!(
                snapshot_head >= state.finalized_number,
                "cannot revert finalized blocks"
            );
            state.blocks = snapshot.blocks;
        }
    }
}
