use starkstream_dna_common::ingestion::{
    state_client::testing::{etcd_server_container, EtcdServerExt},
    IngestionStateClient,
};
use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_segmented_and_grouped_never_move_backwards() {
    let etcd_server = etcd_server_container().start().await.unwrap();
    let etcd_client = etcd_server.etcd_client().await;
    let mut state_client = IngestionStateClient::new(&etcd_client);

    state_client.put_segmented(199).await.unwrap();
    state_client.put_segmented(99).await.unwrap();
    assert_eq!(state_client.get_segmented().await.unwrap(), Some(199));

    state_client.put_segmented(299).await.unwrap();
    assert_eq!(state_client.get_segmented().await.unwrap(), Some(299));

    state_client.put_grouped(19_999).await.unwrap();
    state_client.put_grouped(9_999).await.unwrap();
    assert_eq!(state_client.get_grouped().await.unwrap(), Some(19_999));

    state_client.put_grouped(29_999).await.unwrap();
    assert_eq!(state_client.get_grouped().await.unwrap(), Some(29_999));
}
