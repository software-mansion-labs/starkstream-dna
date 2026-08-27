use testcontainers::{runners::AsyncRunner, ContainerAsync};

use starkstream_dna_common::object_store::{
    testing::{
        self, azurite_container, fake_gcs_server_container, minio_container, AzuriteExt,
        FakeGcsServerExt, MinIOExt,
    },
    AwsS3Client, AzureBlobClient, DeleteOptions, GetOptions, ObjectStore, ObjectStoreClient,
    ObjectStoreError, ObjectStoreOptions, ObjectStoreResultExt, ObjectVersion, PutMode, PutOptions,
};

async fn start_minio() -> (ContainerAsync<testing::MinIO>, ObjectStoreClient) {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;
    let client = AwsS3Client::new_from_config(config);
    (minio, client.into())
}

async fn start_azurite() -> (ContainerAsync<testing::Azurite>, ObjectStoreClient) {
    let azurite = azurite_container().start().await.unwrap();
    let client = AzureBlobClient::new(azurite.location().await, azurite.credentials());
    (azurite, client.into())
}

async fn start_fake_gcs_server() -> (ContainerAsync<testing::FakeGcsServer>, ObjectStoreClient) {
    let server = fake_gcs_server_container().start().await.unwrap();
    let client = server.gcs_client().await.unwrap();
    (server, client.into())
}

async fn dot_put_and_get_no_prefix_no_precondition(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );
    client.ensure_bucket().await.unwrap();

    let put_res = client
        .put("test", "Hello, World".into(), PutOptions::default())
        .await
        .unwrap();

    assert!(!put_res.version.0.is_empty());

    let get_res = client.get("test", GetOptions::default()).await.unwrap();
    assert_eq!(get_res.version, put_res.version);
    assert_eq!(get_res.body, "Hello, World".as_bytes());
}

#[tokio::test]
async fn test_s3_put_and_get_no_prefix_no_precondition() {
    let (_minio, client) = start_minio().await;
    dot_put_and_get_no_prefix_no_precondition(client).await;
}

#[tokio::test]
async fn test_azure_blob_put_and_get_no_prefix_no_precondition() {
    let (_azurite, client) = start_azurite().await;
    dot_put_and_get_no_prefix_no_precondition(client).await;
}

#[tokio::test]
async fn test_gcs_put_and_get_no_prefix_no_precondition() {
    let (_server, client) = start_fake_gcs_server().await;
    dot_put_and_get_no_prefix_no_precondition(client).await;
}

// Put an object in the bucket with prefix.
// Put an object with the same filename in the bucket without prefix.
// Check that they are indeed different.
async fn do_put_and_get_with_prefix_no_precondition(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner.clone(),
        ObjectStoreOptions {
            bucket: "test".to_string(),
            prefix: Some("my-prefix".to_string()),
        },
    );

    client.ensure_bucket().await.unwrap();

    client
        .put("test", "With my-prefix".into(), PutOptions::default())
        .await
        .unwrap();

    {
        let client = ObjectStore::new(
            inner,
            ObjectStoreOptions {
                bucket: "test".to_string(),
                prefix: None,
            },
        );
        client
            .put("test", "Without prefix".into(), PutOptions::default())
            .await
            .unwrap();
    }

    let get_res = client.get("test", GetOptions::default()).await.unwrap();
    assert_eq!(get_res.body, "With my-prefix".as_bytes());
}

#[tokio::test]
async fn test_s3_put_and_get_with_prefix_no_precondition() {
    let (_minio, client) = start_minio().await;
    do_put_and_get_with_prefix_no_precondition(client).await;
}

#[tokio::test]
async fn test_azure_put_and_get_with_prefix_no_precondition() {
    let (_azurite, client) = start_azurite().await;
    do_put_and_get_with_prefix_no_precondition(client).await;
}

#[tokio::test]
async fn test_gcs_put_and_get_with_prefix_no_precondition() {
    let (_server, client) = start_fake_gcs_server().await;
    do_put_and_get_with_prefix_no_precondition(client).await;
}

async fn do_test_get_with_version(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    let put_res = client
        .put("test", "Hello, World".into(), PutOptions::default())
        .await
        .unwrap();

    client
        .get(
            "test",
            GetOptions {
                version: Some(put_res.version),
            },
        )
        .await
        .unwrap();

    let response = client
        .get(
            "test",
            GetOptions {
                version: Some(ObjectVersion("123456789".to_string())),
            },
        )
        .await;

    assert!(response.is_err());
    assert!(response.unwrap_err().is_precondition());
}

#[tokio::test]
async fn test_s3_get_with_etag() {
    let (_minio, client) = start_minio().await;
    do_test_get_with_version(client).await;
}

#[tokio::test]
async fn test_azure_get_with_etag() {
    let (_azurite, client) = start_azurite().await;
    do_test_get_with_version(client).await;
}

#[tokio::test]
// fake-gcs-server ignores the `ifGenerationMatch` query parameter on object
// downloads, so it returns the object instead of the 412 returned by real GCS.
#[ignore = "fake-gcs-server does not enforce generation preconditions on downloads"]
async fn test_gcs_get_with_generation() {
    let (_server, client) = start_fake_gcs_server().await;
    do_test_get_with_version(client).await;
}

#[tokio::test]
async fn test_gcs_get_rejects_invalid_generation() {
    let (_server, client) = start_fake_gcs_server().await;
    let client = ObjectStore::new(
        client,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    let error = client
        .get(
            "test",
            GetOptions {
                version: Some(ObjectVersion("invalid generation".to_string())),
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error.current_context(),
        ObjectStoreError::Metadata
    ));
}

async fn do_put_with_overwrite(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    let put_res = client
        .put("test", "Hello, World".into(), PutOptions::default())
        .await
        .unwrap();

    let original_version = put_res.version;

    let put_res = client
        .put(
            "test",
            "Something else".into(),
            PutOptions {
                mode: PutMode::Overwrite,
            },
        )
        .await
        .unwrap();

    assert_ne!(put_res.version, original_version);
}

#[tokio::test]
async fn test_s3_put_with_overwrite() {
    let (_minio, client) = start_minio().await;
    do_put_with_overwrite(client).await;
}

#[tokio::test]
async fn test_azure_put_with_overwrite() {
    let (_azurite, client) = start_azurite().await;
    do_put_with_overwrite(client).await;
}

#[tokio::test]
async fn test_gcs_put_with_overwrite() {
    let (_server, client) = start_fake_gcs_server().await;
    do_put_with_overwrite(client).await;
}

async fn do_put_with_create(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    client
        .put(
            "test",
            "Hello, World".into(),
            PutOptions {
                mode: PutMode::Create,
            },
        )
        .await
        .unwrap();

    let response = client
        .put(
            "test",
            "Something else".into(),
            PutOptions {
                mode: PutMode::Create,
            },
        )
        .await;

    assert!(response.is_err());
    assert!(response.unwrap_err().is_precondition());
}

#[tokio::test]
async fn test_s3_put_with_create() {
    let (_minio, client) = start_minio().await;
    do_put_with_create(client).await;
}

#[tokio::test]
async fn test_azure_put_with_create() {
    let (_azurite, client) = start_azurite().await;
    do_put_with_create(client).await;
}

#[tokio::test]
async fn test_gcs_put_with_create() {
    let (_server, client) = start_fake_gcs_server().await;
    do_put_with_create(client).await;
}

async fn do_put_with_update(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    let response = client
        .put(
            "test",
            "Hello, World".into(),
            PutOptions {
                mode: PutMode::Create,
            },
        )
        .await
        .unwrap();

    let original_version = response.version;

    let response = client
        .put(
            "test",
            "Something else".into(),
            PutOptions {
                mode: PutMode::Update("123456789".to_string().into()),
            },
        )
        .await;
    assert!(response.is_err());
    assert!(response.unwrap_err().is_precondition());

    let response = client
        .put(
            "test",
            "Something else".into(),
            PutOptions {
                mode: PutMode::Update(original_version.clone()),
            },
        )
        .await
        .unwrap();

    assert_ne!(response.version, original_version);
}

#[tokio::test]
async fn test_s3_put_with_update() {
    let (_minio, inner) = start_minio().await;
    do_put_with_update(inner).await;
}

#[tokio::test]
async fn test_azure_put_with_update() {
    let (_azurite, inner) = start_azurite().await;
    do_put_with_update(inner).await;
}

#[tokio::test]
async fn test_gcs_put_with_update() {
    let (_server, inner) = start_fake_gcs_server().await;
    do_put_with_update(inner).await;
}

async fn do_delete(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    client
        .put("test", "Hello, World".into(), PutOptions::default())
        .await
        .unwrap();

    client
        .delete("test", DeleteOptions::default())
        .await
        .unwrap();

    let response = client.get("test", GetOptions::default()).await;
    assert!(response.is_err());
    assert!(response.unwrap_err().is_not_found());
}

#[tokio::test]
async fn test_s3_delete() {
    let (_minio, inner) = start_minio().await;
    do_delete(inner).await;
}

#[tokio::test]
async fn test_azure_delete() {
    let (_azurite, inner) = start_azurite().await;
    do_delete(inner).await;
}

#[tokio::test]
async fn test_gcs_delete() {
    let (_server, inner) = start_fake_gcs_server().await;
    do_delete(inner).await;
}
