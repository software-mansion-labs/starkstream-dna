use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::config::Credentials;
use azure_storage::{CloudLocation, StorageCredentials};
use bytes::Bytes;
use futures::Future;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
use google_cloud_gax::{error::Error as GaxError, options::RequestOptions, response::Response};
use google_cloud_storage::{
    client::{Storage, StorageControl},
    model::{Bucket, CreateBucketRequest, DeleteObjectRequest, GetBucketRequest},
    stub::StorageControl as StorageControlStub,
};
use testcontainers::{
    core::{ContainerPort, WaitFor},
    ContainerAsync, Image,
};

use super::{GcsClient, ObjectStoreError};

pub struct MinIO;

pub trait MinIOExt {
    fn s3_config(&self) -> impl Future<Output = aws_sdk_s3::Config> + Send;
}

impl Image for MinIO {
    fn name(&self) -> &str {
        "quay.io/minio/minio"
    }

    fn tag(&self) -> &str {
        "RELEASE.2025-09-07T16-13-09Z"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        Vec::default()
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        vec!["server", "/data"]
    }
}

pub struct Azurite;

pub struct FakeGcsServer;

pub trait FakeGcsServerExt {
    fn endpoint(&self) -> impl Future<Output = String>;
    fn gcs_client(&self) -> impl Future<Output = Result<GcsClient, ObjectStoreError>>;
}

impl Image for FakeGcsServer {
    fn name(&self) -> &str {
        "fsouza/fake-gcs-server"
    }

    fn tag(&self) -> &str {
        "latest"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        Vec::default()
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        vec!["-scheme", "http", "-port", "4443"]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[ContainerPort::Tcp(4443)]
    }
}

pub trait AzuriteExt {
    fn credentials(&self) -> StorageCredentials;
    fn location(&self) -> impl Future<Output = CloudLocation>;
}

impl Image for Azurite {
    fn name(&self) -> &str {
        "mcr.microsoft.com/azure-storage/azurite"
    }

    fn tag(&self) -> &str {
        "latest"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        Vec::default()
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        vec![
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            "--blobPort",
            "10000",
        ]
    }
}

pub fn minio_container() -> MinIO {
    MinIO
}

pub fn azurite_container() -> Azurite {
    Azurite
}

pub fn fake_gcs_server_container() -> FakeGcsServer {
    FakeGcsServer
}

impl MinIOExt for ContainerAsync<MinIO> {
    async fn s3_config(&self) -> aws_sdk_s3::Config {
        let port = self
            .get_host_port_ipv4(9000)
            .await
            .expect("MinIO port 9000");
        s3_config_at_port(port).await
    }
}

pub async fn s3_config_at_port(port: u16) -> aws_sdk_s3::Config {
    let endpoint = format!("http://localhost:{}", port);
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .endpoint_url(endpoint)
        .credentials_provider(credentials)
        .load()
        .await;

    let config: aws_sdk_s3::Config = (&config).into();
    config.to_builder().force_path_style(true).build()
}

impl AzuriteExt for ContainerAsync<Azurite> {
    fn credentials(&self) -> StorageCredentials {
        StorageCredentials::emulator()
    }

    async fn location(&self) -> CloudLocation {
        let port = self
            .get_host_port_ipv4(10_000)
            .await
            .expect("Azurite blob storage port 10000");
        CloudLocation::Emulator {
            address: "localhost".to_string(),
            port,
        }
    }
}

impl FakeGcsServerExt for ContainerAsync<FakeGcsServer> {
    async fn endpoint(&self) -> String {
        let port = self
            .get_host_port_ipv4(4443)
            .await
            .expect("fake-gcs-server port 4443");
        format!("http://localhost:{port}")
    }

    async fn gcs_client(&self) -> Result<GcsClient, ObjectStoreError> {
        let endpoint = self.endpoint().await;
        let credentials = AnonymousCredentials::new().build();
        let storage = Storage::builder()
            .with_endpoint(endpoint.clone())
            .with_credentials(credentials)
            .build()
            .await
            .map_err(|_| ObjectStoreError::Configuration)?;
        let control = StorageControl::from_stub(FakeGcsControl { endpoint });
        Ok(GcsClient::from_clients(
            storage,
            control,
            Some("test-project".to_string()),
        ))
    }
}

#[derive(Debug)]
struct FakeGcsControl {
    endpoint: String,
}

impl FakeGcsControl {
    async fn send(
        &self,
        method: reqwest::Method,
        path: &[&str],
        body: Option<String>,
    ) -> google_cloud_gax::Result<()> {
        let mut url = reqwest::Url::parse(&self.endpoint)
            .map_err(|err| GaxError::http(500, Default::default(), Bytes::from(err.to_string())))?;
        url.path_segments_mut()
            .map_err(|_| GaxError::http(500, Default::default(), Bytes::new()))?
            .clear()
            .extend(path);

        let mut request = reqwest::Client::new().request(method, url);
        if let Some(body) = body {
            request = request
                .header("content-type", "application/json")
                .body(body);
        }
        let response = request
            .send()
            .await
            .map_err(|err| GaxError::http(500, Default::default(), Bytes::from(err.to_string())))?;
        let status = response.status().as_u16();
        if response.status().is_success() {
            return Ok(());
        }
        let body = response.bytes().await.unwrap_or_default();
        Err(GaxError::http(status, Default::default(), body))
    }
}

impl StorageControlStub for FakeGcsControl {
    async fn get_bucket(
        &self,
        req: GetBucketRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<Bucket>> {
        let bucket = req.name.rsplit('/').next().unwrap_or(&req.name).to_string();
        self.send(reqwest::Method::GET, &["storage", "v1", "b", &bucket], None)
            .await?;
        Ok(Response::from(
            Bucket::new().set_name(req.name).set_bucket_id(bucket),
        ))
    }

    async fn create_bucket(
        &self,
        req: CreateBucketRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<Bucket>> {
        self.send(
            reqwest::Method::POST,
            &["storage", "v1", "b"],
            Some(format!(r#"{{"name":"{}"}}"#, req.bucket_id)),
        )
        .await?;
        Ok(Response::from(Bucket::new().set_bucket_id(req.bucket_id)))
    }

    async fn delete_object(
        &self,
        req: DeleteObjectRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<()>> {
        let bucket = req.bucket.rsplit('/').next().unwrap_or(&req.bucket);
        self.send(
            reqwest::Method::DELETE,
            &["storage", "v1", "b", bucket, "o", &req.object],
            None,
        )
        .await?;
        Ok(Response::from(()))
    }
}
