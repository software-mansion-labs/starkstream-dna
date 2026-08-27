use bytes::{Buf, BufMut, Bytes, BytesMut};
use error_stack::{Result, ResultExt};
use starkstream_dna_etcd::normalize_prefix;
use tracing::{debug, info};

mod aws_s3;
mod azure_blob;
mod client;
mod error;
mod gcs;
mod metrics;
pub mod testing;

pub use self::aws_s3::AwsS3Client;
pub use self::azure_blob::AzureBlobClient;
pub use self::client::ObjectStoreClient;
pub use self::error::{ObjectStoreError, ObjectStoreResultExt, ToObjectStoreResult};
pub use self::gcs::GcsClient;

/// Options for the object store.
#[derive(Default, Clone, Debug)]
pub struct ObjectStoreOptions {
    /// The S3 bucket to use.
    pub bucket: String,
    /// Under which prefix to store the data.
    pub prefix: Option<String>,
}

/// This is an opinionated object store client.
#[derive(Clone)]
pub struct ObjectStore {
    client: ObjectStoreClient,
    prefix: String,
    bucket: String,
}

/// An opaque per-object version token used for optimistic concurrency.
///
/// Each backend stores whatever it uses for conditional requests: an HTTP ETag
/// for S3 and Azure, the object generation number for GCS. Treat it as opaque —
/// only the originating backend interprets its contents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectVersion(pub String);

#[derive(Default, Clone, Debug)]
pub struct GetOptions {
    /// If the object exists, only return it if its version matches.
    pub version: Option<ObjectVersion>,
}

/// How to put an object.
#[derive(Default, Clone, Debug)]
pub enum PutMode {
    /// Overwrite the object if it exists.
    #[default]
    Overwrite,
    /// Create the object only if it doesn't exist.
    Create,
    /// Update the object only if it exists and its version matches.
    Update(ObjectVersion),
}

#[derive(Default, Clone, Debug)]
pub struct PutOptions {
    pub mode: PutMode,
}

#[derive(Default, Clone, Debug)]
pub struct DeleteOptions {
    /// The provided path is already inclusive of the prefix.
    pub fullpath: Option<bool>,
}

#[derive(Default, Clone, Debug)]
pub struct ListOptions {}

#[derive(Debug)]
pub struct GetResult {
    pub body: Bytes,
    pub version: ObjectVersion,
}

#[derive(Debug)]
pub struct PutResult {
    pub version: ObjectVersion,
}

#[derive(Debug)]
pub struct DeleteResult;

#[derive(Debug)]
pub struct ListResult {
    pub object_ids: Vec<String>,
}

impl ObjectStore {
    pub fn new(client: ObjectStoreClient, options: ObjectStoreOptions) -> Self {
        let prefix = normalize_prefix(options.prefix);

        Self {
            client,
            bucket: options.bucket,
            prefix,
        }
    }

    pub fn new_s3(s3_client: AwsS3Client, options: ObjectStoreOptions) -> Self {
        Self::new(s3_client.into(), options)
    }

    pub fn new_azure_blob(client: AzureBlobClient, options: ObjectStoreOptions) -> Self {
        Self::new(client.into(), options)
    }

    pub fn new_gcs(client: GcsClient, options: ObjectStoreOptions) -> Self {
        Self::new(client.into(), options)
    }

    /// Ensure the currently configured bucket exists.
    pub async fn ensure_bucket(&self) -> Result<(), ObjectStoreError> {
        if self.client.has_bucket(&self.bucket).await? {
            debug!("bucket already exists");
            return Ok(());
        }

        info!(name = self.bucket, "S3 bucket does not exist. Creating.");

        self.client
            .create_bucket(&self.bucket)
            .await
            .attach_printable("failed to create bucket")
            .attach_printable_lazy(|| format!("bucket name: {}", self.bucket))
    }

    #[tracing::instrument(name = "object_store_get", skip(self, options), level = "debug")]
    pub async fn get(
        &self,
        path: &str,
        options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        let key = self.full_key(path);
        let (version, body) = self
            .client
            .get_object(&self.bucket, &key, options)
            .await
            .attach_printable("failed to get object")
            .attach_printable_lazy(|| format!("key: {key}"))?;

        let decompressed = BytesMut::with_capacity(body.remaining());
        let mut writer = decompressed.writer();
        zstd::stream::copy_decode(&mut body.reader(), &mut writer)
            .change_context(ObjectStoreError::Request)?;
        let decompressed = writer.into_inner();

        let checksum = (&decompressed[decompressed.len() - 4..]).get_u32();
        let data = decompressed[..decompressed.len() - 4].as_ref();

        if crc32fast::hash(data) != checksum {
            return Err(ObjectStoreError::ChecksumMismatch)
                .attach_printable("checksum mismatch")
                .attach_printable_lazy(|| format!("key: {key}"));
        }

        let body = Bytes::copy_from_slice(data);

        Ok(GetResult { body, version })
    }

    #[tracing::instrument(
        name = "object_store_put",
        skip_all,
        fields(key, compression_ratio),
        level = "debug"
    )]
    pub async fn put(
        &self,
        path: &str,
        body: Bytes,
        options: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        let current_span = tracing::Span::current();

        let key = self.full_key(path);
        let size_before = body.len();

        let checksum = crc32fast::hash(&body);
        let mut body = BytesMut::from(body);
        body.put_u32(checksum);

        let mut compressed = BytesMut::with_capacity(body.len()).writer();
        zstd::stream::copy_encode(body.reader(), &mut compressed, 0)
            .change_context(ObjectStoreError::Request)?;
        let compressed = compressed.into_inner();

        let size_after = compressed.len();
        let compression_ratio = size_before as f64 / size_after as f64;

        current_span.record("key", &key);
        current_span.record("compression_ratio", compression_ratio);
        debug!(compression_ratio, key, "compressed object");

        let version = self
            .client
            .put_object(&self.bucket, &key, compressed.freeze(), options)
            .await
            .attach_printable("failed to put object")
            .attach_printable_lazy(|| format!("key: {key}"))?;

        Ok(PutResult { version })
    }

    #[tracing::instrument(name = "object_store_delete", skip(self, options), level = "debug")]
    pub async fn delete(
        &self,
        path: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ObjectStoreError> {
        let key = if let Some(true) = options.fullpath {
            path.to_string()
        } else {
            self.full_key(path)
        };
        self.client
            .delete_object(&self.bucket, &key, options)
            .await
            .attach_printable("failed to delete object")
            .attach_printable_lazy(|| format!("key: {key}"))?;

        Ok(DeleteResult)
    }

    #[tracing::instrument(name = "object_store_list", skip(self), level = "debug")]
    pub async fn list(
        &self,
        prefix: &str,
        _options: ListOptions,
    ) -> Result<ListResult, ObjectStoreError> {
        let key = self.full_key(prefix);
        let object_ids = self
            .client
            .list_objects(&self.bucket, &key)
            .await
            .attach_printable("failed to list objects")
            .attach_printable_lazy(|| format!("prefix: {key}"))?;

        Ok(ListResult { object_ids })
    }

    fn full_key(&self, path: &str) -> String {
        format!("{}{}", self.prefix, path)
    }
}

impl From<String> for ObjectVersion {
    fn from(value: String) -> Self {
        Self(value)
    }
}
