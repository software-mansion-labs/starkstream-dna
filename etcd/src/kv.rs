use error_stack::{Result, ResultExt};

pub use etcd_client::{DeleteResponse, GetResponse, PutResponse};
use etcd_client::{GetOptions, TxnResponse};

use crate::client::EtcdClientError;

#[derive(Clone)]
pub struct KvClient {
    pub(crate) client: etcd_client::KvClient,
    pub(crate) prefix: String,
}

impl KvClient {
    #[tracing::instrument(level = "debug", skip_all, fields(key = prefix.as_ref()))]
    pub async fn get_prefix(
        &mut self,
        prefix: impl AsRef<str>,
    ) -> Result<GetResponse, EtcdClientError> {
        let prefix = prefix.as_ref();
        let options = GetOptions::new().with_prefix();
        self.client
            .get(self.format_key(prefix), options.into())
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to get key with prefix from etcd")
            .attach_printable_lazy(|| format!("prefix: {}", prefix))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(key = key.as_ref()))]
    pub async fn get(&mut self, key: impl AsRef<str>) -> Result<GetResponse, EtcdClientError> {
        let key = key.as_ref();
        self.client
            .get(self.format_key(key), None)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to get key from etcd")
            .attach_printable_lazy(|| format!("key: {}", key))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(key = key.as_ref()))]
    pub async fn put(
        &mut self,
        key: impl AsRef<str>,
        value: impl AsRef<[u8]>,
    ) -> Result<PutResponse, EtcdClientError> {
        let key = key.as_ref();
        let value = value.as_ref();
        self.client
            .put(self.format_key(key), value, None)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to put key to etcd")
            .attach_printable_lazy(|| format!("key: {}", key))
    }

    /// Put the value only if the key's mod revision still equals `mod_revision`
    /// (`0` means the key must not exist). Returns whether the put was applied.
    #[tracing::instrument(level = "debug", skip_all, fields(key = key.as_ref()))]
    pub async fn put_if_mod_revision(
        &mut self,
        key: impl AsRef<str>,
        value: impl AsRef<[u8]>,
        mod_revision: i64,
    ) -> Result<bool, EtcdClientError> {
        let formatted_key = self.format_key(key.as_ref());

        // etcd checks the condition and does the put atomically, so no other write can land
        // between them.
        let txn = etcd_client::Txn::new()
            .when(&[etcd_client::Compare::mod_revision(
                formatted_key.clone(),
                etcd_client::CompareOp::Equal,
                mod_revision,
            )])
            .and_then(&[etcd_client::TxnOp::put(formatted_key, value.as_ref(), None)]);

        let response = self
            .client
            .txn(txn)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to conditionally put key to etcd")
            .attach_printable_lazy(|| format!("key: {}", key.as_ref()))?;

        // `false` means the key changed since `mod_revision` and nothing was written.
        Ok(response.succeeded())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(key = key.as_ref()))]
    pub async fn delete(
        &mut self,
        key: impl AsRef<str>,
    ) -> Result<DeleteResponse, EtcdClientError> {
        let key = key.as_ref();
        self.client
            .delete(self.format_key(key), None)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to delete key from etcd")
            .attach_printable_lazy(|| format!("key: {}", key))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(put_key = put_key.as_ref(), del_key = del_key.as_ref()))]
    pub async fn put_and_delete(
        &mut self,
        put_key: impl AsRef<str>,
        put_value: impl AsRef<[u8]>,
        del_key: impl AsRef<str>,
    ) -> Result<TxnResponse, EtcdClientError> {
        let put_key = put_key.as_ref();
        let put_value = put_value.as_ref();
        let del_key = del_key.as_ref();

        let txn = etcd_client::Txn::new().and_then(&[
            etcd_client::TxnOp::put(self.format_key(put_key), put_value, None),
            etcd_client::TxnOp::delete(self.format_key(del_key), None),
        ]);

        self.client
            .txn(txn)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to put and delete key to etcd")
            .attach_printable_lazy(|| format!("put_key: {}", put_key))
            .attach_printable_lazy(|| format!("del_key: {}", del_key))
    }

    fn format_key(&self, key: &str) -> Vec<u8> {
        format!("{}{}", self.prefix, key).into_bytes()
    }
}
