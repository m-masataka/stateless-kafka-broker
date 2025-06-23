use crate::traits::log_store::LogStore;
use crate::storage::file::file_log_store::FileLogStore;
use crate::storage::s3::s3_log_store::S3LogStore;
use crate::traits::log_store::UnsendLogStore;
use bytes::Bytes;

pub enum LogStoreImpl {
    File(FileLogStore),
    S3(S3LogStore),
}

impl LogStore for LogStoreImpl {
    async fn write_batch(&self, topic: &str, partition: i32, records: Option<&Bytes>) -> anyhow::Result<i64> {
        match self {
            LogStoreImpl::File(f) => f.write_batch(topic, partition, records).await,
            LogStoreImpl::S3(s) => s.write_batch(topic, partition, records).await,
        }
    }

    async fn read_records(&self, topic: &str, partition: i32, offset: i64, max_offset: i64) -> anyhow::Result<Bytes> {
        match self {
            LogStoreImpl::File(f) => f.read_records(topic, partition, offset, max_offset).await,
            LogStoreImpl::S3(s) => s.read_records(topic, partition, offset, max_offset).await,
        }
    }

    async fn read_offset(&self, topic: &str, partition: i32) -> anyhow::Result<i64> {
        match self {
            LogStoreImpl::File(f) => f.read_offset(topic, partition).await,
            LogStoreImpl::S3(s) => s.read_offset(topic, partition).await,
        }
    }
}
