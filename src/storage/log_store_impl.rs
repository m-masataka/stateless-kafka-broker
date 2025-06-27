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
    async fn write_records(&self, topic: &str, partition: i32, start_offset: i64, records: Option<&Bytes>) -> anyhow::Result<(i64, String)> {
        match self {
            LogStoreImpl::File(f) => f.write_records(topic, partition, start_offset, records).await,
            LogStoreImpl::S3(s) => s.write_records(topic, partition, start_offset, records).await,
        }
    }

    async fn read_records(&self, keys: Vec<String>) -> anyhow::Result<Bytes> {
        match self {
            LogStoreImpl::File(f) => f.read_records(keys).await,
            LogStoreImpl::S3(s) => s.read_records(keys).await,
        }
    }
}
