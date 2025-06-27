use crate::{storage::redis::redis_index_store::RedisIndexStore, traits::index_store::{IndexStore, UnsendIndexStore}};

pub enum IndexStoreImpl {
    Redis(RedisIndexStore),
}

impl IndexStore for IndexStoreImpl {
    async fn write_offset(&self, topic: &str, partition: i32, offset: i64) -> anyhow::Result<()> {
        match self {
            IndexStoreImpl::Redis(r) => r.write_offset(topic, partition, offset).await,
        }
    }

    async fn read_offset(&self, topic: &str, partition: i32) -> anyhow::Result<i64> {
        match self {
            IndexStoreImpl::Redis(r) => r.read_offset(topic, partition).await,
        }
    }

    async fn lock_exclusive(&self, topic: &str, partition: i32, timeout: i64) -> anyhow::Result<bool> {
        match self {
            IndexStoreImpl::Redis(r) => r.lock_exclusive(topic, partition, timeout).await,
        }
    }

    async fn unlock_exclusive(&self, topic: &str, partition: i32) -> anyhow::Result<()> {
        match self {
            IndexStoreImpl::Redis(r) => r.unlock_exclusive(topic, partition).await,
        }
    }

    async fn set_index(&self, topic: &str, partition: i32, start_offset: i64, data_path: &str) -> anyhow::Result<()> {
        match self {
            IndexStoreImpl::Redis(r) => r.set_index(topic, partition, start_offset, data_path).await,
        }
    }

    async fn get_index_from_start_offset(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
    ) -> anyhow::Result<Vec<String>> {
        match self {
            IndexStoreImpl::Redis(r) => r.get_index_from_start_offset(topic, partition, start_offset).await,
        }
    }
    
}