use crate::{
    common::index::IndexData,
    storage::redis::redis_index_store::RedisIndexStore,
    storage::tikv::tikv_index_store::TikvIndexStore,
    traits::index_store::{IndexStore, UnsendIndexStore},
};

pub enum IndexStoreImpl {
    Redis(RedisIndexStore),
    Tikv(TikvIndexStore),
}

impl IndexStore for IndexStoreImpl {
    async fn write_offset(&self, topic: &str, partition: i32, offset: i64) -> anyhow::Result<()> {
        match self {
            IndexStoreImpl::Redis(r) => r.write_offset(topic, partition, offset).await,
            IndexStoreImpl::Tikv(t) => t.write_offset(topic, partition, offset).await,
        }
    }

    async fn read_offset(&self, topic: &str, partition: i32) -> anyhow::Result<i64> {
        match self {
            IndexStoreImpl::Redis(r) => r.read_offset(topic, partition).await,
            IndexStoreImpl::Tikv(t) => t.read_offset(topic, partition).await,
        }
    }

    async fn lock_exclusive(&self, topic: &str, partition: i32, timeout: i64) -> anyhow::Result<Option<String>> {
        match self {
            IndexStoreImpl::Redis(r) => r.lock_exclusive(topic, partition, timeout).await,
            IndexStoreImpl::Tikv(t) => t.lock_exclusive(topic, partition, timeout).await,
        }
    }

    async fn unlock_exclusive(&self, topic: &str, partition: i32, lock_id: &str) -> anyhow::Result<bool> {
        match self {
            IndexStoreImpl::Redis(r) => r.unlock_exclusive(topic, partition, lock_id).await,
            IndexStoreImpl::Tikv(t) => t.unlock_exclusive(topic, partition, lock_id).await,
        }
    }

    async fn set_index(&self, topic: &str, partition: i32, start_offset: i64, data: &IndexData) -> anyhow::Result<()> {
        match self {
            IndexStoreImpl::Redis(r) => r.set_index(topic, partition, start_offset, data).await,
            IndexStoreImpl::Tikv(t) => t.set_index(topic, partition, start_offset, data).await,
        }
    }

    async fn get_index_from_start_offset(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
    ) -> anyhow::Result<Vec<IndexData>> {
        match self {
            IndexStoreImpl::Redis(r) => r.get_index_from_start_offset(topic, partition, start_offset).await,
            IndexStoreImpl::Tikv(t) => t.get_index_from_start_offset(topic, partition, start_offset).await,
        }
    }
    
}