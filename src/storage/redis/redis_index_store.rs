use core::f64;

use crate::common::index::IndexData;
use crate::traits::index_store::UnsendIndexStore;

use fred::{
    clients::Pool,
    prelude::{KeysInterface, SortedSetsInterface},
    types::{Expiration, SetOptions},
};

pub struct RedisIndexStore {
    client: Pool,
}

impl RedisIndexStore {
    pub fn new(client: Pool) -> Self {
        Self { client }
    }
}

impl UnsendIndexStore for RedisIndexStore {
    async fn write_offset(&self, topic: &str, partition: i32, offset: i64) -> anyhow::Result<()> {
        let key = format!("offset:{}:{}", topic, partition);
        match self
            .client
            .set::<(), String, i64>(key, offset, None, None, false)
            .await
        {
            Ok(_val) => {}
            Err(err) => {
                log::error!("Failed to set offset in Redis: {}", err);
                return Err(anyhow::anyhow!("Failed to set offset in Redis"));
            }
        }
        Ok(())
    }

    async fn read_offset(&self, topic: &str, partition: i32) -> anyhow::Result<i64> {
        let key = format!("offset:{}:{}", topic, partition);
        let offset: Option<i64> = self.client.get(&key).await?;
        match offset {
            Some(value) => Ok(value),
            None => Ok(-1), // Return -1 if no offset is found
        }
    }

    async fn lock_exclusive(
        &self,
        topic: &str,
        partition: i32,
        timeout_secs: i64,
    ) -> anyhow::Result<Option<String>> {
        // let mut conn = self.conn.lock().await;
        let key = format!("lock:{}:{}", topic, partition);
        let lock_id = uuid::Uuid::new_v4().to_string(); // unique token for reentrant safety if needed
        // let result = self.client.lock_exclusive(&key, &value, timeout_secs).await?;
        let result: Option<String> = self
            .client
            .set(
                key,
                &lock_id,
                Some(Expiration::EX(timeout_secs as i64)),
                Some(SetOptions::NX),
                false,
            )
            .await?;
        if result.is_some() {
            Ok(Some(lock_id)) // if lock acquired, return the lock id
        } else {
            Ok(None)
        }
    }

    async fn unlock_exclusive(
        &self,
        topic: &str,
        partition: i32,
        lock_id: &str,
    ) -> anyhow::Result<bool> {
        // TODO: Use Lua script for atomic unlock
        let key = format!("lock:{}:{}", topic, partition);
        let deleted: i64 = self.client.del(&key).await?;
        if deleted == 0 {
            log::warn!(
                "Failed to unlock exclusive lock for topic: {}, partition: {}, lock_id: {}",
                topic,
                partition,
                lock_id
            );
        } else {
            log::debug!(
                "Successfully unlocked exclusive lock for topic: {}, partition: {}, lock_id: {}",
                topic,
                partition,
                lock_id
            );
        }
        Ok(deleted == 1)
    }

    async fn set_index(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        data: &IndexData,
    ) -> anyhow::Result<()> {
        let key = format!("index:{}:{}", topic, partition);
        log::debug!(
            "Setting index for topic: {}, partition: {}, start_offset: {}",
            topic,
            partition,
            start_offset
        );
        log::debug!("Key for index: {}", key);
        let data_string = serde_json::to_string(data).map_err(|e| {
            log::error!("Failed to serialize index data: {:?}", e);
            anyhow::anyhow!("Serialization error")
        })?;
        let _: usize = self
            .client
            .zadd(
                &key,
                Some(SetOptions::NX),
                None,
                true,
                false,
                (start_offset as f64, data_string),
            )
            .await?;
        Ok(())
    }

    async fn get_index_from_start_offset(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
    ) -> anyhow::Result<Vec<IndexData>> {
        let key = format!("index:{}:{}", topic, partition);
        let results: Vec<String> = self
            .client
            .zrangebyscore(&key, start_offset as f64, f64::INFINITY, false, None)
            .await?;
        let mut index_data = Vec::new();
        for result in results {
            let data: IndexData = serde_json::from_str(&result).map_err(|e| {
                log::error!("Failed to deserialize index data: {:?}", e);
                anyhow::anyhow!("Deserialization error")
            })?;
            index_data.push(data);
        }
        Ok(index_data)
    }
}
