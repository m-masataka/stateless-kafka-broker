use crate::traits::index_store::UnsendIndexStore;
use crate::storage::redis::redis_client::RedisClient;
use crate::common::index::IndexData;

pub struct RedisIndexStore {
    client: RedisClient,
}

impl RedisIndexStore {
    pub fn new(client: RedisClient) -> Self {
        Self {
            client,
        }
    }
}

impl UnsendIndexStore for RedisIndexStore {
    async fn write_offset(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> anyhow::Result<()> {
        let key = format!("offset:{}:{}", topic, partition);
        self.client.set(&key, offset).await?;
        Ok(())
    }

    async fn read_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> anyhow::Result<i64> {
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
    ) -> anyhow::Result<bool> {
        // let mut conn = self.conn.lock().await;
        let key = format!("lock:{}:{}", topic, partition);
        let value = uuid::Uuid::new_v4().to_string(); // unique token for reentrant safety if needed
        let result = self.client.lock_exclusive(&key, &value, timeout_secs).await?;
        Ok(result)
    }

    async fn unlock_exclusive(
        &self,
        topic: &str,
        partition: i32,
    ) -> anyhow::Result<()> {
        let key = format!("lock:{}:{}", topic, partition);
        let _: () = self.client.unlock_exclusive(&key).await?;
        Ok(())
    }


    async fn set_index(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        data: &IndexData,
    ) -> anyhow::Result<()> {
        let key = format!("index:{}:{}", topic, partition);
        log::debug!("Setting index for topic: {}, partition: {}, start_offset: {}", topic, partition, start_offset);
        log::debug!("Key for index: {}", key);
        let data_string = serde_json::to_string(data).map_err(|e| {
            log::error!("Failed to serialize index data: {:?}", e);
            anyhow::anyhow!("Serialization error")
        })?;
        let _: () = self.client.zadd(&key, &data_string, start_offset).await?;
        Ok(())
    }

    async fn get_index_from_start_offset(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
    ) -> anyhow::Result<Vec<IndexData>> {
        let key = format!("index:{}:{}", topic, partition);
        let results: Vec<String> = self.client
            .zrangebyscore(&key, start_offset, i64::MAX)
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
