use crate::common::cluster::Node;
use crate::common::consumer::ConsumerGroup;
use crate::common::topic_partition::Topic;
use crate::common::utils::jittered_delay;
use crate::traits::meta_store::UnsendMetaStore;
use anyhow::Result;
use fred::{
    bytes_utils::Str,
    clients::Pool,
    prelude::{KeysInterface, SetsInterface},
    types::Key,
    types::{Expiration, SetOptions},
};
use std::time::Duration;
use tokio::time::sleep;

pub struct RedisMetaStore {
    client: Pool,
    ttl_secs: i64,
}

impl RedisMetaStore {
    pub fn new(client: Pool) -> Self {
        Self {
            client,
            ttl_secs: 10,
        }
    }
}

impl UnsendMetaStore for RedisMetaStore {
    async fn put_topic(&self, data: &Topic) -> Result<()> {
        // set the topic information in Redis
        // using the topic name as the key and the serialized data as the value
        let key = format!("topic:{}", data.topic_id);
        let value = serde_json::to_string(data)?;
        match self
            .client
            .set::<(), String, String>(key, value, None, None, false)
            .await
        {
            Ok(_val) => {}
            Err(err) => {
                log::error!("Failed to set offset in Redis: {}", err);
                return Err(anyhow::anyhow!("Failed to set offset in Redis"));
            }
        }

        match &data.name {
            Some(name) => {
                // If name is provided, create an index key for quick lookup
                let index_key = format!("topic_index:name:{}", name);
                let _: () = self
                    .client
                    .sadd(&index_key, &data.topic_id.to_string())
                    .await?;
            }
            None => {
                // Nothing to do if name is not provided
            }
        }
        Ok(())
    }

    async fn get_topic(&self, topic_id: &str) -> Result<Topic> {
        // get the topic information from Redis
        let key = format!("topic:{}", topic_id);
        // Get the topic data from Redis
        let maybe_value: Option<String> = self.client.get(&key).await?;
        let value = match maybe_value {
            Some(v) => v,
            None => return Err(anyhow::anyhow!("Topic not found with ID: {}", topic_id)),
        };
        let topic: Topic = serde_json::from_str(&value)?;
        return Ok(topic);
    }

    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()> {
        // Delete the topic by ID
        let key = format!("topic:{}", topic_id);
        match self.client.del(&key).await? {
            0 => {
                // If no keys were deleted, it means the topic does not exist
                return Err(anyhow::anyhow!("Topic not found with ID: {}", topic_id));
            }
            _ => {
                // Successfully deleted the topic
                log::info!("Deleted topic with ID: {}", topic_id);
            }
        }
        Ok(())
    }

    async fn get_topics(&self) -> Result<Vec<Topic>> {
        let max_align_topics = 1000000; // Arbitrary large number to limit the scan

        let keys: Vec<Key> = self.scan_keys("topic:*", max_align_topics).await?;
        let mut topics = Vec::new();
        for key in keys {
            let maybe_value: Option<String> = self.client.get(key).await?;
            let value = match maybe_value {
                Some(v) => v,
                None => return Ok(Vec::new()),
            };
            if let Ok(topic) = serde_json::from_str::<Topic>(&value) {
                topics.push(topic);
            }
        }
        if topics.is_empty() {
            return Ok(Vec::new());
        }
        Ok(topics)
    }

    async fn get_topic_id_by_topic_name(&self, topic_name: &str) -> Result<Option<String>> {
        // Get the topic ID by topic name
        let index_key = format!("topic_index:name:{}", topic_name);
        let topic_ids: Vec<String> = self.client.smembers(&index_key).await?;
        if topic_ids.is_empty() {
            return Ok(None); // No topic found with the given name
        }
        Ok(Some(topic_ids[0].clone())) // Return the first matching topic ID
    }

    async fn save_consumer_group(&self, data: &ConsumerGroup) -> Result<()> {
        // Save the consumer group information in Redis
        let key = format!("consumer_group:{}", data.group_id);
        let value = serde_json::to_string(data)?;
        match self
            .client
            .set::<(), String, String>(key, value, None, None, false)
            .await
        {
            Ok(_val) => {}
            Err(err) => {
                log::error!("Failed to set consumer group in Redis: {}", err);
                return Err(anyhow::anyhow!("Failed to set set consumer group in Redis"));
            }
        }
        Ok(())
    }

    async fn get_consumer_groups(&self) -> Result<Vec<ConsumerGroup>> {
        // Get all consumer groups from Redis
        let max_align_groups = 1000000; // Arbitrary large number to limit the scan
        let keys: Vec<Key> = self.scan_keys("consumer_group:*", max_align_groups).await?;
        let mut consumer_groups = Vec::new();
        for key in keys {
            let maybe_value: Option<String> = self.client.get(key).await?;
            let value = match maybe_value {
                Some(v) => v,
                None => continue, // Skip if no value found
            };
            if let Ok(consumer_group) = serde_json::from_str::<ConsumerGroup>(&value) {
                consumer_groups.push(consumer_group);
            }
        }
        Ok(consumer_groups)
    }

    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        // Get the consumer group by group ID
        let key = format!("consumer_group:{}", group_id);
        let lock_key = format!("lock:consumer_group:{}", group_id);

        let result = self
            .with_redis_lock(
                self.client.clone(),
                &lock_key,
                self.ttl_secs,
                move |client| async move {
                    let maybe_value: Option<String> = client.get(&key).await?;
                    let value = match maybe_value {
                        Some(v) => v,
                        None => {
                            log::warn!("Consumer group not found for key: {}", key);
                            return Ok(None);
                        }
                    };
                    let consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
                    Ok(Some(consumer_group))
                },
            )
            .await?;

        Ok(result)
    }

    async fn gen_producer_id(&self) -> Result<i64> {
        let key = "producer_id_counter01";
        let lock_key = format!("lock:{}", key);

        let result = self
            .with_redis_lock(
                self.client.clone(),
                &lock_key,
                self.ttl_secs,
                move |client| async move {
                    let id = client
                        .incr(key)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to increment producer ID: {}", e))?;
                    Ok(id)
                },
            )
            .await?;
        Ok(result)
    }

    async fn update_consumer_group<F, Fut>(
        &self,
        group_id: &str,
        update_fn: F,
    ) -> Result<Option<ConsumerGroup>>
    where
        F: FnOnce(ConsumerGroup) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<ConsumerGroup>> + Send + 'static,
    {
        let key = format!("consumer_group:{}", group_id);
        let lock_key = format!("lock:consumer_group:{}", group_id);

        self.with_redis_lock(
            self.client.clone(),
            &lock_key,
            self.ttl_secs,
            move |client| async move {
                let maybe_value: Option<String> = client.get(&key).await?;
                let value = match maybe_value {
                    Some(v) => v,
                    None => return Ok(None),
                };

                let cg: ConsumerGroup = serde_json::from_str(&value)
                    .map_err(|e| anyhow::anyhow!("Deserialize ConsumerGroup: {}", e))?;

                let cg = update_fn(cg).await?;

                let updated = serde_json::to_string(&cg)?;
                client
                    .set::<(), String, String>(key, updated, None, None, false)
                    .await
                    .map_err(|e| anyhow::anyhow!("Redis SET error: {}", e))?;

                Ok(Some(cg))
            },
        )
        .await
    }

    async fn update_cluster_status(&self, node_config: &Node) -> Result<()> {
        let key = format!("node_config:{}", node_config.node_id);
        let key_self = key.clone();
        let mut node_config = node_config.clone();
        let now_ms: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| anyhow::anyhow!("clock error: {e:?}"))?
            .as_millis() as i64;
        node_config.heartbeat_time = Some(now_ms);
        let value = serde_json::to_string(&node_config)?;

        // Set with a TTL of 60 seconds to allow for some leeway
        match self
            .client
            .set::<(), String, String>(key, value, Some(Expiration::PX(60_000)), None, false)
            .await
        {
            Ok(_val) => {}
            Err(err) => {
                log::error!("Failed to set node config in Redis: {}", err);
                return Err(anyhow::anyhow!("Failed to set node config in Redis"));
            }
        }
        log::debug!("Updated cluster status for node: {}", key_self);
        Ok(())
    }

    async fn get_cluster_status(&self) -> Result<Vec<Node>> {
        let prefix = "node_config:";
        let max_nodes = 1000;
        let keys: Vec<Key> = self.scan_keys(&format!("{}*", prefix), max_nodes).await?;
        let mut nodes = Vec::new();
        for key in keys {
            let key_clone = key.clone();
            let maybe_value: Option<String> = self.client.get(key).await?;
            let value = match maybe_value {
                Some(v) => v,
                None => {
                    log::warn!("Node config not found for key: {:?}", key_clone);
                    continue;
                }
            };
            if let Ok(node) = serde_json::from_str::<Node>(&value) {
                nodes.push(node);
            }
        }
        Ok(nodes)
    }
}

impl RedisMetaStore {
    pub async fn scan_keys(&self, pattern: &str, max_keys: usize) -> Result<Vec<Key>> {
        let mut cursor: Str = "0".to_string().into();
        // break out after max_counts records
        let mut count = 0;
        let mut all_keys = Vec::new();
        loop {
            let (new_cursor, keys): (Str, Vec<Key>) = self
                .client
                .scan_page(cursor.clone(), pattern, Some(100), None)
                .await?;
            count += keys.len();
            for key in keys.into_iter() {
                all_keys.push(key);
            }

            if count >= max_keys || new_cursor == "0" {
                break;
            } else {
                cursor = new_cursor;
            }
        }
        log::debug!(
            "Scanned {} keys matching pattern '{}'",
            all_keys.len(),
            pattern
        );
        Ok(all_keys)
    }

    pub async fn with_redis_lock<F, Fut, T>(
        &self,
        client: Pool,
        lock_key: &str,
        ttl_secs: i64,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(Pool) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        const MAX_RETRIES: usize = 200;
        const RETRY_DELAY_MS: u64 = 100;
        let mut acquired = false;
        for attempt in 0..MAX_RETRIES {
            let result: Option<String> = self
                .client
                .set(
                    lock_key,
                    "lock",
                    Some(Expiration::EX(ttl_secs)),
                    Some(SetOptions::NX),
                    false,
                )
                .await?;
            if result.is_some() {
                acquired = true;
                break;
            } else {
                log::debug!(
                    "🔒 Lock busy (attempt {}/{}): {}. Retrying...",
                    attempt + 1,
                    MAX_RETRIES,
                    lock_key
                );
            }
            sleep(Duration::from_millis(jittered_delay(RETRY_DELAY_MS))).await;
        }

        if !acquired {
            return Err(anyhow::anyhow!("Failed to acquire lock: {}", lock_key));
        }

        let client_for_closure = client.clone();
        let result = f(client_for_closure).await;

        //let _ = client.del(lock_key).await?;
        let deleted: i64 = client.del(lock_key).await?;
        if deleted == 0 {
            log::warn!("Failed to unlock lock for key: {}", lock_key);
            return Err(anyhow::anyhow!("Failed to unlock lock: {}", lock_key));
        } else {
            log::debug!("Successfully unlocked lock for key: {}", lock_key);
        }

        result
    }
}
