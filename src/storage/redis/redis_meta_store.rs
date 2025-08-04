use crate::traits::meta_store::UnsendMetaStore;
use crate::common::topic_partition::Topic;
use crate::common::consumer::{ConsumerGroup, ConsumerGroupMember};
use anyhow::Result;
use redis::AsyncCommands;
use redis::cluster_async::ClusterConnection;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct RedisMetaStore {
    conn: Arc<Mutex<ClusterConnection>>,
    ttl_secs: i64,
}

impl RedisMetaStore {
    pub fn new(conn: Arc<Mutex<ClusterConnection>>) -> Self {
        Self {
            conn,
            ttl_secs: 10,
        }
    }
}

impl UnsendMetaStore for RedisMetaStore {
    async fn save_topic_partition(&self, data: &Topic) -> Result<()> {
        // set the topic information in Redis
        // using the topic name as the key and the serialized data as the value
        let mut conn = self.conn.lock().await;
        let key = format!("topic:{}", data.topic_id);
        let value = serde_json::to_string(data)?;
        let _: () = conn.set(&key, &value).await?;

        match &data.name {
            Some(name) => {
                // If name is provided, create an index key for quick lookup
                let index_key = format!("topic_index:name:{}", name);
                let _: () = conn.sadd(&index_key, &data.topic_id.to_string()).await?;
            }
            None => {
                // Nothing to do if name is not provided
            }
        }
        Ok(())
    }

    async fn get_topic(&self, name: Option<&str>, topic_id: Option<&str>) -> Result<Option<Topic>> {
        let mut conn = self.conn.lock().await;
        // get the topic information from Redis
        if let Some(name) = name {
            match self.get_topic_id_by_name(&mut conn, name).await? {
                Some(topic_id) => {
                    let key = format!("topic:{}", topic_id);
                    let value: String = conn.get(&key).await?;
                    let topic: Topic = serde_json::from_str(&value)?;
                    return Ok(Some(topic));
                },
                None => {
                    return Ok(None);
                }
            }
       } else if let Some(topic_id) = topic_id {
            // If topic_id is provided, search topic by topic_id
            let key = format!("topic:{}", topic_id);
            let value: String = conn.get(&key).await?;
            let topic: Topic = serde_json::from_str(&value)?;
            return Ok(Some(topic));
        }
        Ok(None)
    }

    async fn delete_topic_by_name(&self, name: &str) -> Result<()> {
        let mut conn = self.conn.lock().await;
        match self.get_topic_id_by_name(&mut conn, name).await? {
            Some(topic_id) => {
                // If topic_id is found, delete the topic by ID
                let key = format!("topic:{}", topic_id);
                match conn.del(&key).await? {
                    0 => {
                        // If no keys were deleted, it means the topic does not exist
                        return Err(anyhow::anyhow!("Topic not found with name: {}", name));
                    },
                    _ => {
                        // Successfully deleted the topic
                        log::info!("Deleted topic with name: {}", name);
                    }
                }
                Ok(())
            }
            None => {
                // If no topic found with the given name, return an error
                Err(anyhow::anyhow!("Topic not found with name: {}", name))
            }
        }
    }

    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()> {
        // Delete the topic by ID
        let mut conn = self.conn.lock().await;
        let key = format!("topic:{}", topic_id);
        match conn.del(&key).await? {
            0 => {
                // If no keys were deleted, it means the topic does not exist
                return Err(anyhow::anyhow!("Topic not found with ID: {}", topic_id));
            },
            _ => {
                // Successfully deleted the topic
                log::info!("Deleted topic with ID: {}", topic_id);
            }
        }
        Ok(())
    }

    async fn get_all_topics(&self) -> Result<Vec<Topic>> {
        let mut conn = self.conn.lock().await;
        let keys: Vec<String> = conn.keys("topic:*").await?;
        let mut topics = Vec::new();
        for key in keys {
            let value: String = conn.get(&key).await?;
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
        let mut conn = self.conn.lock().await;
        let index_key = format!("topic_index:name:{}", topic_name);
        let topic_ids: Vec<String> = conn.smembers(&index_key).await?;
        if topic_ids.is_empty() {
            return Ok(None); // No topic found with the given name
        }
        Ok(Some(topic_ids[0].clone())) // Return the first matching topic ID
    }

    async fn save_consumer_group(&self, data: &ConsumerGroup) -> Result<()> {
        // Save the consumer group information in Redis
        let mut conn = self.conn.lock().await;
        let key = format!("consumer_group:{}", data.group_id);
        let value = serde_json::to_string(data)?;
        let _: () = conn.set(&key, &value).await?;
        Ok(())
    }

    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        // Get the consumer group by group ID
        let conn = self.conn.clone();
        let key = format!("consumer_group:{}", group_id);
        let lock_key = format!("lock:consumer_group:{}", group_id);
    
        let result = self.with_redis_lock(conn, &lock_key, self.ttl_secs, move |conn| async move {
            let mut conn = conn.lock().await;
            let maybe_value: Option<String> = conn.get(&key).await?;
            let value = match maybe_value {
                Some(v) => v,
                None => {
                    log::warn!("Consumer group not found for key: {}", key);
                    return Ok(None);
                }
            };
            let consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            Ok(Some(consumer_group))
        }).await?;
    
        Ok(result)
    }

    async fn update_heartbeat(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        // update the heartbeart for the consumer group
        let conn = self.conn.clone();
        let key = format!("consumer_group:{}", group_id);
        let lock_key = format!("lock:consumer_group:{}", group_id);
        log::debug!("Updating heartbeat for consumer group: {}", group_id);

        let result = self.with_redis_lock(conn, &lock_key, self.ttl_secs, |conn| async move {
            log::debug!("Acquiring lock for consumer group: {}", key);
            let mut conn = conn.lock().await;
            let maybe_value: Option<String> = match conn.get(&key).await {
                Ok(val) => val,
                Err(e) => {
                    log::error!("❌ Failed to get key {} from Redis: {}", key, e);
                    return Err(anyhow::anyhow!("Redis GET error: {}", e));
                }
            };
            let value = match maybe_value {
                Some(v) => v,
                None => {
                    log::warn!("Consumer group not found for key: {}", key);
                    return Ok(None); // or your preferred fallback
                }
            };
            let mut consumer_group: ConsumerGroup = match serde_json::from_str(&value) {
                Ok(group) => group,
                Err(e) => {
                    log::error!("❌ Failed to deserialize ConsumerGroup: {}", e);
                    return Err(anyhow::anyhow!("Deserialize error: {}", e));
                }
            };
            log::debug!("Before Updated value for consumer group: {:?}", consumer_group);
            // Set an expiration time for the lock
            consumer_group.update_group_status(10); // Assuming 10 seconds as heartbeat timeout
            // Save the updated consumer group back to Redis
            let updated_value = serde_json::to_string(&consumer_group)?;
            log::debug!("Updated value for consumer group: {}", updated_value);
            let _: () = conn.set(&key, &updated_value).await?;
            Ok(Some(consumer_group))
        }).await?;
        Ok(result)
    }

    async fn offset_commit(&self, group_id: &str, topic: &str, partition: i32, offset: i64) -> Result<()> {
        // get consumer group and update offset
        let conn = self.conn.clone();
        let key = format!("consumer_group:{}", group_id);
        let lock_key: String = format!("lock:consumer_group:{}", group_id);

        let topic = topic.to_string();
        self.with_redis_lock(conn, &lock_key, self.ttl_secs, move |conn| async move {
            let mut conn = conn.lock().await;
            let value: String = conn.get(&key).await?;
            let mut consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            // Update the offset for the specified topic and partition
            consumer_group.update_offset(&topic, partition, offset);
            // Save the updated consumer group back to Redis
            let updated_value = serde_json::to_string(&consumer_group)?;
            let _: () = conn.set(&key, &updated_value).await?;
            Ok(())
        }).await
    }

    async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        // Remove the member from the consumer group
        let conn = self.conn.clone();
        let key = format!("consumer_group:{}", group_id);
        let lock_key = format!("lock:consumer_group:{}", group_id);
        let member_id = member_id.to_string();
        self.with_redis_lock(conn, &lock_key, self.ttl_secs, move |conn| async move {
            let mut conn = conn.lock().await;
            let value: String = conn.get(&key).await?;
            let mut consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            // Remove the member by ID
            consumer_group.members.retain(|m| m.member_id != member_id);
            // Save the updated consumer group back to Redis
            let updated_value = serde_json::to_string(&consumer_group)?;
            let _: () = conn.set(&key, &updated_value).await?;
            Ok(())
        }).await
    }

    async fn update_heartbeat_by_member_id(&self, group_id: &str, member_id: &str) -> Result<Option<ConsumerGroup>> {
        // Update the heartbeat for a specific member in the consumer group
        let conn = self.conn.clone();
        let key = format!("consumer_group:{}", group_id);
        let lock_key = format!("lock:consumer_group:{}", group_id);
        let member_id = member_id.to_string();
        let result = self.with_redis_lock(conn, &lock_key, self.ttl_secs, move |conn| async move {
            let mut conn = conn.lock().await;
            let value: String = conn.get(&key).await?;
            let mut consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            // Find the member and update its last heartbeat
            if let Some(member) = consumer_group.members.iter_mut().find(|m| m.member_id == member_id) {
                member.last_heartbeat = std::time::SystemTime::now();
            }
            // Save the updated consumer group back to Redis
            let updated_value = serde_json::to_string(&consumer_group)?;
            let _: () = conn.set(&key, &updated_value).await?;
            Ok(Some(consumer_group))
        }).await?;
        Ok(result)
    }

    async fn update_consumer_group_member(&self, group_id: &str, member: &ConsumerGroupMember) -> Result<()> {
        let conn = self.conn.clone();
        let key = format!("consumer_group:{}", group_id);
        let lock_key = format!("lock:consumer_group:{}", group_id);
        let member = member.clone();
        self.with_redis_lock(conn, &lock_key, self.ttl_secs, move |conn| async move {
            let mut conn = conn.lock().await;
            let maybe_value: Option<String> = conn.get(&key).await?;
            let value = match maybe_value {
                Some(v) => v,
                None => {
                    return Err(anyhow::anyhow!("Consumer group not found: {}", key));
                }
            };
            let mut consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            log::debug!("Before updating member: {:?}", consumer_group);
            // Update or add the member
            consumer_group.upsert_member(member);
            // Save the updated consumer group back to Redis
            let updated_value = serde_json::to_string(&consumer_group)?;
            log::debug!("Updated consumer group member: {:?}", updated_value);
            let _: () = conn.set(&key, &updated_value).await?;
            Ok(())
        }).await
    }

    async fn gen_producer_id(&self) -> Result<i64> {
        let conn = self.conn.clone();
        let key = "producer_id_counter01";
        let lock_key = format!("lock:{}", key);
        
        let result = self.with_redis_lock(conn, &lock_key, self.ttl_secs, move |conn| async move {
            let mut conn = conn.lock().await;
            let id = conn.incr(key, 1).await
                .map_err(|e| anyhow::anyhow!("Failed to increment producer ID: {}", e))?;
            Ok(id)
        }).await?;
        Ok(result)
    }
}

impl RedisMetaStore {
    pub async fn with_redis_lock<F, Fut, T>(
        &self,
        conn: Arc<Mutex<ClusterConnection>>,
        lock_key: &str,
        ttl_secs: i64,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(Arc<Mutex<ClusterConnection>>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        const MAX_RETRIES: usize = 5;
        const RETRY_DELAY_MS: u64 = 100;
        use tokio::time::sleep;
        use std::time::Duration;
    
        let mut acquired = false;
    
        for attempt in 0..MAX_RETRIES {
            let mut c = conn.lock().await;
            match c.set_nx(lock_key, "lock").await {
                Ok(true) => {
                    log::debug!("✅ Lock acquired: {} (attempt {})", lock_key, attempt + 1);
                    let _: () = c.expire(lock_key, ttl_secs).await?;
                    acquired = true;
                    break;
                }
                Ok(false) => {
                    log::debug!(
                        "🔒 Lock busy (attempt {}/{}): {}. Retrying...",
                        attempt + 1,
                        MAX_RETRIES,
                        lock_key
                    );
                }
                Err(e) => {
                    log::error!("❌ Redis error while acquiring lock {}: {}", lock_key, e);
                    return Err(anyhow::anyhow!("Redis error while acquiring lock: {}", e));
                }
            }
            // drop(c); // Not necessary in async scope; will drop on scope exit
            sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
        }
    
        if !acquired {
            return Err(anyhow::anyhow!("Failed to acquire lock: {}", lock_key));
        }
    
        let result = f(conn.clone()).await;
    
        let mut c = conn.lock().await;
        let _: () = c.del(lock_key).await?;
    
        result
    }

    async fn get_topic_id_by_name(
        &self,
        conn: &mut ClusterConnection,
        name: &str,
    ) -> Result<Option<String>> {
        let index_key = format!("topic_index:name:{}", name);
        let topic_ids: Vec<String> = conn.smembers(&index_key).await?;
        Ok(topic_ids.into_iter().next())
    }
}
