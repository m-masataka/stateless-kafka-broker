use crate::storage::redis::redis_client::RedisClient;
use crate::traits::meta_store::MetaStore;
use crate::common::topic_partition::Topic;
use crate::common::consumer::{ConsumerGroup, ConsumerGroupMember};
use anyhow::Result;

pub struct RedisMetaStore {
    redis_client: RedisClient,
}

impl RedisMetaStore {
    pub fn new(redis_client: RedisClient) -> Self {
        Self {
            redis_client: redis_client,
        }
    }
}

impl MetaStore for RedisMetaStore {
    async fn save_topic_partition_info(&self, data: &Topic) -> Result<()> {
        // set the topic information in Redis
        // using the topic name as the key and the serialized data as the value
        let key = format!("topic:{}", data.topic_id);
        let value = serde_json::to_string(data)?;
        self.redis_client.set(&key, &value).await?;

        match &data.name {
            Some(name) => {
                // If name is provided, create an index key for quick lookup
                let index_key = format!("topic_index:name:{}", name);
                self.redis_client.sadd(&index_key, &data.topic_id.to_string()).await?;
            }
            None => {
                // Nothing to do if name is not provided
            }
        }
        Ok(())
    }

    async fn get_topic_info(&self, name: Option<&str>, topic_id: Option<&str>) -> Result<Option<Topic>> {
        // get the topic information from Redis
        if let Some(name) = name {
            let topic_id = self.get_topic_id_by_name(name).await?
                .unwrap();
            let key = format!("topic:{}", topic_id);
            if let Some(value) = self.redis_client.get(&key).await? {
                let topic: Topic = serde_json::from_str(&value)?;
                return Ok(Some(topic));
            }
        } else if let Some(topic_id) = topic_id {
            // If topic_id is provided, search topic by topic_id
            let key = format!("topic:{}", topic_id);
            if let Some(value) = self.redis_client.get(&key).await? {
                let topic: Topic = serde_json::from_str(&value)?;
                return Ok(Some(topic));
            }
        }
        Ok(None)
    }

    async fn delete_topic_by_name(&self, name: &str) -> Result<()> {
        let topic_id = self.get_topic_id_by_name(name).await?
            .unwrap();
        let key = format!("topic:{}", topic_id);
        self.redis_client.del(&key).await?;
        Ok(())
    }

    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()> {
        let key = format!("topic:{}", topic_id);
        self.redis_client.del(&key).await?;
        Ok(())
    }

    async fn get_all_topics(&self) -> Result<Vec<Topic>> {
        let keys: Vec<String> = self.redis_client.keys("topic:*").await?;
        let mut topics = Vec::new();
        for key in keys {
            if let Some(value) = self.redis_client.get(&key).await? {
                if let Ok(topic) = serde_json::from_str::<Topic>(&value) {
                    topics.push(topic);
                }
            }
        }
        if topics.is_empty() {
            return Ok(Vec::new());
        }
        Ok(topics)
    }

    async fn save_consumer_group(&self, data: &ConsumerGroup) -> Result<()> {
        // Save the consumer group information in Redis
        let key = format!("consumer_group:{}", data.group_id);
        let value = serde_json::to_string(data)?;
        self.redis_client.set(&key, &value).await?;
        Ok(())
    }

    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        let key = format!("consumer_group:{}", group_id);
        if let Some(value) = self.redis_client.get(&key).await? {
            let consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            return Ok(Some(consumer_group));
        }
        Ok(None)
    }

    async fn update_heartbeat(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        // update the heartbeart for the consumer group
        let key = format!("consumer_group:{}", group_id);
        if let Some(value) = self.redis_client.get(&key).await? {
            self.redis_client.lock_exclusive(&key, 10).await?;

            let mut consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            consumer_group.update_group_status(10); // Assuming 10 seconds as heartbeat timeout
            // Save the updated consumer group back to Redis
            let updated_value = serde_json::to_string(&consumer_group)?;
            self.redis_client.set(&key, &updated_value).await?;
            self.redis_client.unlock(&key).await?;
            return Ok(Some(consumer_group));
        }
        Ok(None)
    }

    async fn offset_commit(&self, group_id: &str, topic: &str, partition: i32, offset: i64) -> Result<()> {
        // get consumer group and update offset
        let key = format!("consumer_group:{}", group_id);
        if let Some(value) = self.redis_client.get(&key).await? {
            self.redis_client.lock_exclusive(&key, 10).await?;
            let mut consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            // Update the offset for the specified topic and partition
            consumer_group.update_offset(topic, partition, offset);
            // Save the updated consumer group back to Redis
            let updated_value = serde_json::to_string(&consumer_group)?;
            self.redis_client.set(&key, &updated_value).await?;
            self.redis_client.unlock(&key).await?;
        } else {
            return Err(anyhow::anyhow!("Consumer group not found"));
        }
        Ok(())
    }

    async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        // Remove the member from the consumer group
        let key = format!("consumer_group:{}", group_id);
        if let Some(value) = self.redis_client.get(&key).await? {
            self.redis_client.lock_exclusive(&key, 3).await?;
            let mut consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            // Remove the member by ID
            consumer_group.members.retain(|m| m.member_id != member_id);
            // Save the updated consumer group back to Redis
            let updated_value = serde_json::to_string(&consumer_group)?;
            self.redis_client.set(&key, &updated_value).await?;
            self.redis_client.unlock(&key).await?;
        } else {
            return Err(anyhow::anyhow!("Consumer group not found"));
        }
        Ok(())
    }

    async fn update_heartbeat_by_member_id(&self, group_id: &str, member_id: &str) -> Result<Option<ConsumerGroup>> {
        // Update the heartbeat for a specific member in the consumer group
        let key = format!("consumer_group:{}", group_id);
        if let Some(value) = self.redis_client.get(&key).await? {
            self.redis_client.lock_exclusive(&key, 10).await?;
            let mut consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            // Find the member and update its last heartbeat
            if let Some(member) = consumer_group.members.iter_mut().find(|m| m.member_id == member_id) {
                member.last_heartbeat = std::time::SystemTime::now();
            }
            // Save the updated consumer group back to Redis
            let updated_value = serde_json::to_string(&consumer_group)?;
            self.redis_client.set(&key, &updated_value).await?;
            self.redis_client.unlock(&key).await?;
            return Ok(Some(consumer_group));
        }
        Ok(None)
    }

    async fn update_consumer_group_member(&self, group_id: &str, member: &ConsumerGroupMember) -> Result<()> {
        let key = format!("consumer_group:{}", group_id);
        if let Some(value) = self.redis_client.get(&key).await? {
            self.redis_client.lock_exclusive(&key, 3).await?;
            let mut consumer_group: ConsumerGroup = serde_json::from_str(&value)?;
            // Update or add the member
            consumer_group.upsert_member(member.clone());
            // Save the updated consumer group back to Redis
            let updated_value = serde_json::to_string(&consumer_group)?;
            self.redis_client.set(&key, &updated_value).await?;
            self.redis_client.unlock(&key).await?;
        } else {
            return Err(anyhow::anyhow!("Consumer group not found"));
        }
        Ok(())
    }

    async fn gen_producer_id(&self) -> Result<i64> {
        // Generate a new producer ID
        let key = "producer_id_counter";
        self.redis_client.lock_exclusive(key, 10).await?;
        self.redis_client
            .incr(key, 1)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to increment producer ID: {}", e))?;
        let producer_id: i64 = self.redis_client.get(key).await?
            .ok_or_else(|| anyhow::anyhow!("Failed to retrieve producer ID"))?
            .parse()
            .map_err(|e| anyhow::anyhow!("Failed to parse producer ID: {}", e))?;
        self.redis_client.unlock(key).await?;
        Ok(producer_id)
    }
}

impl RedisMetaStore {
    async fn get_topic_id_by_name(&self, name: &str) -> Result<Option<String>> {
        let index_key = format!("topic_index:name:{}", name);
        let topic_ids: Vec<String> = self.redis_client.smembers(&index_key).await?;
        if topic_ids.is_empty() {
            Ok(None)
        } else {
            Ok(Some(topic_ids[0].clone()))
        }
    }
}
