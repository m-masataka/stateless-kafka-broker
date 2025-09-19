use std::time::Duration;

use crate::common::cluster::Node;
use crate::common::consumer::ConsumerGroup;
use crate::common::topic_partition::Topic;
use crate::common::utils::jittered_delay;
use crate::storage::s3::s3_client::S3Client;
use crate::traits::meta_store::UnsendMetaStore;

pub struct S3MetaStore {
    s3_client: S3Client,
    bucket: String,
    prefix: Option<String>,
}
use anyhow::Result;
use tokio::time::sleep;

impl S3MetaStore {
    pub fn new(s3_client: S3Client, bucket: String, prefix: Option<String>) -> Self {
        Self {
            s3_client: s3_client,
            bucket,
            prefix,
        }
    }
}

impl UnsendMetaStore for S3MetaStore {
    async fn put_topic(&self, data: &Topic) -> Result<()> {
        let object_key = self.metadata_key(format!("topic-{}", data.topic_id));
        let value = serde_json::to_string(data)?;
        let value_bytes = value.as_bytes().to_vec();
        // pessmistic lock
        self.s3_client
            .put_object(&self.bucket, &object_key, &value_bytes, None)
            .await
            .map_err(|e| {
                log::error!("Failed to put object to S3: {:?}", e);
                e
            })?;

        log::debug!("Successfully saved topic partition to S3: {}", object_key);
        Ok(())
    }

    async fn get_topic(&self, topic_id: &str) -> Result<Topic> {
        let object_key = self.metadata_key(format!("topic-{}", topic_id));
        let (data, _) = self.s3_client.get_object(&self.bucket, &object_key).await?;
        let topic: Topic = serde_json::from_slice(&data)?;
        log::debug!("Successfully retrieved topic from S3: {}", object_key);
        Ok(topic)
    }

    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()> {
        let object_key = self.metadata_key(format!("topic-{}", topic_id));
        self.s3_client
            .delete_object(&self.bucket, &object_key)
            .await
            .map_err(|e| {
                log::error!("Failed to delete topic from S3: {:?}", e);
                e
            })?;
        log::debug!("Successfully deleted topic from S3: {}", object_key);
        Ok(())
    }

    async fn get_topics(&self) -> Result<Vec<Topic>> {
        let topic_list = self
            .s3_client
            .list_objects(&self.bucket, self.prefix.as_deref())
            .await?;

        let mut topics = Vec::new();
        for object in topic_list {
            if object.starts_with("topic-") {
                let (data, _) = self.s3_client.get_object(&self.bucket, &object).await?;
                if let Ok(topic) = serde_json::from_slice::<Topic>(&data) {
                    topics.push(topic);
                } else {
                    log::warn!("Failed to deserialize topic from S3: {}", object);
                }
            }
        }
        log::debug!("Successfully retrieved all topics from S3");
        Ok(topics)
    }

    async fn get_topic_id_by_topic_name(&self, topic_name: &str) -> Result<Option<String>> {
        // TODO: Implement logic to find topic ID by name
        log::debug!("Retrieving topic ID for topic name: {}", topic_name);
        Ok(None)
    }

    async fn save_consumer_group(
        &self,
        data: &crate::common::consumer::ConsumerGroup,
    ) -> Result<()> {
        let object_key = self.metadata_key(format!("consumer-group-{}", data.group_id));
        let lock_key = self.metadata_key(format!("lock-consumer-group-{}", data.group_id));
        let value = serde_json::to_string(data)?;
        let value_bytes = value.into_bytes(); // Vec<u8>

        // Acquire lock before writing
        self.acquire_lock(&self.bucket, &lock_key).await?;
        let result = self
            .s3_client
            .put_object(&self.bucket, &object_key, &value_bytes, None)
            .await;
        if let Err(e) = result {
            log::error!("Failed to put consumer group object to S3: {:?}", e);
            self.release_lock(&self.bucket, &lock_key).await?;
            return Err(e.into());
        } else {
            // Release lock after writing
            self.release_lock(&self.bucket, &lock_key).await?;
            log::debug!("Successfully saved consumer group to S3: {}", object_key);
            Ok(())
        }
    }

    async fn get_consumer_groups(&self) -> Result<Vec<ConsumerGroup>> {
        let group_list = self
            .s3_client
            .list_objects(&self.bucket, self.prefix.as_deref())
            .await?;

        let mut groups = Vec::new();
        for object in group_list {
            if object.starts_with("consumer-group-") {
                let (data, _) = self.s3_client.get_object(&self.bucket, &object).await?;
                if let Ok(group) = serde_json::from_slice::<ConsumerGroup>(&data) {
                    groups.push(group);
                } else {
                    log::warn!("Failed to deserialize consumer group from S3: {}", object);
                }
            }
        }
        log::debug!("Successfully retrieved all consumer groups from S3");
        Ok(groups)
    }

    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        let object_key = self.metadata_key(format!("consumer-group-{}", group_id));
        match self.s3_client.get_object(&self.bucket, &object_key).await {
            Ok((data, _)) => {
                let group: ConsumerGroup = serde_json::from_slice(&data)?;
                log::debug!(
                    "Successfully retrieved consumer group from S3: {}",
                    object_key
                );
                Ok(Some(group))
            }
            Err(e) => {
                log::warn!(
                    "Failed to get consumer group from S3: {} - {:?}",
                    group_id,
                    e
                );
                Ok(None)
            }
        }
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
        let object_key = self.metadata_key(format!("consumer-group-{}", group_id));
        let lock_key = self.metadata_key(format!("lock-consumer-group-{}", group_id));
        // Acquire lock before writing
        self.acquire_lock(&self.bucket, &lock_key).await?;
        let mut group = self.get_consumer_group(group_id).await?;
        let updated_group = if let Some(ref mut group) = group {
            // Apply the update function
            match update_fn(group.clone()).await {
                Ok(updated) => {
                    let value = serde_json::to_string(&updated)?;
                    let value_bytes = value.into_bytes(); // Vec<u8>
                    self.s3_client
                        .put_object(&self.bucket, &object_key, &value_bytes, None)
                        .await?;
                    log::debug!(
                        "Successfully updated consumer group via closure: {}",
                        group_id
                    );
                    Some(updated)
                }
                Err(e) => {
                    log::error!(
                        "Failed to apply update function for consumer group {}: {:?}",
                        group_id,
                        e
                    );
                    None
                }
            }
        } else {
            log::warn!("Consumer group not found for closure update: {}", group_id);
            None
        };
        self.release_lock(&self.bucket, &lock_key).await?;
        Ok(updated_group)
    }

    async fn gen_producer_id(&self) -> Result<i64> {
        let object_key = self.metadata_key("producer-id".to_string());
        let lock_key = self.metadata_key("lock-producer-id".to_string());

        // Acquire lock before writing
        self.acquire_lock(&self.bucket, &lock_key).await?;

        let mut producer_id = 0;
        match self.s3_client.get_object(&self.bucket, &object_key).await {
            Ok((data, _)) => {
                if let Ok(id) = String::from_utf8(data.to_vec()) {
                    producer_id = id.parse::<i64>().unwrap_or(0);
                }
            }
            Err(e) => {
                log::warn!("Failed to get producer ID from S3: {:?}", e);
            }
        }

        producer_id += 1;
        let value = producer_id.to_string();
        let value_bytes = value.into_bytes(); // Vec<u8>

        self.s3_client
            .put_object(&self.bucket, &object_key, &value_bytes, None)
            .await?;

        log::debug!("Successfully generated new producer ID: {}", producer_id);

        self.release_lock(&self.bucket, &lock_key).await?;

        Ok(producer_id)
    }

    async fn update_cluster_status(&self, node_config: &Node) -> Result<()> {
        Ok(())
    }

    async fn get_cluster_status(&self) -> Result<Vec<Node>> {
        Ok(vec![])
    }
}

impl S3MetaStore {
    fn metadata_key(&self, key: String) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, key),
            None => format!("{}", key),
        }
    }

    async fn acquire_lock(&self, bucket: &str, lock_key: &str) -> Result<()> {
        const MAX_RETRIES: usize = 200;
        const RETRY_DELAY_MS: u64 = 100;

        let mut acquired = false;

        for attempt in 0..MAX_RETRIES {
            match self.s3_client.acquire_lock(bucket, lock_key).await {
                Ok(_) => {
                    log::debug!("✅ Lock acquired: {} (attempt {})", lock_key, attempt + 1);
                    acquired = true;
                    break;
                }
                Err(e) => {
                    log::warn!(
                        "🔒 Lock attempt {}/{} failed for {}: {}",
                        attempt + 1,
                        MAX_RETRIES,
                        lock_key,
                        e
                    );
                    sleep(Duration::from_millis(jittered_delay(RETRY_DELAY_MS))).await;
                }
            }
        }

        if !acquired {
            return Err(anyhow::anyhow!(
                "Failed to acquire lock after retries: {}",
                lock_key
            ));
        }
        return Ok(());
    }

    async fn release_lock(&self, bucket: &str, key: &str) -> Result<()> {
        self.s3_client
            .release_lock(bucket, key)
            .await
            .map_err(|e| {
                log::error!("Failed to release lock {}: {:?}", key, e);
                e
            })?;
        log::debug!("Successfully released lock: {}", key);
        Ok(())
    }
}
