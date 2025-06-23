
use crate::storage::s3::s3_client::S3Client;
use crate::traits::meta_store::UnsendMetaStore;
use crate::common::topic_partition::Topic;
use crate::common::consumer::{ConsumerGroup, ConsumerGroupMember};

pub struct S3MetaStore {
    s3_client: S3Client,
    bucket: String,
    prefix: Option<String>,
    topic_metadata_file_name: String,
    offset_file_name: String,
}
use anyhow::Result;

impl S3MetaStore {
    pub fn new(s3_client: S3Client, bucket: String, prefix: Option<String>) -> Self {
        Self {
            s3_client: s3_client,
            bucket,
            prefix,
            topic_metadata_file_name: "metadata.json".to_string(),
            offset_file_name: "offset.txt".to_string(),
        }
    }
}

impl UnsendMetaStore for S3MetaStore {
    async fn save_topic_partition_info(&self, data: &crate::common::topic_partition::Topic) -> Result<()> {
        Ok(())
    }

    async fn get_topic_info(&self, name: Option<&str>, topic_id: Option<&str>) -> Result<Option<Topic>> {
        Ok(None)
    }

    async fn delete_topic_by_name(&self, name: &str) -> Result<()> {
        Ok(())
    }

    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()> {
        Ok(())
    }

    async fn get_all_topics(&self) -> Result<Vec<Topic>> {
        Ok(Vec::new())
    }

    async fn save_consumer_group(&self, data: &crate::common::consumer::ConsumerGroup) -> Result<()> {
        Ok(())
    }

    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        Ok(None)
    }

    async fn update_heartbeat(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        Ok(None)
    }

    async fn offset_commit(&self, group_id: &str, topic: &str, partition: i32, offset: i64) -> Result<()> {
        Ok(())
    }

    async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        Ok(())
    }

    async fn update_heartbeat_by_member_id(&self, group_id: &str, member_id: &str) -> Result<Option<ConsumerGroup>> {
        Ok(None)
    }

    async fn update_consumer_group_member(&self, group_id: &str, member: &ConsumerGroupMember) -> Result<()> {
        Ok(())
    }

    async fn gen_producer_id(&self) -> Result<i64> {
        Ok(0)
    }
    

}

impl S3MetaStore {
    fn topic_metadata_key(&self) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, self.topic_metadata_file_name),
            None => format!("{}", self.topic_metadata_file_name),
        }
    }
}