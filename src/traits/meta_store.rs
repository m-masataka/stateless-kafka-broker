use crate::common::{
    topic_partition::Topic,
    consumer::ConsumerGroup,
    consumer::ConsumerGroupMember,
};
use anyhow::Result;

#[trait_variant::make(MetaStore: Send)]
pub trait UnsendMetaStore {
    async fn save_topic_partition(&self, data: &Topic) -> Result<()>;
    async fn get_topic(&self, name: Option<&str>, topic_id: Option<&str>) -> Result<Option<Topic>>;
    async fn delete_topic_by_name(&self, name: &str) -> Result<()>;
    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()>;
    async fn get_all_topics(&self) -> Result<Vec<Topic>>;
    async fn get_topic_id_by_topic_name(&self, topic_name: &str) -> Result<Option<String>>;
    async fn save_consumer_group(&self, data: &ConsumerGroup) -> Result<()>;
    async fn get_consumer_groups(&self) -> Result<Vec<ConsumerGroup>>;
    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>>;
    async fn offset_commit(&self, group_id: &str, topic: &str, partition: i32, offset: i64) -> Result<()>;
    async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()>;
    async fn update_heartbeat(&self, group_id: &str) -> Result<Option<ConsumerGroup>>;
    async fn update_heartbeat_by_member_id(&self, group_id: &str, member_id: &str) -> Result<Option<ConsumerGroup>>;
    async fn update_consumer_group_member(&self, group_id: &str, member: &ConsumerGroupMember) -> Result<()>;
    async fn gen_producer_id(&self) -> Result<i64>;
}