use crate::common::{
    topic_partition::Topic,
    consumer::ConsumerGroup,
    consumer::ConsumerGroupMember,
};
use std::io::Result;

pub trait MetaStore: Send + Sync {
    fn save_topic_partition_info(&self, data: &Topic) -> Result<()>;
    fn get_topic_info(&self, name: Option<&str>, topic_id: Option<&str>) -> Result<Option<Topic>>;
    fn delete_topic_by_name(&self, name: &str) -> Result<()>;
    fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()>;
    fn get_all_topics(&self) -> Result<Vec<Topic>>;
    fn save_consumer_group(&self, data: &ConsumerGroup) -> Result<()>;
    fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>>;
    fn check_heartbeat(&self, group_id: &str) -> Result<Option<ConsumerGroup>>;
    fn offset_commit(&self, group_id: &str, topic: &str, partition: i32, offset: i64) -> Result<()>;
    fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()>;
    fn update_heartbeat(&self, group_id: &str, member_id: &str) -> Result<Option<ConsumerGroup>>;
    fn update_consumer_group_member(&self, group_id: &str, member: &ConsumerGroupMember) -> Result<()>;
    fn gen_producer_id(&self) -> Result<i64>;
}