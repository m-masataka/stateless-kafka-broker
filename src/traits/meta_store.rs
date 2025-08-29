use crate::common::{
    topic_partition::Topic,
    consumer::ConsumerGroup,
};
use anyhow::Result;

#[trait_variant::make(MetaStore: Send)]
pub trait UnsendMetaStore {
    async fn put_topic(&self, data: &Topic) -> Result<()>;
    async fn get_topic(&self, topic_id: &str) -> Result<Topic>;
    async fn get_topics(&self) -> Result<Vec<Topic>>;
    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()>;
    async fn get_topic_id_by_topic_name(&self, topic_name: &str) -> Result<Option<String>>;
    async fn save_consumer_group(&self, data: &ConsumerGroup) -> Result<()>;
    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>>;
    async fn get_consumer_groups(&self) -> Result<Vec<ConsumerGroup>>;
    async fn gen_producer_id(&self) -> Result<i64>;
    async fn update_consumer_group<F, Fut>(
        &self,
        group_id: &str,
        update_fn: F,
    ) -> Result<Option<ConsumerGroup>>where
        F: FnOnce(ConsumerGroup) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<ConsumerGroup>> + Send + 'static;
}