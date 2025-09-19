use crate::common::{cluster::Node, consumer::ConsumerGroup, topic_partition::Topic};
use crate::storage::file::file_meta_store::FileMetaStore;
use crate::storage::redis::redis_meta_store::RedisMetaStore;
use crate::storage::s3::s3_meta_store::S3MetaStore;
use crate::storage::tikv::tikv_meta_store::TikvMetaStore;
use crate::traits::meta_store::{MetaStore, UnsendMetaStore};
use anyhow::Result;

pub enum MetaStoreImpl {
    File(FileMetaStore),
    S3(S3MetaStore),
    Redis(RedisMetaStore),
    Tikv(TikvMetaStore),
}

impl MetaStore for MetaStoreImpl {
    async fn put_topic(&self, data: &Topic) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.put_topic(data).await,
            MetaStoreImpl::S3(s) => s.put_topic(data).await,
            MetaStoreImpl::Redis(r) => r.put_topic(data).await,
            MetaStoreImpl::Tikv(t) => t.put_topic(data).await,
        }
    }

    async fn get_topic(&self, topic_id: &str) -> Result<Topic> {
        match self {
            MetaStoreImpl::File(f) => f.get_topic(topic_id).await,
            MetaStoreImpl::S3(s) => s.get_topic(topic_id).await,
            MetaStoreImpl::Redis(r) => r.get_topic(topic_id).await,
            MetaStoreImpl::Tikv(t) => t.get_topic(topic_id).await,
        }
    }

    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.delete_topic_by_id(topic_id).await,
            MetaStoreImpl::S3(s) => s.delete_topic_by_id(topic_id).await,
            MetaStoreImpl::Redis(r) => r.delete_topic_by_id(topic_id).await,
            MetaStoreImpl::Tikv(t) => t.delete_topic_by_id(topic_id).await,
        }
    }

    async fn get_topics(&self) -> Result<Vec<Topic>> {
        match self {
            MetaStoreImpl::File(f) => f.get_topics().await,
            MetaStoreImpl::S3(s) => s.get_topics().await,
            MetaStoreImpl::Redis(r) => r.get_topics().await,
            MetaStoreImpl::Tikv(t) => t.get_topics().await,
        }
    }

    async fn get_topic_id_by_topic_name(&self, topic_name: &str) -> Result<Option<String>> {
        match self {
            MetaStoreImpl::File(f) => f.get_topic_id_by_topic_name(topic_name).await,
            MetaStoreImpl::S3(s) => s.get_topic_id_by_topic_name(topic_name).await,
            MetaStoreImpl::Redis(r) => r.get_topic_id_by_topic_name(topic_name).await,
            MetaStoreImpl::Tikv(t) => t.get_topic_id_by_topic_name(topic_name).await,
        }
    }

    async fn save_consumer_group(&self, data: &ConsumerGroup) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.save_consumer_group(data).await,
            MetaStoreImpl::S3(s) => s.save_consumer_group(data).await,
            MetaStoreImpl::Redis(r) => r.save_consumer_group(data).await,
            MetaStoreImpl::Tikv(t) => t.save_consumer_group(data).await,
        }
    }

    async fn get_consumer_groups(&self) -> Result<Vec<ConsumerGroup>> {
        match self {
            MetaStoreImpl::File(f) => f.get_consumer_groups().await,
            MetaStoreImpl::S3(s) => s.get_consumer_groups().await,
            MetaStoreImpl::Redis(r) => r.get_consumer_groups().await,
            MetaStoreImpl::Tikv(t) => t.get_consumer_groups().await,
        }
    }

    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        match self {
            MetaStoreImpl::File(f) => f.get_consumer_group(group_id).await,
            MetaStoreImpl::S3(s) => s.get_consumer_group(group_id).await,
            MetaStoreImpl::Redis(r) => r.get_consumer_group(group_id).await,
            MetaStoreImpl::Tikv(t) => t.get_consumer_group(group_id).await,
        }
    }

    async fn gen_producer_id(&self) -> Result<i64> {
        match self {
            MetaStoreImpl::File(f) => f.gen_producer_id().await,
            MetaStoreImpl::S3(s) => s.gen_producer_id().await,
            MetaStoreImpl::Redis(r) => r.gen_producer_id().await,
            MetaStoreImpl::Tikv(t) => t.gen_producer_id().await,
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
        match self {
            MetaStoreImpl::File(f) => f.update_consumer_group(group_id, update_fn).await,
            MetaStoreImpl::S3(s) => s.update_consumer_group(group_id, update_fn).await,
            MetaStoreImpl::Redis(r) => r.update_consumer_group(group_id, update_fn).await,
            MetaStoreImpl::Tikv(t) => t.update_consumer_group(group_id, update_fn).await,
        }
    }

    async fn update_cluster_status(&self, node_config: &Node) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.update_cluster_status(node_config).await,
            MetaStoreImpl::S3(s) => s.update_cluster_status(node_config).await,
            MetaStoreImpl::Redis(r) => r.update_cluster_status(node_config).await,
            MetaStoreImpl::Tikv(t) => t.update_cluster_status(node_config).await,
        }
    }

    async fn get_cluster_status(&self) -> Result<Vec<Node>> {
        match self {
            MetaStoreImpl::File(f) => f.get_cluster_status().await,
            MetaStoreImpl::S3(s) => s.get_cluster_status().await,
            MetaStoreImpl::Redis(r) => r.get_cluster_status().await,
            MetaStoreImpl::Tikv(t) => t.get_cluster_status().await,
        }
    }
}
