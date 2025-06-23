use crate::common::consumer::ConsumerGroupMember;
use crate::storage::redis::redis_meta_store::RedisMetaStore;
use crate::traits::meta_store::{UnsendMetaStore, MetaStore};
use crate::storage::file::file_meta_store::FileMetaStore;
use crate::storage::s3::s3_meta_store::S3MetaStore;
use crate::common::{
    topic_partition::Topic,
    consumer::ConsumerGroup,
};
use anyhow::Result;

pub enum MetaStoreImpl {
    File(FileMetaStore),
    S3(S3MetaStore),
    Redis(RedisMetaStore),
}

impl MetaStore for MetaStoreImpl {
    async fn save_topic_partition_info(&self, data: &Topic) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.save_topic_partition_info(data).await,
            MetaStoreImpl::S3(s) => s.save_topic_partition_info(data).await,
            MetaStoreImpl::Redis(r) => r.save_topic_partition_info(data).await,
        }
    }

    async fn get_topic_info(&self, name: Option<&str>, topic_id: Option<&str>) -> Result<Option<Topic>> {
        match self {
            MetaStoreImpl::File(f) => f.get_topic_info(name, topic_id).await,
            MetaStoreImpl::S3(s) => s.get_topic_info(name, topic_id).await,
            MetaStoreImpl::Redis(r) => r.get_topic_info(name, topic_id).await,
        }
    }

    async fn delete_topic_by_name(&self, name: &str) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.delete_topic_by_name(name).await,
            MetaStoreImpl::S3(s) => s.delete_topic_by_name(name).await,
            MetaStoreImpl::Redis(r) => r.delete_topic_by_name(name).await,
        }
    }

    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.delete_topic_by_id(topic_id).await,
            MetaStoreImpl::S3(s) => s.delete_topic_by_id(topic_id).await,
            MetaStoreImpl::Redis(r) => r.delete_topic_by_id(topic_id).await,
        }
    }

    async fn get_all_topics(&self) -> Result<Vec<Topic>> {
        match self {
            MetaStoreImpl::File(f) => f.get_all_topics().await,
            MetaStoreImpl::S3(s) => s.get_all_topics().await,
            MetaStoreImpl::Redis(r) => r.get_all_topics().await,
        }
    }

    async fn save_consumer_group(&self, data: &ConsumerGroup) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.save_consumer_group(data).await,
            MetaStoreImpl::S3(s) => s.save_consumer_group(data).await,
            MetaStoreImpl::Redis(r) => r.save_consumer_group(data).await,
        }
    }

    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        match self {
            MetaStoreImpl::File(f) => f.get_consumer_group(group_id).await,
            MetaStoreImpl::S3(s) => s.get_consumer_group(group_id).await,
            MetaStoreImpl::Redis(r) => r.get_consumer_group(group_id).await,
        }
    }

    async fn update_heartbeat(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        match self {
            MetaStoreImpl::File(f) => f.update_heartbeat(group_id).await,
            MetaStoreImpl::S3(s) => s.update_heartbeat(group_id).await,
            MetaStoreImpl::Redis(r) => r.update_heartbeat(group_id).await,
        }
    }

    async fn offset_commit(&self, group_id: &str, topic: &str, partition: i32, offset: i64) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.offset_commit(group_id, topic, partition, offset).await,
            MetaStoreImpl::S3(s) => s.offset_commit(group_id, topic, partition, offset).await,
            MetaStoreImpl::Redis(r) => r.offset_commit(group_id, topic, partition, offset).await,
        }
    }

    async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.leave_group(group_id, member_id).await,
            MetaStoreImpl::S3(s) => s.leave_group(group_id, member_id).await,
            MetaStoreImpl::Redis(r) => r.leave_group(group_id, member_id).await,
        }
    }

    async fn update_heartbeat_by_member_id(&self, group_id: &str, member_id: &str) -> Result<Option<ConsumerGroup>> {
        match self {
            MetaStoreImpl::File(f) => f.update_heartbeat_by_member_id(group_id, member_id).await,
            MetaStoreImpl::S3(s) => s.update_heartbeat_by_member_id(group_id, member_id).await,
            MetaStoreImpl::Redis(r) => r.update_heartbeat_by_member_id(group_id, member_id).await,
        } 
    }

    async fn update_consumer_group_member(&self, group_id: &str, member: &ConsumerGroupMember) -> Result<()> {
        match self {
            MetaStoreImpl::File(f) => f.update_consumer_group_member(group_id, member).await,
            MetaStoreImpl::S3(s) => s.update_consumer_group_member(group_id, member).await,
            MetaStoreImpl::Redis(r) => r.update_consumer_group_member(group_id, member).await,
        }
    }

    async fn gen_producer_id(&self) -> Result<i64> {
        match self {
            MetaStoreImpl::File(f) => f.gen_producer_id().await,
            MetaStoreImpl::S3(s) => s.gen_producer_id().await,
            MetaStoreImpl::Redis(r) => r.gen_producer_id().await,
        }
    }
}