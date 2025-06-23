use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::messages::{RequestHeader, TopicName};
use kafka_protocol::messages::delete_topics_response::{
    DeleteTopicsResponse,
    DeletableTopicResult,
};
use kafka_protocol::messages::delete_topics_request::{
    DeleteTopicsRequest,
    DeleteTopicState,
};
use kafka_protocol::error::ResponseError::UnknownTopicOrPartition;

use crate::storage::meta_store_impl::MetaStoreImpl;
use crate::{common::{response::send_kafka_response}};
use crate::traits::meta_store::MetaStore;

pub async fn handle_delete_topics_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &DeleteTopicsRequest,
    meta_store: &MetaStoreImpl,
) -> Result<()>
where
W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling DeleteTopicsRequest API VERSION {}", header.request_api_version);
    log::debug!("DeleteTopicsRequest: {:?}", request);

    let mut response = DeleteTopicsResponse::default();
    response.throttle_time_ms = 0;
    let mut topic_responses = Vec::new();
    if header.request_api_version < 5 {
        for topic_name in &request.topic_names {
            log::debug!("Deleting topic by name: {}", topic_name.as_str());
            let result = delete_topic_by_name(topic_name.as_str(), meta_store).await;
            topic_responses.push(result);
        }
    } else {
        for topic in &request.topics {
            let result = delete_topic(topic, meta_store).await;
            topic_responses.push(result);
        }
    }
    response.responses = topic_responses;
    send_kafka_response(stream, header, &response).await?;
    Ok(())
}

async fn delete_topic(topic: &DeleteTopicState, meta_store: &MetaStoreImpl) -> DeletableTopicResult {
    if let Some(name) = &topic.name {
        log::debug!("Deleting topic by name: {}", name.as_str());
        delete_topic_by_name(name.as_str(), meta_store).await
    } else {
        log::debug!("Deleting topic by ID: {}", topic.topic_id);
        match meta_store.delete_topic_by_id(topic.topic_id).await {
            Ok(_) => {
                log::info!("Successfully deleted topic: {}", topic.topic_id);
                let mut result = DeletableTopicResult::default();
                result.name = topic.name.clone();
                result.topic_id = topic.topic_id;
                result.error_code = 0; // 0 means no error
                result.topic_id = uuid::Uuid::new_v4(); // topic_id is not used in this version
                result
            },
            Err(e) => {
                log::error!("Failed to delete topic {}: {:?}", topic.topic_id, e);
                let mut result = DeletableTopicResult::default();
                result.name = topic.name.clone();
                result.topic_id = topic.topic_id;
                result.error_code = UnknownTopicOrPartition.code();
                result
            },
        }
    } 
}

async fn delete_topic_by_name(
    name: &str,
    meta_store: &MetaStoreImpl,
) -> DeletableTopicResult {
    log::debug!("Deleting topic by name: {}", name);
    match meta_store.delete_topic_by_name(name).await {
        Ok(_) => {
            log::info!("Successfully deleted topic: {}", name);
            let mut result = DeletableTopicResult::default();
            result.name = Some(TopicName(name.to_string().into()));
            result.error_code = 0; // 0 means no error
            result.topic_id = uuid::Uuid::new_v4(); // topic_id is not used in this version
            result
        },
        Err(e) => {
            log::error!("Failed to delete topic {}: {:?}", name, e);
            let mut result = DeletableTopicResult::default();
            result.name = Some(TopicName(name.to_string().into()));
            result.error_code = UnknownTopicOrPartition.code();
            result
        },
    }
}