use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::delete_topics_response::{
    DeleteTopicsResponse,
    DeletableTopicResult,
};
use kafka_protocol::messages::delete_topics_request::{
    DeleteTopicsRequest,
};
use kafka_protocol::error::ResponseError::UnknownTopicOrPartition;

use crate::{common::{response::send_kafka_response}};
use crate::traits::{
    meta_store::MetaStore,
    log_store::LogStore,
};

pub async fn handle_delete_topics_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &DeleteTopicsRequest,
    meta_store: &dyn MetaStore,
    log_store: &dyn LogStore,
) -> Result<()>
where
W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling DeleteTopicsRequest API VERSION {}", header.request_api_version);
    log::debug!("DeleteTopicsRequest: {:?}", request);

    let mut response = DeleteTopicsResponse::default();
    response.throttle_time_ms = 0;
    if header.request_api_version < 5 {
        response.responses = request.topic_names
            .iter()
            .map(|topic_name| {
                match meta_store.delete_topic_by_name(topic_name.as_str()) {
                    Ok(_) => {
                        log::info!("Successfully deleted topic: {}", topic_name.as_str());
                        log_store.delete_topic_by_name(topic_name.as_str()).unwrap_or_else(|e| {
                            log::error!("Failed to delete topic from log store: {}", e);
                        });
                        let mut result = DeletableTopicResult::default();
                        result.name = Some(topic_name.clone());
                        result.error_code = 0; // 0 means no error
                        result.topic_id = uuid::Uuid::new_v4(); // topic_id is not used in this version
                        result
                    },
                    Err(e) => {
                        log::error!("Failed to delete topic {}: {:?}", topic_name.as_str(), e);
                        let mut result = DeletableTopicResult::default();
                        result.name = Some(topic_name.clone());
                        result.error_code = UnknownTopicOrPartition.code();
                        result
                    },
                }
            }) // 0 means no error
            .collect::<Vec<_>>();
    } else {
        response.responses = request.topics
            .iter()
            .map(|topic| {
                if let Some(name) = &topic.name {
                    log::debug!("Deleting topic by name: {}", name.as_str());
                    match meta_store.delete_topic_by_name(name.as_str()) {
                        Ok(_) => {
                            log::info!("Successfully deleted topic: {}", name.as_str());
                            log_store.delete_topic_by_name(name.as_str()).unwrap_or_else(|e| {
                                log::error!("Failed to delete topic from log store: {}", e);
                            });
                            let mut result = DeletableTopicResult::default();
                            result.name = Some(name.clone());
                            result.error_code = 0; // 0 means no error
                            result.topic_id = uuid::Uuid::new_v4(); // topic_id is not used in this version
                            result
                        },
                        Err(e) => {
                            log::error!("Failed to delete topic {}: {:?}", name.as_str(), e);
                            let mut result = DeletableTopicResult::default();
                            result.name = Some(name.clone());
                            result.error_code = UnknownTopicOrPartition.code();
                            result
                        },
                    }
                } else {
                    log::debug!("Deleting topic by ID: {}", topic.topic_id);
                    match meta_store.delete_topic_by_id(topic.topic_id) {
                        Ok(_) => {
                            log::info!("Successfully deleted topic: {}", topic.topic_id);
                            log_store.delete_topic_by_id(topic.topic_id).unwrap_or_else(|e| {
                                log::error!("Failed to delete topic from log store: {}", e);
                            });
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
                
            })
            .collect::<Vec<_>>();
    }
    send_kafka_response(stream, header, &response).await?;
    Ok(())
}