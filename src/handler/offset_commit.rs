use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::messages::{
    offset_commit_request::{
        OffsetCommitRequest,
    },
    offset_commit_response::{
        OffsetCommitResponse,
        OffsetCommitResponseTopic,
        OffsetCommitResponsePartition,
    },
    RequestHeader,
};
use kafka_protocol::error::ResponseError::UnknownServerError;
use crate::{storage::meta_store_impl::MetaStoreImpl, traits::meta_store::MetaStore};
use crate::common::response::send_kafka_response;

pub async fn handle_offset_commit_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &OffsetCommitRequest,
    meta_store: &MetaStoreImpl,
) -> Result<()> 
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling OffsetCommitRequest API VERSION {}", header.request_api_version);
    log::debug!("OffsetCommitRequest: {:?}", request);
    // update heartbeat for the consumer group
    match meta_store.update_heartbeat(request.group_id.as_str()).await {
        Ok(g) => g,
        Err(e) => {
            log::error!("Failed to check heartbeat for group {}: {:?}", request.group_id.as_str(), e);
            return Err(e.into());
        }
    };

    let mut response = OffsetCommitResponse::default();
    let mut topic_responses = Vec::new();
    for topic in &request.topics {
        let mut topic_response =  OffsetCommitResponseTopic::default();
        topic_response.name = topic.name.clone();
        let mut partition_responses = Vec::new();
        for partition in &topic.partitions {
            let partition_response = match meta_store.offset_commit(
                &request.group_id,
                &topic.name,
                partition.partition_index,
                partition.committed_offset,
            ).await {
                Ok(_) => {
                    log::debug!("Successfully committed offset for topic: {}, partition: {}", topic.name.as_str(), partition.partition_index);
                    let mut partition_response = OffsetCommitResponsePartition::default();
                    partition_response.partition_index = partition.partition_index;
                    partition_response.error_code = 0;
                    partition_response
                },
                Err(e) => {
                    log::error!("Failed to commit offset for topic: {}, partition: {}: {:?}", topic.name.as_str(), partition.partition_index, e);
                    let mut partition_response = OffsetCommitResponsePartition::default();
                    partition_response.error_code = UnknownServerError.code();
                    partition_response
                },
            };
            partition_responses.push(partition_response);
            // Check updated group
            match meta_store.get_consumer_group(&request.group_id).await {
                Ok(Some(consumer_group)) => {
                    log::debug!("Updated consumer group: {:?}", consumer_group);
                },
                Ok(None) => {
                    log::warn!("Consumer group {} not found after commit", request.group_id.as_str());
                },
                Err(e) => {
                    log::error!("Error fetching consumer group {}: {:?}", request.group_id.as_str(), e);
                }
            };
        }
        topic_response.partitions = partition_responses;
        topic_responses.push(topic_response);
    }
    response.topics = topic_responses;
    response.throttle_time_ms = 0;
    send_kafka_response(stream, header, &response).await?;
    log::debug!("Sent OffsetCommitResponse: {:?}", response);
    Ok(())
}