use crate::common::response::send_kafka_response;
use crate::handler::context::HandlerContext;
use crate::traits::meta_store::MetaStore;
use anyhow::Result;
use kafka_protocol::error::ResponseError::UnknownServerError;
use kafka_protocol::messages::{
    RequestHeader,
    offset_commit_request::OffsetCommitRequest,
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
};

pub async fn handle_offset_commit_request(
    header: &RequestHeader,
    request: &OffsetCommitRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>> {
    log::info!(
        "Handling OffsetCommitRequest API VERSION {}",
        header.request_api_version
    );
    log::debug!("OffsetCommitRequest: {:?}", request);

    let meta_store = handler_ctx.meta_store.clone();
    // update heartbeat for the consumer group
    match meta_store
        .update_consumer_group(request.group_id.as_str(), move |mut cg| async move {
            cg.update_group_status(10);
            Ok(cg)
        })
        .await
    {
        Ok(g) => g,
        Err(e) => {
            log::error!(
                "Failed to check heartbeat for group {}: {:?}",
                request.group_id.as_str(),
                e
            );
            return Err(e.into());
        }
    };

    let mut response = OffsetCommitResponse::default();
    let mut topic_responses = Vec::new();
    for topic in &request.topics {
        let mut topic_response = OffsetCommitResponseTopic::default();
        topic_response.name = topic.name.clone();
        let mut partition_responses = Vec::new();
        for partition in &topic.partitions {
            let topic_name = topic.name.clone();
            let partition_index = partition.partition_index;
            let committed_offset = partition.committed_offset;
            let partition_response = match meta_store
                .update_consumer_group(&request.group_id, move |mut cg| async move {
                    cg.update_offset(&topic_name, partition_index, committed_offset);
                    Ok(cg)
                })
                .await
            {
                Ok(_) => {
                    log::debug!(
                        "Successfully committed offset for topic: {}, partition: {}",
                        topic.name.as_str(),
                        partition.partition_index
                    );
                    let mut partition_response = OffsetCommitResponsePartition::default();
                    partition_response.partition_index = partition_index;
                    partition_response.error_code = 0;
                    partition_response
                }
                Err(e) => {
                    log::error!(
                        "Failed to commit offset for topic: {}, partition: {}: {:?}",
                        topic.name.as_str(),
                        partition.partition_index,
                        e
                    );
                    let mut partition_response = OffsetCommitResponsePartition::default();
                    partition_response.error_code = UnknownServerError.code();
                    partition_response
                }
            };
            partition_responses.push(partition_response);
        }
        topic_response.partitions = partition_responses;
        topic_responses.push(topic_response);
    }
    response.topics = topic_responses;
    response.throttle_time_ms = 0;

    log::debug!("Sent OffsetCommitResponse: {:?}", response);
    send_kafka_response(header, &response).await
}
