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
use crate::traits::meta_store::MetaStore;
use crate::common::response::send_kafka_response;

pub async fn handle_offset_commit_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &OffsetCommitRequest,
    meta_store: &dyn MetaStore,
) -> Result<()> 
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling OffsetCommitRequest API VERSION {}", header.request_api_version);
    // groupを更新
    match meta_store.check_heartbeat(request.group_id.as_str()) {
        Ok(g) => g,
        Err(e) => {
            log::error!("Failed to check heartbeat for group {}: {:?}", request.group_id.as_str(), e);
            return Err(e.into());
        }
    };

    let mut response = OffsetCommitResponse::default();
    response.topics = request.topics
        .iter()
        .map(|topic| {
            let mut topic_response =  OffsetCommitResponseTopic::default();
            topic_response.name = topic.name.clone();
            topic_response.partitions = topic.partitions
                .iter()
                .map(|partition| {
                    match meta_store.offset_commit(
                        &request.group_id,
                        &topic.name,
                        partition.partition_index,
                        partition.committed_offset,
                    ) {
                        Ok(_) => {
                            log::info!("Successfully committed offset for topic: {}, partition: {}", topic.name.as_str(), partition.partition_index);
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
                    }
                })
                .collect();
            topic_response
        }).collect();
    response.throttle_time_ms = 0;
    send_kafka_response(stream, header, &response).await?;
    log::debug!("OffsetCommitRequest: {:?}", request);
    Ok(())
}