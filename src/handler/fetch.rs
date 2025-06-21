use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::{
    messages::{
        fetch_request::FetchRequest,
        fetch_response::{
            FetchResponse,
            FetchableTopicResponse,
            PartitionData,
        },
        RequestHeader,
    },
};

use crate::{common::response::send_kafka_response, storage::log_store_impl::LogStoreImpl};
use crate::traits::log_store::LogStore;


pub async fn handle_fetch_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &FetchRequest,
    log_store: &LogStoreImpl,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    log::debug!("Handling FetchRequest API VERSION {}", header.request_api_version);
    log::debug!("FetchRequest: {:?}", request);
    let mut response = FetchResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = 0; // 成功
    response.session_id = request.session_id;
    let mut topic_responses = Vec::new();
    for topic in &request.topics {
        let mut topic_response = FetchableTopicResponse::default();
        let topic_name = topic.topic.clone();
        topic_response.topic = topic_name.clone();
        let mut partition_responses = Vec::new();
        for partition in &topic.partitions {
            log::debug!("FetchRequest partition: {:?}", partition);
            let mut partition_response = PartitionData::default();
            match log_store.read_offset(&topic_name, partition.partition).await {
                Ok(current_offset) => {
                    match log_store.read_records(&topic_name, partition.partition, partition.fetch_offset, current_offset).await {
                        Ok(records) => {
                            partition_response.error_code = 0;
                            partition_response.records = Some(records);
                            partition_response.high_watermark = current_offset + 1;
                            partition_response.last_stable_offset = current_offset + 1;
                            partition_response.log_start_offset = 0; //TODO: Set log start offset
                            partition_response.partition_index = partition.partition;
                        }
                        Err(e) => {
                            log::debug!("Failed to read records for topic {:?} partition {}: {:?}", topic_name, partition.partition, e);
                            // if no data found, set error code to 0 and return empty records
                            partition_response.error_code = 0;
                            partition_response.partition_index = partition.partition;
                            partition_response.records = None;
                            partition_response.high_watermark = 0;
                            partition_response.last_stable_offset = 0;
                            partition_response.log_start_offset = 0;
                            partition_response.partition_index = partition.partition;
                        }
                    }
                }
                Err(e) => {
                    log::debug!("Failed to read offset for topic {:?} partition {}: {:?}", topic_name, partition.partition, e);
                        // if no data found, set error code to 0 and return empty records
                        partition_response.error_code = 0;
                        partition_response.partition_index = partition.partition;
                        partition_response.records = None;
                        partition_response.high_watermark = 0;
                        partition_response.last_stable_offset = 0;
                        partition_response.log_start_offset = 0;
                        partition_response.partition_index = partition.partition;
                }
            }
            partition_responses.push(partition_response);
        }
        topic_response.partitions = partition_responses;
        topic_responses.push(topic_response);
    }
    response.responses = topic_responses;
    
    send_kafka_response(stream, header, &response).await?;
    log::debug!("Sent JoinGroupResponse");
    log::debug!("FetchResponse: {:?}", response);
    Ok(())
}