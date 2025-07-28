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

use crate::{common::response::send_kafka_response, storage::{index_store_impl::IndexStoreImpl, log_store_impl::LogStoreImpl, meta_store_impl::MetaStoreImpl}, traits::meta_store::MetaStore};
use crate::traits::log_store::LogStore;
use crate::traits::index_store::IndexStore;


pub async fn handle_fetch_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &FetchRequest,
    log_store: &LogStoreImpl,
    meta_store: &MetaStoreImpl,
    index_store: &IndexStoreImpl,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    log::debug!("Handling FetchRequest API VERSION {}", header.request_api_version);
    log::debug!("FetchRequest: {:?}", request);
    let mut response = FetchResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = 0; // set succeeded error code
    response.session_id = request.session_id;
    let mut topic_responses = Vec::new();
    for topic in &request.topics {
        let mut topic_response = FetchableTopicResponse::default();
        topic_response.topic = topic.topic.clone();
        let mut partition_responses = Vec::new();
        
        for partition in &topic.partitions {
            log::debug!("FetchRequest partition: {:?}", partition);
            let mut partition_response = PartitionData::default();
            match meta_store.get_topic_id_by_topic_name(&topic.topic).await? {
                Some(topic_id) => {
                    match index_store.read_offset(&topic_id.to_string(), partition.partition).await {
                        Ok(current_offset) => {
                            match index_store.get_index_from_start_offset(&topic_id.to_string(), partition.partition, partition.fetch_offset).await {
                                Ok(keys) => {
                                    match log_store.read_records(keys).await {
                                        Ok(records) => {
                                            partition_response.error_code = 0;
                                            partition_response.records = Some(records);
                                            partition_response.high_watermark = current_offset + 1;
                                            partition_response.last_stable_offset = current_offset + 1;
                                            partition_response.log_start_offset = 0; //TODO: Set log start offset
                                            partition_response.partition_index = partition.partition;
                                        }
                                        Err(e) => {
                                            log::debug!("Failed to read records for topic {:?} partition {}: {:?}", topic_id, partition.partition, e);
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
                                    log::debug!("Failed to read records for topic {:?} partition {}: {:?}", topic_id, partition.partition, e);
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
                            log::debug!("Failed to read offset for topic {:?} partition {}: {:?}", topic_id, partition.partition, e);
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
                None => {
                    log::debug!("Topic {:?} not found", topic.topic);
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
    log::debug!("Sent FetchResponse");
    log::debug!("FetchResponse: {:?}", response);
    Ok(())
}