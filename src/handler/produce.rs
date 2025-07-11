use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::
{   
    error::ResponseError,
    messages::{
        BrokerId,
        RequestHeader,
        produce_response::{
            ProduceResponse,
            TopicProduceResponse,
            PartitionProduceResponse,
            LeaderIdAndEpoch,
            NodeEndpoint,
        },
        produce_request::ProduceRequest,
    },
    protocol::StrBytes, 
};

use crate::{
    common::response::send_kafka_response,
    storage::{index_store_impl::IndexStoreImpl, log_store_impl::LogStoreImpl, meta_store_impl::MetaStoreImpl},
    traits::{index_store::IndexStore, log_store::LogStore, meta_store::MetaStore}
};
use crate::common::config::ClusterConfig;



pub async fn handle_produce_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &ProduceRequest,
    cluster_config: &ClusterConfig,
    meta_store: &MetaStoreImpl,
    log_store: &LogStoreImpl,
    index_store: &IndexStoreImpl,
) -> Result<()> 
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling ProduceRequest API VERSION {}", header.request_api_version);
    log::debug!("ProduceRequest: {:?}", request);

    let mut response = ProduceResponse::default();
    response.throttle_time_ms = 0;

    let mut topic_responses = Vec::new();
    for topic_data in &request.topic_data {
        let mut topic_produce_response = TopicProduceResponse::default();
        topic_produce_response.name = topic_data.name.clone();


        let mut partition_responses = Vec::new();

        for request_partition_data in &topic_data.partition_data {
            let mut partition_produce_response = PartitionProduceResponse::default();
            partition_produce_response.index = request_partition_data.index;

            let topic_info = meta_store.get_topic(Some(topic_data.name.clone().as_str()), None).await
                .map_err(|e| {
                    log::error!("Failed to get topic info: {:?}", e);
                    ResponseError::UnknownTopicOrPartition
                })?;

            let topic_id = topic_info
                .as_ref()
                .ok_or_else(|| {
                    log::error!("Topic info is None");
                    ResponseError::UnknownTopicOrPartition
                })?
                .topic_id;


            // lock index store
            try_lock_with_retry(
                index_store,
                &topic_id.to_string(),
                request_partition_data.index,
                10, // TTL in seconds
                5, // Wait for 5 seconds before giving up
            ).await.map_err(|e| {
                log::error!("Failed to lock index store: {:?}", e);
                ResponseError::KafkaStorageError
            })?;

            let start_offset = index_store.read_offset(&topic_id.to_string(), request_partition_data.index).await
                .map_err(|e| {
                    log::error!("Failed to read offset: {:?}", e);
                    ResponseError::KafkaStorageError
                })?;

            match log_store
                .write_records(
                    &topic_id.to_string(),
                    request_partition_data.index,
                    start_offset,
                    request_partition_data.records.as_ref(),
                ).await
            { 
                Ok((current_offset, key )) => {
                    partition_produce_response.error_code = 0;
                    partition_produce_response.base_offset = start_offset;
                    partition_produce_response.log_append_time_ms = 0;
                    partition_produce_response.log_start_offset = 0;

                    // Update the index store with the new base offset and key
                    index_store.set_index(
                        &topic_id.to_string(),
                        request_partition_data.index,
                        current_offset,
                        &key,
                    ).await?;

                    // update the offset in the index store
                    index_store.write_offset(&topic_id.to_string(), request_partition_data.index, current_offset).await
                        .map_err(|e| {
                            log::error!("Failed to set offset in index store: {:?}", e);
                            ResponseError::KafkaStorageError
                        })?;

                    // Unlock the index store after writing
                    index_store.unlock_exclusive(&topic_id.to_string(), request_partition_data.index).await
                        .map_err(|e| {
                            log::error!("Failed to unlock index store: {:?}", e);
                            ResponseError::KafkaStorageError
                        })?;
                }
                Err(e) => {
                    log::error!("Failed to write records to file: {:?}", e);
                    partition_produce_response.error_code = ResponseError::KafkaStorageError.code();
                    partition_produce_response.base_offset = 0;
                    partition_produce_response.log_append_time_ms = 0;
                    partition_produce_response.log_start_offset = -1;
                    // Unlock the index store even if writing fails
                    index_store.unlock_exclusive(&topic_id.to_string(), request_partition_data.index).await
                        .map_err(|e| {
                            log::error!("Failed to unlock index store after error: {:?}", e);
                            ResponseError::KafkaStorageError
                        })?;
                }
            }

            let mut leader = LeaderIdAndEpoch::default();
            leader.leader_id = BrokerId(1);
            leader.leader_epoch = 0;
            partition_produce_response.current_leader = leader;

            partition_responses.push(partition_produce_response);
        }

        topic_produce_response.partition_responses = partition_responses;
        topic_responses.push(topic_produce_response);
    }
    response.responses = topic_responses;

    // Set the node endpoint information
    let mut node_endpoint = NodeEndpoint::default();
    node_endpoint.node_id = BrokerId(cluster_config.controller_id);
    node_endpoint.host = StrBytes::from_string(cluster_config.host.clone());
    node_endpoint.port = cluster_config.port;
    response.node_endpoints = vec![node_endpoint];

    log::debug!("ProduceResponse: {:?}", response);
    send_kafka_response(stream, header, &response).await?;
    log::debug!("Sent ProduceResponse");
    Ok(())
}

use tokio::time::{sleep, Duration, Instant};

async fn try_lock_with_retry(
    index_store: &IndexStoreImpl,
    topic: &str,
    partition: i32,
    ttl_secs: i64,
    wait_secs: u64,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(wait_secs);

    loop {
        match index_store.lock_exclusive(topic, partition, ttl_secs).await {
            Ok(true) => return Ok(()), // lock acquired successfully
            Ok(false) => {
                if Instant::now() >= deadline {
                    anyhow::bail!("Timed out waiting for lock");
                }
                sleep(Duration::from_millis(200)).await; // wait before retrying
            }
            Err(e) => return Err(e),
        }
    }
}