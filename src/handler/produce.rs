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
    common::{index::index_data, response::send_kafka_response, utils::jittered_delay},
    storage::index_store_impl::IndexStoreImpl,
    traits::{
        index_store::IndexStore,
        log_store::LogStore,
        meta_store::MetaStore
    }
};
use crate::handler::context::HandlerContext;

pub async fn handle_produce_request(
    header: &RequestHeader,
    request: &ProduceRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>>
{
    log::debug!("Handling ProduceRequest API VERSION {}", header.request_api_version);

    let meta_store = handler_ctx.meta_store.clone();
    let index_store = handler_ctx.index_store.clone();
    let log_store = handler_ctx.log_store.clone();
    let cluster_config = handler_ctx.cluster_config.clone();
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


            let lock_start = Instant::now();
            // lock index store
            let lock_id = try_lock_with_retry(
                &index_store,
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

            let start = Instant::now();  // Start timing the write operation for performance measurement
            match log_store
                .write_records(
                    &topic_id.to_string(),
                    request_partition_data.index,
                    start_offset,
                    request_partition_data.records.as_ref(),
                ).await
            { 
                Ok((current_offset, key , size)) => {
                    let elapsed = start.elapsed();
                    log::debug!(
                        "📝 write_records took {:.2?} ms for topic={}, partition={}, size={:?}",
                        elapsed.as_millis(),
                        topic_id.to_string(),
                        request_partition_data.index,
                        request_partition_data.records.as_ref().map(|r| r.len())
                    );

                    partition_produce_response.error_code = 0;
                    partition_produce_response.base_offset = start_offset;
                    partition_produce_response.log_append_time_ms = 0;
                    partition_produce_response.log_start_offset = 0;

                    // Update the index store with the new base offset and key
                    let data = index_data(&key, size);
                    index_store.set_index(
                        &topic_id.to_string(),
                        request_partition_data.index,
                        current_offset,
                        &data,
                    ).await?;

                    // update the offset in the index store
                    index_store.write_offset(&topic_id.to_string(), request_partition_data.index, current_offset).await
                        .map_err(|e| {
                            log::error!("Failed to set offset in index store: {:?}", e);
                            ResponseError::KafkaStorageError
                        })?;

                    // Unlock the index store after writing
                    index_store.unlock_exclusive(&topic_id.to_string(), request_partition_data.index, &lock_id).await
                        .map_err(|e| {
                            log::error!("Failed to unlock index store: {:?}", e);
                            ResponseError::KafkaStorageError
                        })?;
                    let lock_duration = lock_start.elapsed();
                    log::debug!("Lock duration: {:.2?} ms", lock_duration.as_millis());
                }
                Err(e) => {
                    log::error!("Failed to write records to file: {:?}", e);
                    partition_produce_response.error_code = ResponseError::KafkaStorageError.code();
                    partition_produce_response.base_offset = 0;
                    partition_produce_response.log_append_time_ms = 0;
                    partition_produce_response.log_start_offset = -1;
                    // Unlock the index store even if writing fails
                    index_store.unlock_exclusive(&topic_id.to_string(), request_partition_data.index, &lock_id).await
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

    log::debug!("Sent ProduceResponse");
    send_kafka_response(header, &response).await
}

use tokio::time::{sleep, Duration, Instant};

async fn try_lock_with_retry(
    index_store: &IndexStoreImpl,
    topic: &str,
    partition: i32,
    ttl_secs: i64,
    wait_secs: u64,
) -> anyhow::Result<String> {
    let deadline = Instant::now() + Duration::from_secs(wait_secs);

    loop {
        match index_store.lock_exclusive(topic, partition, ttl_secs).await {
            Ok(lock_id) => {
                match lock_id {
                    Some(id) => return Ok(id), // lock acquired successfully
                    None => {
                        if Instant::now() >= deadline {
                            anyhow::bail!("Timed out waiting for lock");
                        }
                        sleep(Duration::from_millis(jittered_delay(200))).await; // wait before retrying
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }
}