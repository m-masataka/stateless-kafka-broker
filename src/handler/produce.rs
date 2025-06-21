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

use crate::{common::response::send_kafka_response, storage::log_store_impl::LogStoreImpl, traits::log_store::LogStore};
use crate::common::config::ClusterConfig;



pub async fn handle_produce_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &ProduceRequest,
    cluster_config: &ClusterConfig,
    log_store: &LogStoreImpl,
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

            match log_store
                .write_batch(
                    &topic_data.name,
                    request_partition_data.index,
                    request_partition_data.records.as_ref(),
                )
                .await
            {
                Ok(base_offset) => {
                    partition_produce_response.error_code = 0;
                    partition_produce_response.base_offset = base_offset;
                    partition_produce_response.log_append_time_ms = 0;
                    partition_produce_response.log_start_offset = 0;
                }
                Err(e) => {
                    log::error!("Failed to write records to file: {:?}", e);
                    partition_produce_response.error_code = ResponseError::KafkaStorageError.code();
                    partition_produce_response.base_offset = 0;
                    partition_produce_response.log_append_time_ms = 0;
                    partition_produce_response.log_start_offset = -1;
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
