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

use crate::{common::response::send_kafka_response, traits::log_store::LogStore};
use crate::common::config::ClusterConfig;



pub async fn handle_produce_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &ProduceRequest,
    cluster_config: &ClusterConfig,
    log_store: &dyn LogStore,
) -> Result<()> 
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling ProduceRequest API VERSION {}", header.request_api_version);

    let mut response = ProduceResponse::default();
    response.throttle_time_ms = 0;
    println!("Produce Request:: {:?}", request);
    response.responses = request.topic_data
        .iter()
        .map(|topic_data| {
            let mut topic_produce_response = TopicProduceResponse::default();
            topic_produce_response.name = topic_data.name.clone();
            topic_produce_response.partition_responses = topic_data.partition_data.iter()
                .map(|request_partition_data|{
                    let mut partition_produce_response = PartitionProduceResponse::default();
                    partition_produce_response.index = request_partition_data.index;
                    // write record to file
                    match log_store.write_batch(
                        &topic_data.name,
                        request_partition_data.index,
                        request_partition_data.records.as_ref()
                    ) {
                        Ok(base_offset) => {
                            partition_produce_response.error_code = 0; // 成功
                            partition_produce_response.base_offset = base_offset; // オフセットは後で設定
                            partition_produce_response.log_append_time_ms = 0; // ログ追加時間
                            partition_produce_response.log_start_offset = 0; // ログ開始オフセット
                        }
                        Err(e) => {
                            log::error!("Failed to write records to file: {:?}", e);
                            partition_produce_response.error_code = ResponseError::KafkaStorageError.code();
                            partition_produce_response.base_offset = 0;
                            partition_produce_response.log_append_time_ms = 0;
                            partition_produce_response.log_start_offset = -1;
                        }
                    }
                    let mut cl = LeaderIdAndEpoch::default();
                    cl.leader_id = BrokerId(1);
                    cl.leader_epoch = 0;
                    partition_produce_response.current_leader = cl;
                    partition_produce_response
                })
                .collect();
            topic_produce_response.name = topic_data.name.clone();
            topic_produce_response
        }).collect();
    let mut node_endpoint = NodeEndpoint::default();
    node_endpoint.node_id = BrokerId(cluster_config.controller_id);
    node_endpoint.host = StrBytes::from_string(cluster_config.host.clone());
    node_endpoint.port = cluster_config.port;

    response.node_endpoints = vec![node_endpoint];

    // レスポンスをエンコードして送信
    log::debug!("ProduceResponse: {:?}", response);
    send_kafka_response(stream, header, &response).await?;
    log::debug!("Sent ProduceResponse");
    Ok(())
}
