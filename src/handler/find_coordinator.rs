use crate::common::config::ClusterConfig;
use crate::common::response::{send_kafka_response, send_kafka_response_insert_prefix};
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::find_coordinator_response::{
    FindCoordinatorResponse,
    Coordinator
};
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::BrokerId;
use anyhow::Result;
use tokio::io::AsyncWrite;


pub async fn handle_find_coordinator_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &FindCoordinatorRequest,
    cluster_config: &ClusterConfig,
) -> Result<()> 
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling FindCoordinatorRequest API VERSION {}", header.request_api_version);
    log::info!("FindCoordinatorRequest: {:?}", request);
    let mut response = FindCoordinatorResponse::default();

    // API version 3 and above returns a list of coordinators
    // API version 0-2 returns a single coordinator
    if header.request_api_version >= 3 {
        let mut coordinators = Vec::new();
        for key in &request.coordinator_keys {
            let mut coordinator = Coordinator::default();
            coordinator.key = key.clone();
            coordinator.node_id = BrokerId(cluster_config.node_id);
            coordinator.host = cluster_config.host.clone().into();
            coordinator.port = cluster_config.port;
            coordinator.error_code = 0;
            coordinators.push(coordinator);
        }
        response.coordinators = coordinators;
        send_kafka_response(stream, header, &response).await?;
    } else {
        response.node_id = BrokerId(cluster_config.node_id);
        response.host = cluster_config.host.clone().into();
        response.port = cluster_config.port;
        response.error_code = 0;
        send_kafka_response_insert_prefix(stream, header, &response, false).await?;
    }

    log::debug!("Sent FindCoordinatorResponse");
    log::debug!("FindCoordinatorResponse: {:?}", response);
    Ok(())
}