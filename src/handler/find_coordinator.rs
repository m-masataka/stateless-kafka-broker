use crate::common::response::send_kafka_response;
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::find_coordinator_response::{
    FindCoordinatorResponse,
    Coordinator
};
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::BrokerId;
use anyhow::Result;
use crate::handler::context::HandlerContext;

pub async fn handle_find_coordinator_request(
    header: &RequestHeader,
    request: &FindCoordinatorRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>>
{
    log::info!("Handling FindCoordinatorRequest API VERSION {}", header.request_api_version);
    log::info!("FindCoordinatorRequest: {:?}", request);

    let node_config = handler_ctx.node_config.clone();
    let mut response = FindCoordinatorResponse::default();

    // API version 3 and above returns a list of coordinators
    // API version 0-2 returns a single coordinator
    if header.request_api_version >= 3 {
        let mut coordinators = Vec::new();
        for key in &request.coordinator_keys {
            let mut coordinator = Coordinator::default();
            coordinator.key = key.clone();
            coordinator.node_id = BrokerId(node_config.node_id);
            coordinator.host = node_config.advertised_host.clone().into();
            coordinator.port = node_config.advertised_port;
            coordinator.error_code = 0;
            coordinators.push(coordinator);
        }
        response.coordinators = coordinators;
    } else {
        response.node_id = BrokerId(node_config.node_id);
        response.host = node_config.advertised_host.clone().into();
        response.port = node_config.advertised_port;
        response.error_code = 0;
    }
    send_kafka_response(header, &response).await
}