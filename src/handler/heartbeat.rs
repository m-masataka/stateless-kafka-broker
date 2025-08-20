use anyhow::Result;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;

use crate::common::response::send_kafka_response;
use crate::handler::context::HandlerContext;

pub async fn handle_heartbeat_request(
    header: &RequestHeader,
    _request: &HeartbeatRequest,
    _handler_ctx: &HandlerContext,
) -> Result<Vec<u8>>
{
    let mut response = HeartbeatResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = 0;

    log::info!("Heartbeat response sent successfully for API VERSION {}", header.request_api_version);
    send_kafka_response(header, &response).await
}