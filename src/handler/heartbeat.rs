use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;

use crate::common::response::send_kafka_response;

pub async fn handle_heartbeat_request<W>(
    stream: &mut W,
    header: &RequestHeader
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    // TODO: Implement heartbeat logic
    let mut response = HeartbeatResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = 0;

    send_kafka_response(stream, header, &response).await?;
    println!("Sent HeartbeatResponse");
    Ok(())
}