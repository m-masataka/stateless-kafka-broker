use bytes::Bytes;
use kafka_protocol::protocol::StrBytes;
use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::messages::{
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::{ConsumerGroupHeartbeatResponse, Assignment},
    RequestHeader,
};
use kafka_protocol::error::ResponseError::UnknownServerError;
use crate::{common::response::send_kafka_response, traits::meta_store::MetaStore};
use kafka_protocol::protocol::Decodable;

pub async fn handle_consumer_group_heartbeat_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &ConsumerGroupHeartbeatRequest,
    meta_store: &dyn MetaStore,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling ConsumerGroupHeartbeatRequest API VERSION {}", header.request_api_version);
    log::debug!("ConsumerGroupHeartbeatRequest: {:?}", request);

    let mut response = ConsumerGroupHeartbeatResponse::default();
    response.throttle_time_ms = 0;
    match meta_store.update_heartbeat(&request.group_id, &request.member_id.as_str()) {
        Ok(cg) => {
            log::info!("Successfully updated heartbeat for group: {}", request.group_id.as_str());
            if let Some(consumer_group) = &cg{
                response.error_code = UnknownServerError.code();
                match consumer_group.get_member_by_id(&request.member_id) {
                    Some(member) => {
                        response.error_code = 0; // 0 means no error
                        response.member_id = Some(StrBytes::from_string(member.member_id.clone()));
                        response.heartbeat_interval_ms = 10;
                        response.assignment = match bytes_to_assigment(member.assignment.as_ref(), header.request_api_version) {
                            Ok(Some(assignment)) => {
                                log::info!("Successfully decoded assignment for member: {}", request.member_id);
                                Some(assignment)
                            },
                            Ok(None) => {
                                log::warn!("No assignment found for member: {}", request.member_id);
                                None
                            },
                            Err(e) => {
                                log::error!("Failed to decode assignment for member {}: {:?}", request.member_id, e);
                                response.error_code = UnknownServerError.code();
                                None
                            }
                        };
                    },
                    None => {
                        log::warn!("Consumer group member not found: {}", request.member_id);
                        response.error_code = UnknownServerError.code();
                        response.member_id = Some(request.member_id.clone());
                    }
                };
            } else {
                log::warn!("Consumer group not found: {}", request.group_id.as_str());
                response.error_code = UnknownServerError.code();
            }
        },
        Err(e) => {
            log::error!("Failed to update heartbeat for group {}: {:?}", request.group_id.as_str(), e);
            response.error_code = UnknownServerError.code();
            return Err(e.into());
        }
    }

    log::debug!("ConsumerGroupHeartbeatResponse: {:?}", response);
    send_kafka_response(stream, header, &response).await?;
    log::info!("Sent ConsumerGroupHeartbeatResponse");
    Ok(())
}

fn bytes_to_assigment(bytes: Option<&Bytes>, version: i16) -> Result<Option<Assignment>>{
    match bytes {
        Some(b) => {
            let mut assignment_bytes = Bytes::from(b.clone());
            Assignment::decode(&mut assignment_bytes, version)
                .map(Some)
                .map_err(|e| {
                    log::error!("Failed to decode assignment: {:?}", e);
                    e.into()
                })
        },
        None => Ok(None),
    }
}