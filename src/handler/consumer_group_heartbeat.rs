use crate::handler::context::HandlerContext;
use crate::{common::response::send_kafka_response, traits::meta_store::MetaStore};
use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::error::ResponseError::UnknownServerError;
use kafka_protocol::messages::{
    RequestHeader,
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
};
use kafka_protocol::protocol::Decodable;
use kafka_protocol::protocol::StrBytes;

pub async fn handle_consumer_group_heartbeat_request(
    header: &RequestHeader,
    request: &ConsumerGroupHeartbeatRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>> {
    let meta_store = handler_ctx.meta_store.clone();
    log::info!(
        "Handling ConsumerGroupHeartbeatRequest API VERSION {}",
        header.request_api_version
    );
    log::debug!("ConsumerGroupHeartbeatRequest: {:?}", request);

    let mut response = ConsumerGroupHeartbeatResponse::default();
    response.throttle_time_ms = 0;

    let member_id = request.member_id.as_str().to_string().to_owned();
    let consumer_group = match meta_store
        .update_consumer_group(&request.group_id, move |mut cg| async move {
            // Find the member and update its last heartbeat
            if let Some(member) = cg.members.iter_mut().find(|m| m.member_id == member_id) {
                member.last_heartbeat = std::time::SystemTime::now();
            }
            Ok(cg)
        })
        .await
    {
        Ok(cg) => match cg {
            Some(group) => {
                log::info!(
                    "Successfully updated heartbeat for group: {}",
                    request.group_id.as_str()
                );
                group
            }
            None => {
                log::warn!("Consumer group not found: {}", request.group_id.as_str());
                return Err(UnknownServerError.into());
            }
        },
        Err(e) => {
            log::error!(
                "Failed to update heartbeat for group {}: {:?}",
                request.group_id.as_str(),
                e
            );
            response.error_code = UnknownServerError.code();
            return Err(e.into());
        }
    };

    match consumer_group.get_member_by_id(&request.member_id) {
        Some(requested_member) => {
            log::info!(
                "Found consumer group member: {}",
                requested_member.member_id
            );
            response.error_code = 0;
            response.member_id = Some(StrBytes::from_string(requested_member.member_id.clone()));
            response.heartbeat_interval_ms = 10;
            response.assignment = match bytes_to_assigment(
                requested_member.assignment.as_ref(),
                header.request_api_version,
            ) {
                Ok(Some(assignment)) => {
                    log::info!(
                        "Successfully decoded assignment for member: {}",
                        request.member_id
                    );
                    Some(assignment)
                }
                Ok(None) => {
                    log::warn!("No assignment found for member: {}", request.member_id);
                    None
                }
                Err(e) => {
                    log::error!(
                        "Failed to decode assignment for member {}: {:?}",
                        request.member_id,
                        e
                    );
                    response.error_code = UnknownServerError.code();
                    None
                }
            };
        }
        None => {
            log::warn!("Consumer group member not found: {}", request.member_id);
            response.error_code = UnknownServerError.code();
            response.member_id = Some(request.member_id.clone());
        }
    };

    log::debug!("ConsumerGroupHeartbeatResponse: {:?}", response);
    log::info!("Sent ConsumerGroupHeartbeatResponse");
    send_kafka_response(header, &response).await
}

fn bytes_to_assigment(bytes: Option<&Bytes>, version: i16) -> Result<Option<Assignment>> {
    match bytes {
        Some(b) => {
            let mut assignment_bytes = Bytes::from(b.clone());
            Assignment::decode(&mut assignment_bytes, version)
                .map(Some)
                .map_err(|e| {
                    log::error!("Failed to decode assignment: {:?}", e);
                    e.into()
                })
        }
        None => Ok(None),
    }
}
