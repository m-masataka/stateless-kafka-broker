use std::time::SystemTime;
use anyhow::Result;
use kafka_protocol::messages::sync_group_request::SyncGroupRequest;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::messages::sync_group_response::{
    SyncGroupResponse,
};
use kafka_protocol::error::ResponseError;

use crate::common::consumer::ConsumerGroupMember;
use crate::common::response::send_kafka_response;
use crate::traits::meta_store::MetaStore;
use crate::handler::context::HandlerContext;
use bytes::Bytes;


pub async fn handle_sync_group_request(
    header: &RequestHeader,
    request: &SyncGroupRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>>
{
    log::info!("Handling SyncGroupRequest API VERSION {}", header.request_api_version);
    log::debug!("SyncGroupRequest: {:?}", request);

    let meta_store = handler_ctx.meta_store.clone();
    // get consumer group
    let group_id = request.group_id.clone();
    let consumer_group = meta_store.get_consumer_group(group_id.as_str()).await?;
    log::debug!("Consumer group found: {:?}", consumer_group);

    // TODO: 
    let response = if let Some(consumer_group) = consumer_group {
        log::info!("Found existing consumer group: {}", *group_id.clone());
        let member = match consumer_group.get_member_by_id(&request.member_id) {
            Some(member) => {
                log::info!("Found existing member: {}", request.member_id);
                member.clone()
            },
            None => {
                log::warn!("Member {} not found in group {}", request.member_id, group_id.as_str());
                ConsumerGroupMember {
                    member_id: request.member_id.to_string(),
                    is_leader: false,
                    is_pending: false,
                    last_heartbeat: SystemTime::now(),
                    metadata: None,
                    assignment: Some(request.assignments.iter()
                        .map(|a| a.assignment.clone())
                        .next().unwrap_or_else(Bytes::new)),
                }
            }
        };

        match meta_store.update_consumer_group_member(&group_id.clone(), &member).await {
            Ok(_) => {
                log::info!("Successfully updated consumer group member: {}", request.member_id);
            },
            Err(e) => {
                log::error!("Failed to update consumer group member {}: {:?}", request.member_id, e);
                return Err(e.into());
            }
        }

        let protocol_type: StrBytes = request.protocol_type.clone().unwrap_or_else(|| "consumer".into());
        let protocol_name: StrBytes = request.protocol_name.clone().unwrap_or_else(|| "range".into());

        let mut g = SyncGroupResponse::default();
        g.throttle_time_ms = 0;
        g.error_code = 0;
        g.protocol_type = Some(protocol_type);
        g.protocol_name = Some(protocol_name);
        g.assignment = request
            .assignments
            .iter()
            .map(|a|a.assignment.clone())
            .next()
            .unwrap_or_else(Bytes::new); 
        g
    } else {
        let mut g = SyncGroupResponse::default();
        g.error_code = ResponseError::GroupIdNotFound.code();
        g
    };


    log::debug!("SyncGroupResponse: {:?}", response);
    log::debug!("Sent SyncGroupResponse");
    send_kafka_response(header, &response).await
}
