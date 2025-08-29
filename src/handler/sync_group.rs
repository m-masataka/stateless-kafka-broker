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
    let group_id = request.group_id.as_str().to_owned();
    let member_id = request.member_id.as_str().to_owned();
    let assignments = request.assignments.clone();
    let group_id_cloned = group_id.clone();
    let consumer_group = meta_store.update_consumer_group(&group_id, move |mut cg| async move {
        let member = match cg.get_member_by_id(&member_id) {
            Some(m) => {
                log::info!("Found existing member: {}", member_id);
                m.clone()
            },
            None => {
                log::warn!("Member {} not found in group {}", member_id, group_id_cloned.as_str());
                ConsumerGroupMember {
                    member_id: member_id.to_string(),
                    is_leader: false,
                    is_pending: false,
                    last_heartbeat: SystemTime::now(),
                    metadata: None,
                    assignment: Some(assignments.iter()
                        .map(|a| a.assignment.clone())
                        .next().unwrap_or_else(Bytes::new)),
                }
            }
        };
        cg.upsert_member(member);
        Ok(cg)
    }).await?;

    let response = if let Some(_) = consumer_group {

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
