use anyhow::Result;
use std::time::SystemTime;
use uuid::Uuid;
use std::collections::BTreeMap;
use kafka_protocol::messages::join_group_request::JoinGroupRequest;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::messages::join_group_response::{
    JoinGroupResponse,
    JoinGroupResponseMember
};

use crate::common::consumer::{ConsumerGroup, ConsumerGroupMember, ConsumerGroupTopic};
use crate::common::response::send_kafka_response;
use crate::traits::meta_store::MetaStore;
use crate::handler::context::HandlerContext;

pub async fn handle_join_group_request(
    header: &RequestHeader,
    request: &JoinGroupRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>>
{
    log::info!("Handling JoinGroupRequest API VERSION {}", header.request_api_version);
    log::debug!("JoinGroupRequest: {:?}", request);

    let meta_store = handler_ctx.meta_store.clone();
    // Get the consumer group from the meta store
    let group_id = request.group_id.clone();
    // Update the heartbeat for the consumer group
    let mut consumer_group = meta_store.update_consumer_group(group_id.as_str(), move |mut cg| async move {
        cg.update_group_status(10);
        Ok(cg)
    }).await?;
    log::debug!("Consumer group after heartbeat check: {:?}", consumer_group);

    let response = if let Some(store_group) = consumer_group.as_mut() {
        let member_id = if request.member_id.is_empty() {
            generate_member_id(&group_id)
        } else {
            request.member_id.clone().to_string()
        };
        let requester_id = member_id.clone();
        if store_group.is_rebalancing {
            log::warn!("Consumer group {} is currently rebalancing, rejecting join request", *group_id);
            let generation_id = store_group.generation_id + 1; // New generation ID
            let member_id_str= generate_member_id(&group_id);
            let requester_id = member_id_str.clone();
            let leader_id = member_id_str.clone();
            let cg = new_consumer_group(request, member_id_str, leader_id, &group_id, generation_id, store_group.topics.clone());
            meta_store.save_consumer_group(&cg).await?;
        
            convert_consumer_group_to_join_response(
                &cg,
                requester_id,
                0,
            )
        } else {
            // If the group exists and is not rebalancing, update the member
            log::info!("Found existing consumer group: {}", *group_id.clone());
            let member = ConsumerGroupMember {
                member_id: member_id,
                is_leader: false,
                is_pending: false,
                last_heartbeat: SystemTime::now(),
                metadata: request.protocols.first().map(|p| p.metadata.clone()),
                assignment: None,
            };
            store_group.members.push(member);
            meta_store.save_consumer_group(&store_group).await?;
            convert_consumer_group_to_join_response(store_group, requester_id, 0)
        }
    } else {
        // if the group does not exist, create a new one
        log::info!("Consumer group not found, creating new group: {}", *group_id.clone());
        let member_id_str = generate_member_id(&group_id);
        let requester_id = member_id_str.clone();
        let leader_id = member_id_str.clone();
        let cg = new_consumer_group(request, member_id_str, leader_id, &group_id, 1, None);
        meta_store.save_consumer_group(&cg).await?;
        
        convert_consumer_group_to_join_response(
            &cg,
            requester_id,
            0,
        )
    };

    log::debug!("JoinGroupResponse: {:?}", response);
    log::debug!("Sent JoinGroupResponse");
    send_kafka_response(header, &response).await
}

fn generate_member_id(group_id: &str) -> String {
    let uuid = Uuid::new_v4();
    format!("{}-{}", group_id, uuid)
}

/// Convert a ConsumerGroup into a JoinGroupResponse that can be returned from the broker.
pub fn convert_consumer_group_to_join_response(
    cg: &ConsumerGroup,
    requester_id: String,
    error_code: i16,
) -> JoinGroupResponse {
    // Convert the members of the ConsumerGroup into JoinGroupResponseMember
    let members: Vec<JoinGroupResponseMember> = cg
        .members
        .iter()
        .map(|m| {
            let mut resp_member = JoinGroupResponseMember::default();
            resp_member.member_id = StrBytes::from(m.member_id.clone());
            resp_member.group_instance_id = None;
            resp_member.metadata = m
                .metadata
                .clone()
                .unwrap_or_default();
            resp_member.unknown_tagged_fields = BTreeMap::new();
            resp_member
        })
        .collect();

    let mut response = JoinGroupResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = error_code;
    response.generation_id = cg.generation_id;
    response.protocol_type = Some(StrBytes::from(cg.protocol_type.clone()));
    response.protocol_name = Some(StrBytes::from(cg.protocol_name.clone()));
    response.leader = StrBytes::from(cg.leader_id.clone());
    response.skip_assignment = false;
    response.member_id = StrBytes::from(requester_id);
    response.members = members;
    response.unknown_tagged_fields = BTreeMap::new();

    response
}

fn new_consumer_group(request: &JoinGroupRequest, member_id: String, leader_id: String, group_id: &StrBytes, generation_id: i32, topics: Option<Vec<ConsumerGroupTopic>>) -> ConsumerGroup {
    let generation_id = generation_id;
    let protocol_type: StrBytes = request.protocol_type.clone();

    // 🔍 protocol_name
    let protocol_name: StrBytes = request.protocols.first()
        .map(|p| p.name.clone())
        .unwrap_or_else(|| "range".into());
    // find metadata in protocols
    let metadata = request
        .protocols
        .iter()
        .find(|p| p.name == protocol_name)
        .map(|p| p.metadata.clone())
        .unwrap_or_default();


    let member = ConsumerGroupMember {
        member_id: member_id,
        is_leader: true,
        is_pending: false,
        last_heartbeat: SystemTime::now(),
        metadata: Some(metadata.clone()),
        assignment: None,
    };
    let members = vec![member];

    let cg = ConsumerGroup{
        group_id: group_id.clone().to_string(),
        members,
        rebalance_in_progress: false,
        generation_id,
        topics: topics,
        protocol_type: protocol_type.to_string(),
        protocol_name: protocol_name.to_string(),
        leader_id: leader_id,
        is_rebalancing: false,
    };
    cg
}