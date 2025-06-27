
use tokio::io::AsyncWrite;
use anyhow::Result;
use std::time::SystemTime;
use kafka_protocol::messages::join_group_request::JoinGroupRequest;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::messages::join_group_response::{
    JoinGroupResponse,
    JoinGroupResponseMember
};

use crate::common::consumer::{ConsumerGroup, ConsumerGroupMember};
use crate::common::response::send_kafka_response;
use crate::storage::meta_store_impl::MetaStoreImpl;
use crate::traits::meta_store::MetaStore;
use uuid::Uuid;
use std::collections::BTreeMap;


pub async fn handle_join_group_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &JoinGroupRequest,
    meta_store: &MetaStoreImpl,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling JoinGroupRequest API VERSION {}", header.request_api_version);
    log::debug!("JoinGroupRequest: {:?}", request);

    // グループIDとメンバーIDを取得
    let group_id = request.group_id.clone();
    // Heartbeatの更新とgroupの取得
    let mut consumer_group = meta_store.update_heartbeat(group_id.as_str()).await?;
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
            let generation_id = store_group.generation_id + 1; // 初期のgeneration_id
            let member_id_str = generate_member_id(&group_id);
            let requester_id = member_id_str.clone();
            let leader_id = member_id_str.clone();
            let cg = new_consumer_group(request, member_id_str, leader_id, &group_id, generation_id);
            meta_store.save_consumer_group(&cg).await?;
        
            convert_consumer_group_to_join_response(
                &cg,
                requester_id,
                0,
            )
        } else {
            // 既存のグループが見つかった場合は、メンバー情報を更新
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
            convert_consumer_group_to_join_response(store_group, requester_id, 0)
        }
    } else {
        // グループが存在しない場合は新規作成
        log::info!("Consumer group not found, creating new group: {}", *group_id.clone());
        let member_id_str = generate_member_id(&group_id);
        let requester_id = member_id_str.clone();
        let leader_id = member_id_str.clone();
        let cg = new_consumer_group(request, member_id_str, leader_id, &group_id, 1);
        meta_store.save_consumer_group(&cg).await?;
        
        convert_consumer_group_to_join_response(
            &cg,
            requester_id,
            0,
        )
    };

    log::debug!("JoinGroupResponse: {:?}", response);

    // レスポンスをエンコードして送信
    send_kafka_response(stream, header, &response).await?;
    log::debug!("Sent JoinGroupResponse");
    Ok(())
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
    // メンバー一覧を JoinGroupResponseMember に変換
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

    // JoinGroupResponse を default から構築
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

fn new_consumer_group(request: &JoinGroupRequest, member_id: String, leader_id: String, group_id: &StrBytes, generation_id: i32) -> ConsumerGroup {
    let generation_id = generation_id;
    let protocol_type: StrBytes = request.protocol_type.clone();

    // 🔍 protocol_name
    let protocol_name: StrBytes = request.protocols.first()
        .map(|p| p.name.clone())
        .unwrap_or_else(|| "range".into());
    // 🔍 protocol_name に対応する metadata を探す
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
        topics: None,
        protocol_type: protocol_type.to_string(),
        protocol_name: protocol_name.to_string(),
        leader_id: leader_id,
        is_rebalancing: false,
    };
    cg
}