use axum::{
    extract::State,
    Json,
};
use crate::traits::meta_store::MetaStore;
use crate::{server::rest_server::AppState};

pub async fn get_consumer_group(State(st): State<AppState>) -> Json<serde_json::Value>{
    let consumer_groups = &st.meta_store.get_consumer_groups().await.unwrap_or_default();
    log::debug!("Retrieved consumer groups: {:?}", consumer_groups);

    Json(serde_json::json!({
        "consumerGroups": consumer_groups.iter().map(|cg| {
            serde_json::json!({
                "groupId": cg.group_id,
                "leaderId": cg.leader_id,
                "protocolType": cg.protocol_type,
                "generationId": cg.generation_id,
                "topics": cg.topics,
                "members": cg.members.iter().map(|m| {
                    serde_json::json!({
                        "memberId": m.member_id,
                        "isLeader": m.is_leader,
                        "last_heartbeat": m.last_heartbeat,
                        "isPending": m.is_pending,
                        "assignment": m.assignment,
                    })
                }).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>()
    }))
}