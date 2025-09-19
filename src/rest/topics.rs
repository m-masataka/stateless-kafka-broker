use axum::{
    extract::State,
    Json,
};
use crate::traits::meta_store::MetaStore;
use crate::{server::rest_server::AppState};

pub async fn get_topics(State(st): State<AppState>) -> Json<serde_json::Value>{
    let topics = &st.meta_store.get_topics().await.unwrap_or_default();
    Json(serde_json::json!({
        "topics": topics.iter().map(|topic| {
            serde_json::json!({
                "name": topic.name,
                "partitions": topic.partitions.iter().flatten().map(|partition| {
                    serde_json::json!({
                        "partitionIndex": partition.partition_index,
                        "leaderId": partition.leader_id,
                        "leaderEpoch": partition.leader_epoch,
                        "replicaNodes": partition.replica_nodes,
                        "isrNodes": partition.isr_nodes,
                        "offlineReplicas": partition.offline_replicas,
                    })
                }).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>()
    }))
}
