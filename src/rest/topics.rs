use axum::{
    extract::State,
    Json,
};
use crate::traits::meta_store::MetaStore;
use crate::{server::rest_server::AppState};
use crate::server::loader::load_meta_store;

pub async fn get_topics(State(st): State<AppState>) -> Json<serde_json::Value>{
    let meta_store = load_meta_store(&st.config).await.unwrap();
    let topics = meta_store.get_all_topics().await.unwrap_or_default();
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
