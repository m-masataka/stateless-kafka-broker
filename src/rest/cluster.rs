use crate::server::rest_server::AppState;
use crate::traits::meta_store::MetaStore;
use axum::{Json, extract::State};

pub async fn get_nodes(State(st): State<AppState>) -> Json<serde_json::Value> {
    let nodes = &st.meta_store.get_cluster_status().await.unwrap_or_default();
    Json(serde_json::json!({
        "nodes": nodes.iter().map(|node| {
            serde_json::json!({
                "nodeId": node.node_id,
                "host": node.host,
                "port": node.port,
                "advertisedHost": node.advertised_host,
                "advertisedPort": node.advertised_port,
                "heartbeatTime": node.heartbeat_time,
            })
        }).collect::<Vec<_>>(),
    }))
}
