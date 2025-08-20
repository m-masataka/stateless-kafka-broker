use std::sync::Arc;
use anyhow::Ok;
use axum::{
    extract::State,
    routing::get,
    Json, Router,
};
use tower_http::{cors::{Any, CorsLayer}, trace::TraceLayer};
use tracing::error;

use crate::{common::config::{load_server_config, ServerConfig}};
use crate::rest::consumer_groups::get_consumer_group;
use crate::rest::topics::get_topics;
use crate::rest::tp_index::get_tp_offsets;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServerConfig>,
}

pub async fn rest_server_start() -> anyhow::Result<()> {
    log::info!("Starting Kafka-compatible server...");
    let server_config_load = Arc::new(load_server_config()?);
    

    let state = AppState {
        config: Arc::clone(&server_config_load),
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/consumerGroups", get(get_consumer_group))
        .route("/topics", get(get_topics))
        .route("/offsets", get(get_tp_offsets))
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .unwrap();
    axum::serve(listener, app).await.map_err(|e| {
        error!("Failed to start server: {}", e);
        anyhow::anyhow!("Server error: {}", e)
    })?;

    Ok(())
}

async fn health(State(st): State<AppState>) -> Json<serde_json::Value>{
    Json(serde_json::json!({
        "status": "ok",
        "version": "1.0.0",
        "config": {
            "tcp_nodelay": st.config.tcp_nodelay,
        }
    }))
}