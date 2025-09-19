use anyhow::Ok;
use axum::{Json, Router, extract::State, routing::get};
use std::sync::Arc;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::error;

use crate::rest::{
    cluster::get_nodes, consumer_groups::get_consumer_group, topics::get_topics,
    tp_index::get_tp_offsets,
};
use crate::{
    common::config::{ServerConfig, load_server_config},
    server::loader::{
        load_index_store,
        // load_log_store,
        load_meta_store,
    },
    storage::{
        index_store_impl::IndexStoreImpl,
        // log_store_impl::LogStoreImpl,
        meta_store_impl::MetaStoreImpl,
    },
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServerConfig>,
    // pub log_store: Arc<LogStoreImpl>,
    pub meta_store: Arc<MetaStoreImpl>,
    pub index_store: Arc<IndexStoreImpl>,
}

pub async fn rest_server_start() -> anyhow::Result<()> {
    log::info!("Starting Rest Api server...");
    let server_config_load = Arc::new(load_server_config()?);

    let meta_store = Arc::new(load_meta_store(&server_config_load).await.unwrap());
    // let log_store = Arc::new(load_log_store(&server_config_load).await.unwrap());
    let index_store = Arc::new(load_index_store(&server_config_load).await.unwrap());

    let state = AppState {
        config: Arc::clone(&server_config_load),
        // log_store,
        meta_store,
        index_store,
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/consumerGroups", get(get_consumer_group))
        .route("/topics", get(get_topics))
        .route("/offsets", get(get_tp_offsets))
        .route("/nodes", get(get_nodes))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.map_err(|e| {
        error!("Failed to start server: {}", e);
        anyhow::anyhow!("Server error: {}", e)
    })?;

    Ok(())
}

async fn health(State(st): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "version": "1.0.0",
        "config": {
            "tcp_nodelay": st.config.tcp_nodelay,
        }
    }))
}
