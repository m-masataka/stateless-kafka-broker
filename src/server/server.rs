use crate::server::loader::{load_index_store, load_log_store, load_meta_store};

use crate::common::config::{load_node_config, load_server_config};
use crate::common::utils::jittered_delay;
use crate::server::transport;
use crate::handler::context::HandlerContext;
use crate::server::cluster_heartbeat::send_heartbeat;
use std::sync::Arc;
use tokio::{
    time::{self, Duration},
};

pub async fn server_start(config_path: &str) -> anyhow::Result<()> {
    env_logger::init();
    log::info!("Starting Kafka-compatible server...");
    let node_conf_load = Arc::new(load_node_config(config_path)?);
    let server_config_load = Arc::new(load_server_config()?);

    let listener = tokio::net::TcpListener::bind(format!(
        "{}:{}",
        server_config_load.host, server_config_load.port
    ))
    .await?;
    log::info!(
        "Kafka-compatible server listening on port {}:{}",
        server_config_load.host,
        server_config_load.port
    );

    let meta_store = Arc::new(load_meta_store(&server_config_load).await.unwrap());
    let log_store = Arc::new(load_log_store(&server_config_load).await.unwrap());
    let index_store = Arc::new(load_index_store(&server_config_load).await.unwrap());

    // Start cluster heartbeat task
    log::info!("Starting cluster heartbeat task...");
    {
        let meta_store = meta_store.clone();
        let node_config = node_conf_load.clone();
        tokio::spawn(async move {
            // Update cluster status immediately on startup
            if let Err(e) = send_heartbeat(&node_config, meta_store.clone()).await {
                log::warn!("heartbeat failed: {:?}", e);
            }
            // Then start periodic updates
            let jitter = jittered_delay(10); // 10 seconds base
            let mut ticker = time::interval(Duration::from_secs(jitter));
            loop {
                ticker.tick().await;
                if let Err(e) = send_heartbeat(&node_config, meta_store.clone()).await {
                    log::warn!("heartbeat failed: {:?}", e);
                }
            }
        });
    }

    let ctx = HandlerContext {
        node_config: node_conf_load,
        meta_store,
        log_store,
        index_store,
    };
    const MAX_REQUEST_BYTES: usize = 10024 * 1024 * 1024;
    transport::serve(listener, ctx, MAX_REQUEST_BYTES).await
}
