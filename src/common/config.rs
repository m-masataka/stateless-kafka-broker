use crate::common::cluster::Node;
use anyhow::Result;
use config::{Config, Environment};
use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")] // "s3", "file", "redis", "tikv"
pub enum StorageType {
    File,
    S3,
    Redis,
    Tikv,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: i32,

    pub log_store_type: StorageType,
    // Log Store configuration For S3
    pub log_store_s3_bucket: Option<String>,
    pub log_store_s3_prefix: Option<String>,
    pub log_store_s3_endpoint: Option<String>,
    pub log_store_s3_access_key: Option<String>,
    pub log_store_s3_secret_key: Option<String>,
    pub log_store_s3_region: Option<String>,

    pub meta_store_type: StorageType, // "file", "s3", "redis"
    // Log Store configuration For Redis
    // Log Store configuration For S3
    pub meta_store_s3_bucket: Option<String>,
    pub meta_store_s3_prefix: Option<String>,
    pub meta_store_s3_endpoint: Option<String>,
    pub meta_store_s3_access_key: Option<String>,
    pub meta_store_s3_secret_key: Option<String>,
    pub meta_store_s3_region: Option<String>,
    pub meta_store_redis_urls: Option<String>, // "redis://redis-1:7001,redis://redis-2:7002"
    pub meta_store_redis_pool_size: Option<usize>,
    pub meta_store_tikv_endpoints: Option<String>, // "tikv-1:2379,tikv-2:2379"

    pub index_store_type: StorageType, // "file", "s3", "redis"
    pub index_store_redis_urls: Option<String>, // "redis://redis-1:7001,redis://redis-2:7002"
    pub index_store_redis_pool_size: Option<usize>,
    pub index_store_tikv_endpoints: Option<String>, // "tikv-1:2379,tikv-2:2379"

    pub tcp_send_buffer_bytes: Option<usize>,
    pub tcp_recv_buffer_bytes: Option<usize>,
    pub tcp_nodelay: Option<bool>,
}

pub fn load_server_config() -> Result<ServerConfig> {
    dotenv::dotenv().ok(); // Load .env file if exists
    let cfg = Config::builder()
        .add_source(Environment::default().prefix("KAFKA").try_parsing(true))
        .build()?;
    let config: ServerConfig = cfg.try_deserialize()?;
    Ok(config)
}

pub fn load_node_config(path: &str) -> Result<Node> {
    log::info!("Loading node config from: {}", path);
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    log::debug!("Reading node config file...");
    let node = serde_json::from_reader(reader).map_err(|e| {
        log::error!("Failed to parse node config JSON: {:?}", e);
        e
    })?;
    log::debug!("Node config loaded successfully");
    Ok(node)
}
