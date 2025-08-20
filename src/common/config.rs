use serde::Deserialize;
use config::{Config, Environment};
use std::fs::File;
use std::io::BufReader;
use anyhow::Result;

#[derive(Debug, Deserialize)]
pub struct BrokerConfig {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Deserialize)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub controller_id: i32,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub advertised_host: String,
    pub advertised_port: i32,
    pub brokers: Vec<BrokerConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]  // "s3", "file", "redis"
pub enum StorageType {
    File,
    S3,
    Redis,
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

    pub index_store_type: StorageType, // "file", "s3", "redis"
    pub index_store_redis_urls: Option<String>, // "redis://redis-1:7001,redis://redis-2:7002"

    pub tcp_send_buffer_bytes: Option<usize>,
    pub tcp_recv_buffer_bytes: Option<usize>,
    pub tcp_nodelay: Option<bool>,
}

pub fn load_server_config() -> Result<ServerConfig> {
    dotenv::dotenv().ok(); // 開発用
    let cfg = Config::builder()
        .add_source(Environment::default()
            .prefix("KAFKA")
            .try_parsing(true)
        )
        .build()?;
    let config: ServerConfig = cfg.try_deserialize()?;
    Ok(config)
}

pub fn load_cluster_config(path: &str) -> Result<ClusterConfig> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let config = serde_json::from_reader(reader)?;
    Ok(config)
}