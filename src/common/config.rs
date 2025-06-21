use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    File,
    S3,
    Redis,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageBackendConfig {
    File, // 設定不要
    S3 {
        bucket: String,
        prefix: Option<String>,
    },
    Redis {
        url: String,
        db: Option<i32>,
    },
}

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
    pub log_store: StorageBackendConfig,
    pub meta_store: StorageBackendConfig,
    pub brokers: Vec<BrokerConfig>,
}

use std::fs::File;
use std::io::BufReader;
use anyhow::Result;

pub fn load_cluster_config(path: &str) -> Result<ClusterConfig> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let config = serde_json::from_reader(reader)?;
    Ok(config)
}