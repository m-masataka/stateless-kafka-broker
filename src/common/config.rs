use serde::Deserialize;

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
    pub brokers: Vec<BrokerConfig>,
}

use std::fs::File;
use std::io::BufReader;
use anyhow::Result;

pub fn load_cluster_config(path: &str) -> Result<ClusterConfig> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let config = serde_json::from_reader(reader)?; // serde_yaml::from_reader(...) もOK
    Ok(config)
}