use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Node {
    pub cluster_id: String,
    pub controller_id: i32,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub advertised_host: String,
    pub advertised_port: i32,
    pub heartbeat_time: Option<i64>, // milliseconds since UNIX_EPOCH
}
