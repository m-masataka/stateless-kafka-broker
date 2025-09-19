use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Topic {
    pub name: Option<String>,
    pub topic_id: Uuid,
    pub is_internal: bool,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub partitions: Option<Vec<Partition>>,
    pub topic_authorized_operations: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Partition {
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}
