use serde::{Serialize, Deserialize};
use std::time::SystemTime;
use bytes::Bytes;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConsumerGroupMember {
    pub member_id: String,
    pub is_leader: bool,
    pub is_pending: bool,
    pub last_heartbeat: SystemTime,
    pub metadata: Option<Bytes>,
    pub assignment: Option<Bytes>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Topics {
    pub name: String,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Partition {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_leader_epoch: i32,
    pub metadata: Option<Bytes>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub members: Vec<ConsumerGroupMember>,
    pub rebalance_in_progress: bool,
    pub leader_id: String,
    pub generation_id: i32,
    pub topics: Option<Vec<Topics>>,
    pub protocol_type: String,
    pub protocol_name: String,
    pub is_rebalancing: bool,
}

impl ConsumerGroup {
    pub fn get_partition_by_topic_and_index(
        &self,
        topic_name: &str,
        partition_index: i32,
    ) -> Option<&Partition> {
        self.topics
            .as_ref()? // Option<Vec<Topics>> を Option<&Vec<Topics>> に
            .iter()
            .find(|t| t.name == topic_name)
            .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition_index))
    }

    pub fn get_member_by_id(&self, member_id: &str) -> Option<&ConsumerGroupMember> {
        self.members.iter().find(|m| m.member_id == member_id)
    }
}