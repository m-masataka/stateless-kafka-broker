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
pub struct ConsumerGroupTopic {
    pub name: String,
    pub partitions: Vec<ConsumerGroupPartition>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConsumerGroupPartition {
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
    pub topics: Option<Vec<ConsumerGroupTopic>>,
    pub protocol_type: String,
    pub protocol_name: String,
    pub is_rebalancing: bool,
}

impl ConsumerGroup {
    pub fn get_partition_by_topic_and_index(
        &self,
        topic_name: &str,
        partition_index: i32,
    ) -> Option<&ConsumerGroupPartition> {
        if self.topics.is_none() {
            log::warn!("❗ topics is None");
            return None;
        } else {
            if let Some(topics) = &self.topics {
                for topic in topics {
                    log::debug!("Topic: '{}'", topic.name);
                    for part in &topic.partitions {
                        log::debug!("  Partition: {}, offset: {}", part.partition_index, part.committed_offset);
                    }
                }
            }
            self.topics
                .as_ref()? 
                .iter()
                .find(|t| t.name == topic_name)
                .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition_index))
        }
    }

    pub fn get_member_by_id(&self, member_id: &str) -> Option<&ConsumerGroupMember> {
        self.members.iter().find(|m| m.member_id == member_id)
    }

    pub fn update_offset(&mut self, topic_name: &str, partition_index: i32, offset: i64) {
        log::debug!(
            "Updating offset for topic: {}, partition: {}, offset: {}",
            topic_name, partition_index, offset
        );
        log::debug!("Current group state: {:?}", self);
    
        // Ensure `self.topics` is initialized
        let topics = self.topics.get_or_insert(Vec::new());
    
        // Try to find the topic
        let topic_entry = topics.iter_mut().find(|t| t.name == topic_name);
    
        if let Some(topic) = topic_entry {
            // Topic found, try to find partition
            if let Some(partition) = topic.partitions.iter_mut().find(|p| p.partition_index == partition_index) {
                partition.committed_offset = offset;
            } else {
                // Partition not found, insert new
                topic.partitions.push(ConsumerGroupPartition {
                    partition_index,
                    committed_offset: offset,
                    committed_leader_epoch: 0, // Default value, adjust as needed
                    metadata: None, // Optional, adjust as needed
                });
            }
        } else {
            // Topic not found, insert new topic with the partition
            topics.push(ConsumerGroupTopic {
                name: topic_name.to_string(),
                partitions: vec![ConsumerGroupPartition {
                    partition_index,
                    committed_offset: offset,
                    committed_leader_epoch: 0, // Default value, adjust as needed
                    metadata: None, // Optional, adjust as needed
                }],

            });
        }
    }

    pub fn upsert_member(&mut self, member: ConsumerGroupMember) {
        if let Some(existing) = self.members.iter_mut().find(|m| m.member_id == member.member_id) {
            *existing = member;
        } else {
            self.members.push(member);
        }
    }

    pub fn update_group_status(&mut self, heartbeat_timeout: u64) {
        // remove members that are pending or have not sent a heartbeat in a while
        self.members.retain(|m| {
            if m.is_pending {
                return false;
            }
            if let Ok(duration) = SystemTime::now().duration_since(m.last_heartbeat) {
                if duration.as_secs() >= heartbeat_timeout {
                    return false; // remove member
                } else {
                    return true;
                }
            }
            false
        });
        // If there is no leader, set the first member as the leader
        if self.members.is_empty() {
            log::debug!("No members left in the group, resetting group status");
            self.leader_id.clear();
            self.is_rebalancing = true;
        }
        if !self.members.iter().any(|m| m.is_leader) {
            log::debug!("No leader found, setting the first member as the leader");
            self.is_rebalancing = true;
            self.leader_id.clear(); // Clear leader ID if the leader was removed
        }
    }
}
