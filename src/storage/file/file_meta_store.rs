use crate::common::consumer::{
    ConsumerGroupPartition,
    ConsumerGroupTopic,
};
use crate::traits::meta_store::UnsendMetaStore;
use crate::common::{
    topic_partition::Topic,
    consumer::{ConsumerGroup, ConsumerGroupMember},
};
use anyhow::Result;
use std::io::{ 
    BufReader,
    ErrorKind::NotFound,
    Read,
    Write,
    Seek,
    SeekFrom,
};
use std::fs::{File, create_dir_all, OpenOptions};
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use fs2::FileExt;

pub struct FileMetaStore {
    meta_store_path: PathBuf,
    consumer_group_dir_path: PathBuf,
    producer_id_path: PathBuf,
}

impl FileMetaStore {
    pub fn new() -> Self {
        Self {
            meta_store_path: Path::new("./data/metadata.json").to_owned(),
            consumer_group_dir_path: Path::new("./data/consumer_group").to_owned(),
            producer_id_path: Path::new("./data/producer_id.json").to_owned(),
        }
    }
}

impl UnsendMetaStore for FileMetaStore {
    async fn save_topic_partition(&self, data: &Topic) -> Result<()> {
        let mut metadata_map: HashMap<String, Topic> = match File::open(&self.meta_store_path) {
            Ok(mut file) => {
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                if contents.trim().is_empty() {
                    HashMap::new()
                } else {
                    serde_json::from_str(&contents)?
                }
            },
            Err(e) if e.kind() == NotFound => {
                // If the file does not exist, create a new one
                HashMap::new()
            },
            Err(e) => return Err(e.into()), // Other errors are returned as is
        };
    
        metadata_map.insert(data.topic_id.to_string(), data.clone());
    
        let mut file = File::create(&self.meta_store_path)?;
        let json = serde_json::to_string_pretty(&metadata_map)?;
        file.write_all(json.as_bytes())?;
    
        Ok(())
    }

    async fn get_topic(&self, name: Option<&str>, topic_id: Option<&str>) -> Result<Option<Topic>> {
        let file = match File::open(&self.meta_store_path) {
            Ok(f) => f,
            Err(e) if e.kind() == NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let reader = BufReader::new(file);
        let map: HashMap<String, Topic> = serde_json::from_reader(reader)?;
    
        let result = map.values().find(|topic| {
            name.is_some_and(|n| topic.name.as_deref() == Some(n)) ||
            topic_id.is_some_and(|id| topic.topic_id.to_string() == id)
        });
    
        Ok(result.cloned())
    }
    
    async fn get_all_topics(&self) -> Result<Vec<Topic>> {
        let file = match File::open(&self.meta_store_path) {
            Ok(f) => f,
            Err(e) if e.kind() == NotFound => return Ok(vec![]),
            Err(e) => return Err(e.into()),
        };
    
        let reader = BufReader::new(file);
        let map: HashMap<String, Topic> = serde_json::from_reader(reader)?;
    
        Ok(map.values().cloned().collect())
    }

    async fn get_topic_id_by_topic_name(&self, topic_name: &str) -> Result<Option<String>> {
        let file = match File::open(&self.meta_store_path) {
            Ok(f) => f,
            Err(e) if e.kind() == NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
    
        let reader = BufReader::new(file);
        let map: HashMap<String, Topic> = serde_json::from_reader(reader)?;
    
        for (id, topic) in map.iter() {
            if topic.name.as_deref() == Some(topic_name) {
                return Ok(Some(id.clone()));
            }
        }
    
        Ok(None)
    }

    async fn delete_topic_by_name(&self, name: &str) -> Result<()> {
        let mut metadata_map: HashMap<String, Topic> = match File::open(&self.meta_store_path) {
            Ok(mut file) => {
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                if contents.trim().is_empty() {
                    HashMap::new()
                } else {
                    serde_json::from_str(&contents)?
                }
            },
            Err(e) if e.kind() == NotFound => {
                HashMap::new()
            },
            Err(e) => return Err(e.into()),
        };
    
        metadata_map.retain(|_, topic| topic.name.as_deref() != Some(name));
    
        let mut file = File::create(&self.meta_store_path)?;
        let json = serde_json::to_string_pretty(&metadata_map)?;
        file.write_all(json.as_bytes())?;
    
        Ok(())
    }

    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()> {
        let mut metadata_map: HashMap<String, Topic> = match File::open(&self.meta_store_path) {
            Ok(mut file) => {
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                if contents.trim().is_empty() {
                    HashMap::new()
                } else {
                    serde_json::from_str(&contents)?
                }
            },
            Err(e) if e.kind() == NotFound => {
                HashMap::new()
            },
            Err(e) => return Err(e.into()),
        };
    
        metadata_map.retain(|_, topic| topic.topic_id != topic_id);
    
        let mut file = File::create(&self.meta_store_path)?;
        let json = serde_json::to_string_pretty(&metadata_map)?;
        file.write_all(json.as_bytes())?;
    
        Ok(())
    }

    async fn save_consumer_group(&self, data: &ConsumerGroup) -> Result<()> {
        let filename = format!("{0}.json", data.group_id);
        let path = &self.consumer_group_dir_path.join(filename);
    
        log::debug!("Upserting consumer group data to file: {:?}", path);
    
        create_dir_all(&self.consumer_group_dir_path)?;

        let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;

        log::debug!("Saving updated consumer group data to file: {:?}", path);
        let json = serde_json::to_string_pretty(&data)?;
        file.set_len(0)?;
        file.seek(std::io::SeekFrom::Start(0))?; 
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    async fn get_consumer_groups(&self) -> Result<Vec<ConsumerGroup> > {
        let mut consumer_groups = Vec::new();
        if !self.consumer_group_dir_path.exists() {
            return Ok(consumer_groups);
        }

        for entry in std::fs::read_dir(&self.consumer_group_dir_path)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                let file = File::open(entry.path())?;
                let reader = BufReader::new(file);
                let consumer_group: ConsumerGroup = serde_json::from_reader(reader)?;
                consumer_groups.push(consumer_group);
            }
        }
        Ok(consumer_groups)
    }

    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        let filename = format!("{group_id}.json");
        let path = &self.consumer_group_dir_path.join(filename);
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let reader = BufReader::new(file);
        match serde_json::from_reader::<_, ConsumerGroup>(reader) {
            Ok(cg) => {
                log::debug!("Consumer group found: {}", group_id);
                Ok(Some(cg))
            }
            Err(e) if e.is_eof() => {
                log::warn!("Consumer group file is empty: {:?}", path);
                Ok(None)
            }
            Err(e) => {
                log::error!("Failed to read consumer group data: {}", e);
                Err(e.into())
            }

        }
    }

    async fn update_heartbeat(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        let filename = format!("{group_id}.json");
        let path = &self.consumer_group_dir_path.join(filename);

        log::debug!("Checking heartbeat for consumer group: {}", group_id);
        let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;
        
        file.lock_exclusive()?;
        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&mut file);
    
        let checked_group: ConsumerGroup = match serde_json::from_reader::<_, ConsumerGroup>(reader) {
            Ok(g) => {
                 let mut g = g.clone();
                g.members.retain(|member| {
                    if let Ok(duration) = std::time::SystemTime::now().duration_since(member.last_heartbeat) {
                        duration.as_secs() < 10 // TODO: Set heartbeat timeout duration
                    } else {
                        log::error!("Failed to calculate duration since last heartbeat for member {}", member.member_id);
                        false
                    }
                });
                g.is_rebalancing = match g.members.iter().find(|m| m.is_leader) {
                    Some(_) => false,
                    None => true,
                };
                g
            },
            Err(e) if e.is_eof() => {
                log::warn!("File is empty: {:?}", path);
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        log::debug!("Saving updated consumer group data to file: {:?}", path);
        let json = serde_json::to_string_pretty(&checked_group)?;
        file.set_len(0)?;
        file.seek(std::io::SeekFrom::Start(0))?; 
        file.write_all(json.as_bytes())?;
        Ok(Some(checked_group.clone()))
    }

    async fn offset_commit(&self, group_id: &str, topic_name: &str, partition_index: i32, offset: i64) -> Result<()> {
        let filename = format!("{group_id}.json");
        let path = &self.consumer_group_dir_path.join(filename);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        
        file.lock_exclusive()?;
        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&mut file);
        let mut consumer_group: ConsumerGroup = match serde_json::from_reader::<_, ConsumerGroup>(reader) {
            Ok(g) => g,
            Err(e) if e.is_eof() => {
                log::warn!("File is empty: {:?}", path);
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        if consumer_group.group_id != group_id {
            log::error!("Consumer group ID mismatch: expected {}, found {}", group_id, consumer_group.group_id);
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Consumer group ID mismatch").into());
        }
        if consumer_group.is_rebalancing {
            log::warn!("Consumer group {} is currently rebalancing, cannot commit offset", group_id);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Consumer group is rebalancing").into());
        }

        // update offset
        log::debug!("Committing offset for group: {}, topic: {}, partition: {}, offset: {}", group_id, topic_name, partition_index, offset);
        let topics = consumer_group.topics.get_or_insert_with(Vec::new);
        let topic = topics.iter_mut().find(|t| t.name == topic_name);
        let generation_id = consumer_group.generation_id;
        match topic {
            Some(topic) => {
                match topic.partitions.iter_mut().find(|p| p.partition_index == partition_index) {
                    Some(partition) => {
                        partition.committed_offset = offset;
                        partition.committed_leader_epoch = generation_id;
                        log::debug!(
                            "Updated committed_offset for partition {} of topic '{}': {}",
                            partition_index, topic_name, offset
                        );
                    }
                    None => {
                        topic.partitions.push(ConsumerGroupPartition {
                            partition_index: partition_index,
                            committed_offset: offset,
                            committed_leader_epoch: generation_id,
                            metadata: None,
                        });
                        log::debug!(
                            "Created new partition {} in topic '{}', committed_offset: {}",
                            partition_index, topic_name, offset
                        );
                    }
                }
            }
            None => {
                topics.push(ConsumerGroupTopic {
                    name: topic_name.to_string(),
                    partitions: vec![ConsumerGroupPartition {
                        partition_index: partition_index,
                        committed_offset: offset,
                        committed_leader_epoch: generation_id,
                        metadata: None,
                    }],
                });
                log::debug!(
                    "Created new topic '{}' with partition {}, committed_offset: {}",
                    topic_name, partition_index, offset
                );
            }
        }


        log::debug!("Saving updated consumer group data to file: {:?}", path);
        let json = serde_json::to_string_pretty(&consumer_group)?;
        file.set_len(0)?;
        file.seek(std::io::SeekFrom::Start(0))?; 
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        let filename = format!("{group_id}.json");
        let path = &self.consumer_group_dir_path.join(filename);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        
        file.lock_exclusive()?;
        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&mut file);
        let mut consumer_group: ConsumerGroup = match serde_json::from_reader::<_, ConsumerGroup>(reader) {
            Ok(g) => g,
            Err(e) if e.is_eof() => {
                log::warn!("File is empty: {:?}", path);
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        
        consumer_group.members.retain(|member| member.member_id != member_id);
        
        log::debug!("Saving updated consumer group data to file: {:?}", path);
        let json = serde_json::to_string_pretty(&consumer_group)?;
        file.set_len(0)?;
        file.seek(std::io::SeekFrom::Start(0))?; 
        file.write_all(json.as_bytes())?;
        
        Ok(())
    }

    async fn update_heartbeat_by_member_id(&self, group_id: &str, member_id: &str) -> Result<Option<ConsumerGroup>> {
        let filename = format!("{group_id}.json");
        let path = &self.consumer_group_dir_path.join(filename);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        
        file.lock_exclusive()?;
        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&mut file);
        let mut consumer_group: ConsumerGroup = match serde_json::from_reader::<_, ConsumerGroup>(reader) {
            Ok(g) => g,
            Err(e) if e.is_eof() => {
                log::warn!("File is empty: {:?}", path);
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        if let Some(member) = consumer_group.members.iter_mut().find(|m| m.member_id == member_id) {
            member.last_heartbeat = std::time::SystemTime::now();
            log::debug!("Updated heartbeat for member: {}", member_id);
        } else {
            log::warn!("Member {} not found in group {}", member_id, group_id);
        }

        log::debug!("Saving updated consumer group data to file: {:?}", path);
        let json = serde_json::to_string_pretty(&consumer_group)?;
        file.set_len(0)?;
        file.seek(std::io::SeekFrom::Start(0))?; 
        file.write_all(json.as_bytes())?;
        
        Ok(Some(consumer_group))
    }

    async fn update_consumer_group_member(&self, group_id: &str, member: &ConsumerGroupMember) -> Result<()> {
        let filename = format!("{group_id}.json");
        let path = &self.consumer_group_dir_path.join(filename);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        
        file.lock_exclusive()?;
        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&mut file);
        let mut consumer_group: ConsumerGroup = match serde_json::from_reader::<_, ConsumerGroup>(reader) {
            Ok(g) => g,
            Err(e) if e.is_eof() => {
                log::warn!("File is empty: {:?}", path);
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        if let Some(existing_member) = consumer_group
            .members.iter_mut().find(|m| m.member_id == member.member_id) {
                existing_member.metadata = member.metadata.clone();
                existing_member.last_heartbeat = std::time::SystemTime::now();
                existing_member.is_leader = member.is_leader;
                existing_member.assignment = member.assignment.clone();
            log::debug!("Updated existing member: {}", member.member_id);
        } else {
            consumer_group.members.push(member.clone());
            log::debug!("Added new member: {}", member.member_id);
        }

        log::debug!("Saving updated consumer group data to file: {:?}", path);
        let json = serde_json::to_string_pretty(&consumer_group)?;
        file.set_len(0)?;
        file.seek(std::io::SeekFrom::Start(0))?; 
        file.write_all(json.as_bytes())?;

        Ok(()) 
    }

    async fn gen_producer_id(&self) -> Result<i64> {
        let path = &self.producer_id_path;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        
        file.lock_exclusive()?;
        file.seek(SeekFrom::Start(0))?;

        use serde::{Deserialize, Serialize};
        #[derive(Debug, Deserialize, Serialize)]
        pub struct ProducerId {
            pub producer_id: i64,
        }

        let mut producer_id: ProducerId = match serde_json::from_reader(&file) {
            Ok(id) => id,
            Err(e) if e.is_eof() => ProducerId { producer_id: 0 },
            Err(_e) => ProducerId { producer_id: 0 },
        };
        producer_id.producer_id += 1;
        log::debug!("Generated new producer ID: {}", producer_id.producer_id);
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        let json = serde_json::to_string_pretty(&producer_id)?;
        file.write_all(json.as_bytes())?;
        Ok(producer_id.producer_id)
    }
}
