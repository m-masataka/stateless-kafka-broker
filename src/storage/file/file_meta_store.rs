use crate::traits::meta_store::UnsendMetaStore;
use crate::common::{
    topic_partition::Topic,
    consumer::ConsumerGroup,
    cluster::Node,
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
    async fn put_topic(&self, data: &Topic) -> Result<()> {
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

    async fn get_topic(&self, topic_id: &str) -> Result<Topic> {
        let file = match File::open(&self.meta_store_path) {
            Ok(f) => f,
            Err(e) => return Err(e.into()),
        };
        let reader = BufReader::new(file);
        let map: HashMap<String, Topic> = serde_json::from_reader(reader)?;

        let result = map.values().find(|topic| topic.topic_id.to_string() == topic_id);
        Ok(result.cloned().ok_or_else(|| anyhow::anyhow!("Topic not found"))?)
    }
    
    async fn get_topics(&self) -> Result<Vec<Topic>> {
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

    async fn update_consumer_group<F,Fut>(&self,group_id: &str,update_fn:F,) -> Result<Option<ConsumerGroup> >where F:FnOnce(ConsumerGroup) -> Fut+Send+'static,Fut:std::future::Future<Output = Result<ConsumerGroup> > +Send+'static {
        let filename = format!("{group_id}.json");
        let path = &self.consumer_group_dir_path.join(filename);

        log::debug!("Updating consumer group with closure: {}", group_id);
        let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;
        
        file.lock_exclusive()?;
        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&mut file);
    
        let consumer_group: ConsumerGroup = match serde_json::from_reader::<_, ConsumerGroup>(reader) {
            Ok(g) => g,
            Err(e) if e.is_eof() => {
                log::warn!("File is empty: {:?}", path);
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        let updated_group = update_fn(consumer_group).await?;

        log::debug!("Saving updated consumer group data to file: {:?}", path);
        let json = serde_json::to_string_pretty(&updated_group)?;
        file.set_len(0)?;
        file.seek(std::io::SeekFrom::Start(0))?; 
        file.write_all(json.as_bytes())?;
        Ok(Some(updated_group))
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

    async fn update_cluster_status(&self, node_config: &Node) -> Result<()> {
        Ok(())
    }

    async fn get_cluster_status(&self) -> Result<Vec<Node> > {
        Ok(vec![])
    }
}
