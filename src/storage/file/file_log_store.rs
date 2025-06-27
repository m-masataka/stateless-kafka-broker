use crate::traits::log_store::UnsendLogStore;
use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{Error, ErrorKind::InvalidData, Read, Result, Write},
    path::{Path, PathBuf},
};
use bytes::Bytes;
use kafka_protocol::records::{
    RecordBatchEncoder,
};
use fs2::FileExt;
use crate::common::record::{bytes_to_output, record_bytes_to_kafka_response_bytes};

pub struct FileLogStore {
    log_store_dir: PathBuf,
    log_file_name_prefix: String,
}

impl FileLogStore {
    pub fn new() -> Self {
        Self {
            log_store_dir: Path::new("./data").to_owned(),
            log_file_name_prefix: "log-file-".to_string(),
        }
    }
}

impl UnsendLogStore for FileLogStore {
    async fn write_records(&self, topic_id: &str, partition: i32, start_offset: i64,  records: Option<&Bytes>) -> anyhow::Result<(i64, String)> {
        // Check if the log store directory exists, if not create it
        if let Some(data) = records {
            let object_key = self.log_key(topic_id, partition, start_offset);
            
            let (output, current_offset) = bytes_to_output(data, start_offset)?;
            
            self.put_object(&object_key, &output)
                .map_err(|e| {
                    log::error!("Failed to put object to Dir: {:?}", e);
                    e
                })?;
            
            log::debug!("Successfully wrote batch to file for topic: {}, partition: {}", topic_id, partition);
            let object_key_str = object_key.to_str().unwrap_or("Invalid UTF-8 path").to_string();
            Ok((current_offset, object_key_str))
        } else {
            Err(anyhow::anyhow!("No records to write"))
        }
    }

    async fn read_records(&self, keys: Vec<String>) -> anyhow::Result<Bytes> {
        // Read records from file, and return Bytes encoded data.
        if keys.is_empty() {
            return Err(anyhow::anyhow!("No keys provided for reading records"));
        }
        let mut response = Vec::new();
        for key in keys {
            let key_path = PathBuf::from(&key);
            let data = match self.get_object(key_path.as_path()) {
                Ok(data) => data,
                Err(e) => {
                    // If the object does not exist, we initialize it with an empty log
                    log::warn!("Log object not found. Initializing with empty log. Error: {:?}", e);
                    let init_log = Vec::new();
                    Bytes::from(init_log)
                }
            };
            let records = record_bytes_to_kafka_response_bytes(&data).map_err(|e| {
                log::error!("Failed to read record from S3: {}", e);
                e
            })?;
            response.extend_from_slice(&records);
        }
        // Encode the records back to Bytes
        let encode_options = kafka_protocol::records::RecordEncodeOptions{
            version: 2,
            compression: kafka_protocol::records::Compression::None,
        };
        let mut data = Vec::new();
        if let Err(e) = RecordBatchEncoder::encode(&mut data, &response, &encode_options) {
            return Err(anyhow::anyhow!("Failed to encode record batch: {}", e));
        }
        Ok(Bytes::from(data))
    }
}

impl FileLogStore {
    fn log_key(&self, topic_id: &str, partition: i32, offset: i64) -> PathBuf {
        self.log_store_dir.join(format!("{}/{}/{}{}.log", topic_id, partition, self.log_file_name_prefix, offset))
    }

    fn put_object(&self, path: &Path, data: &[u8]) -> Result<()> {
        if !path.exists() {
            let parent_dir =  path.parent().ok_or_else(|| {
                Error::new(InvalidData, "Path has no parent directory")
            })?;
            let _ = create_dir_all(parent_dir);
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        file.lock_exclusive()?;
        file.write_all(data)?;
        file.flush()?;
        FileExt::unlock(&file)?;
        Ok(())
    }

    fn get_object(&self, path: &Path) -> Result<Bytes> {
        if !path.exists() {
            return Err(Error::new(InvalidData, "File does not exist"));
        }
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let bytes = Bytes::from(buffer);
        Ok(bytes)
    }
}