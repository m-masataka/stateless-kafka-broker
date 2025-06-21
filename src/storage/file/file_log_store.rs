use crate::{common::record::convert_kafka_record_to_record_entry, traits::log_store::LogStore};
use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{self, BufRead, BufReader, Error, ErrorKind::InvalidData, Result, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use bytes::Bytes;
use kafka_protocol::records::{
    RecordBatchDecoder,
    RecordBatchEncoder,
    RecordSet,
};
use fs2::FileExt;
use chrono::Local;
use std::fs;
use crate::common::record::RecordEntry;
use crate::common::record::Offset;

pub struct FileLogStore {
    log_store_dir: PathBuf,
}

impl FileLogStore {
    pub fn new() -> Self {
        Self {
            log_store_dir: Path::new("./data").to_owned(),
        }
    }
}

impl LogStore for FileLogStore {
    async fn write_batch(&self, topic: &str, partition: i32, records: Option<&Bytes>) -> anyhow::Result<i64> {
        if let Some(data) = records {
            let mut cursor = std::io::Cursor::new(data);
            let batch: RecordSet = RecordBatchDecoder::decode(&mut cursor)?;

            let log_partition_dir = &self.log_store_dir.join(format!("{}/{}", topic, partition));
            if !log_partition_dir.exists() {
                create_dir_all(log_partition_dir)?;
            }
            let log_file_path = log_partition_dir.join("00000000000000000000.log");
            let offset_file_path = log_partition_dir.join("offset.txt");
            // offset file open and lock
            let offset_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(offset_file_path)?;
            offset_file.lock_exclusive()?;
            let base_offset = read_offset_from_file(&offset_file)?;

            // log file open and lock
            let mut log_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_file_path)?;
            log_file.lock_exclusive()?;

            let mut entries = Vec::with_capacity(batch.records.len());
            batch.records.iter()
                .enumerate()
                .for_each(|(i, r)| {
                    let entry = convert_kafka_record_to_record_entry(r, base_offset as i64 + i as i64 + 1);
                    entries.push(entry);
                });
            let joined = entries
                .into_iter()
                .map(|entry| {
                    serde_json::to_string(&entry).map_err(io::Error::other)
                })
                .collect::<std::result::Result<Vec<_>, io::Error>>()?
                .join("\n");
            writeln!(log_file, "{}", joined)?;

            FileExt::unlock(&log_file)?;
            // Updated offset file
            let current_offset = base_offset + batch.records.len() as i64;
            write_offset_to_file(&offset_file, current_offset)?;
            FileExt::unlock(&offset_file)?;

            Ok(base_offset)
        } else {
            Err(anyhow::anyhow!("No records provided"))
        }
    }

    async fn read_records(&self, topic: &str, partition: i32, target_offset: i64, max_offset: i64) -> anyhow::Result<Bytes> {
        let log_partition_dir = &self.log_store_dir.join(format!("{}/{}", topic, partition));
        if !log_partition_dir.exists() {
            create_dir_all(log_partition_dir)?;
        }
        let log_file_path = log_partition_dir.join("00000000000000000000.log");
        // offset file open and lock
        let log_file = OpenOptions::new()
            .read(true)
            .open(log_file_path)?;

        match read_record_from_file(&log_file, target_offset, max_offset) {
            Ok(record_entries) => {
                let records: Vec<kafka_protocol::records::Record> = record_entries
                    .into_iter()
                    .filter_map(|record_entry| {
                        record_entry
                            .convert_to_kafka_record()
                    }).collect();
                log::debug!("Records read: {:?}", records);
                let mut data = Vec::new();
                let encode_options = kafka_protocol::records::RecordEncodeOptions{
                    version: 2,
                    compression: kafka_protocol::records::Compression::None,
                };
                if let Err(e) = RecordBatchEncoder::encode(&mut data, &records, &encode_options) {
                    return Err(anyhow::anyhow!("Failed to encode record batch: {}", e));
                }
                Ok(Bytes::from(data))
            },
            Err(e) => Err(anyhow::anyhow!("Failed to read record: {}", e)),
        }

    }

    async fn read_offset(&self, topic: &str, partition: i32) -> anyhow::Result<i64> {
        let offset_file_path = self.log_store_dir.join(format!("{}/{}/offset.txt", topic, partition));
        let file = OpenOptions::new()
            .read(true)
            .open(offset_file_path)?;
        let current_offset = read_offset_from_file(&file);
        match current_offset {
            Ok(offset) => Ok(offset),
            Err(e) => Err(anyhow::anyhow!("Failed to read offset: {}", e)),
        }
    }

    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> anyhow::Result<()> {
        let topic_dir = self.log_store_dir.join(topic_id.to_string());
    
        if topic_dir.exists() {
            let prefix = timestamp_prefix();
            let deleted_dir = self
                .log_store_dir
                .join(".deleted")
                .join(format!("{}_{}", prefix, topic_id));
    
            fs::create_dir_all(deleted_dir.parent().unwrap())?;
            fs::rename(&topic_dir, &deleted_dir)?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Topic directory not found: {}", topic_id))
        }
    }
    
    async fn delete_topic_by_name(&self, topic_name: &str) -> anyhow::Result<()> {
        let topic_dir = self.log_store_dir.join(topic_name);
    
        if topic_dir.exists() {
            let prefix = timestamp_prefix();
            let deleted_dir = self
                .log_store_dir
                .join(".deleted")
                .join(format!("{}_{}", prefix, topic_name));
    
            fs::create_dir_all(deleted_dir.parent().unwrap())?;
            fs::rename(&topic_dir, &deleted_dir)?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Topic directory not found: {}", topic_name))
        }
    }
}

fn read_offset_from_file(file: &File) -> Result<i64> {
    let reader = BufReader::new(file);
    match  serde_json::from_reader::<_, Offset>(reader) {
        Ok(offset) => Ok(offset.offset),
        Err(e) => {
            if e.is_eof() || e.is_data() {
                Ok(-1)
            } else {
                Err(Error::new(InvalidData, e))
            }
        },
    }
}

fn write_offset_to_file(mut file: &File, offset: i64) -> Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    let offset_entry = Offset {
        offset,
    };
    serde_json::to_writer(file, &offset_entry)?;
    Ok(())
}

fn read_record_from_file(file: &File, target_offset: i64, max_offset: i64) -> Result<Vec<RecordEntry>> {
    let reader = BufReader::new(file);
    let mut result = Vec::new();

    for line in reader.lines() {
        let line = line.map_err(|e| {
            io::Error::other(
                format!("failed to read line from file: {}", e),
            )
        })?;

        let parsed: RecordEntry = serde_json::from_str(&line).map_err(|e| {
            io::Error::other(
                format!("failed to parse JSON line into RecordEntry: {}", e),
            )
        })?;

        let parsed_offset = parsed.offset; // Clone or copy the offset field
        if parsed_offset >= target_offset && parsed_offset < max_offset {
            result.push(parsed);
        }

        if parsed_offset >= max_offset {
            break; // Stop reading if we have reached or exceeded the max_offset
        }
    }
    Ok(result)
}

fn timestamp_prefix() -> String {
    Local::now().format("%Y%m%d_%H%M%S%3f").to_string()
}