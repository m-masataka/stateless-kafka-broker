use crate::{common::record::{convert_kafka_record_to_record_entry, RecordEntry}, traits::log_store::LogStore};
use std::{
    io::{self, BufReader, Result},
};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::records::{
    RecordBatchDecoder,
    RecordBatchEncoder,
    RecordSet,
};
use crate::common::record::Offset;
use crate::storage::s3::s3_client::S3Client;

pub struct S3LogStore {
    s3_client: S3Client,
    bucket: String,
    prefix: Option<String>,
    log_file_name: String,
    offset_file_name: String,
}

impl S3LogStore {
    pub fn new(s3_client: S3Client, bucket: String, prefix: Option<String>) -> Self {
        Self {
            s3_client: s3_client,
            bucket,
            prefix,
            log_file_name: "00000000.log".to_string(),
            offset_file_name: "offset.txt".to_string(),
        }
    }
}

impl LogStore for S3LogStore {
    async fn write_batch(&self, topic_id: &str, partition: i32, records: Option<&Bytes>) -> anyhow::Result<i64> {
        if let Some(data) = records {
            let offset_key = self.offset_key(topic_id, partition);
            let object_key = self.log_key(topic_id, partition);
            
            let (offset_data, offset_etag) = match self.s3_client.get_object(&self.bucket, &offset_key).await {
                Ok((data, etag)) => (data, Some(etag)),
                Err(e) => {
                    log::warn!("Offset object not found. Initializing with offset -1. Error: {:?}", e);
                    let init_offset = encode_offset(-1);
                    self.s3_client.put_object(&self.bucket, &offset_key, &String::from_utf8_lossy(&init_offset), None).await?;
                    (init_offset, None)
                }
            };
            let base_offset = decode_offset(offset_data)?;

            let (output, current_offset) = bytes_to_joined_string(data, base_offset)?;

            let (obj, obj_etag) = match self.s3_client.get_object(&self.bucket, &object_key).await {
                Ok((data, etag)) => (data, Some(etag)),
                Err(e) => {
                    log::warn!("Log object not found. Initializing with empty log. Error: {:?}", e);
                    let init_log = "".to_string();
                    self.s3_client.put_object(&self.bucket, &object_key, &init_log, None).await?;
                    let init_log_bytes = Bytes::from(init_log);
                    (init_log_bytes, None)
                }
            };
            
            let output_bytes = Bytes::from(output);
            let mut buf = BytesMut::with_capacity(obj.len() + 1 + output_bytes.len());
            if !obj.is_empty() {
                buf.extend_from_slice(&obj);
                buf.extend_from_slice(b"\n"); // Add a newline to separate records
            }
            buf.extend_from_slice(&output_bytes);
            let buf_str = String::from_utf8(buf.to_vec())
                .map_err(|e| {
                    log::error!("Failed to convert bytes to string: {:?}", e);
                    anyhow::anyhow!("Invalid UTF-8 sequence in data")
                })?;

            self.s3_client.put_object(&self.bucket, &object_key, &buf_str, obj_etag).await
                .map_err(|e| {
                    log::error!("Failed to put object to S3: {:?}", e);
                    e
                })?;
            
            log::debug!("Successfully wrote batch to S3 for topic: {}, partition: {}", topic_id, partition);

            let output_offset = encode_offset(current_offset);
            let output_offset_str = String::from_utf8(output_offset.to_vec())
                .map_err(|e| {
                    log::error!("Failed to convert offset bytes to string: {:?}", e);
                    anyhow::anyhow!("Invalid UTF-8 sequence in offset data")
                })?;

            self.s3_client.put_object(&self.bucket, &offset_key, &output_offset_str, offset_etag).await
                .map_err(|e| {
                    log::error!("Failed to put offset object to S3: {:?}", e);
                    e
                })?;

            Ok(current_offset)
        } else {
            Err(anyhow::anyhow!("No records to write"))
        }
    }

    async fn read_offset(&self, topic_id: &str, partition: i32) -> anyhow::Result<i64> {
        let offset_key = self.offset_key(topic_id, partition);
        let (offset_data, _) = match self.s3_client.get_object(&self.bucket, &offset_key).await {
            Ok((data, etag)) => (data, Some(etag)),
            Err(e) => {
                log::warn!("Offset object not found. Initializing with offset -1. Error: {:?}", e);
                let init_offset = encode_offset(-1);
                self.s3_client.put_object(&self.bucket, &offset_key, &String::from_utf8_lossy(&init_offset), None).await?;
                (init_offset, None)
            }
        };
        let base_offset = decode_offset(offset_data)?;
        Ok(base_offset)
    }

    async fn read_records(&self, topic_id: &str, partition: i32, offset: i64, max_offset: i64) -> anyhow::Result<Bytes> {
        let object_key = self.log_key(topic_id, partition);
        let (data, _) = match self.s3_client.get_object(&self.bucket, &object_key).await {
            Ok((data, etag)) => (data, Some(etag)),
            Err(e) => {
                log::warn!("Log object not found. Initializing with empty log. Error: {:?}", e);
                let init_log = "".to_string();
                self.s3_client.put_object(&self.bucket, &object_key, &init_log, None).await?;
                (Bytes::from(init_log), None)
            }
        };
        log::debug!("Print data: {:?}", data);
        match read_record_from_byte(&data, offset, max_offset) {
            Ok(record_entries) => {
                let records: Vec<kafka_protocol::records::Record> = record_entries
                    .into_iter()
                    .filter_map(|record_entry| {
                        record_entry.convert_to_kafka_record()
                    }).collect();
                log::debug!("Records read: {:?}", records);
                let mut output = Vec::new();
                let encode_options = kafka_protocol::records::RecordEncodeOptions {
                    version: 2,
                    compression: kafka_protocol::records::Compression::None,
                };
                //RecordBatchEncoder::encode(&mut output, &records, &encode_options)
                //    .map_err(|e| anyhow::anyhow!("Failed to encode record batch: {}", e))?;
                if let Err(e) = RecordBatchEncoder::encode(&mut output, &records, &encode_options) {
                    log::error!("Failed to encode record batch: {}", e);
                    return Err(anyhow::anyhow!("Failed to encode record batch: {}", e));
                }
                Ok(Bytes::from(output))
            },
            Err(e) => Err(anyhow::anyhow!("Failed to read record: {}", e)),
        }
    }
}

impl S3LogStore {
    fn offset_key(&self, topic_id: &str, partition: i32) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}/{}/{}", prefix, topic_id, partition, self.offset_file_name),
            None => format!("{}/{}/{}", topic_id, partition, self.offset_file_name),
        }
    }

    fn log_key(&self, topic_id: &str, partition: i32) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}/{}/{}", prefix, topic_id, partition, self.log_file_name),
            None => format!("{}/{}/{}", topic_id, partition, self.log_file_name),
        }
    }
}

fn read_record_from_byte(data: &Bytes, target_offset: i64, max_offset: i64) -> Result<Vec<RecordEntry>> {
    let s = std::str::from_utf8(data)
        .map_err(|e| io::Error::other(format!("Invalid UTF-8 sequence in log: {}", e)))?;
    let mut result = Vec::new();

    for line in s.lines() {
        if line.trim().is_empty() {
            log::warn!("Skipping empty line in log data");
            continue;  // 空行はスキップ
        }
        let parsed: RecordEntry = serde_json::from_str(line).map_err(|e| {
            io::Error::other(format!("failed to parse JSON line into RecordEntry: {}", e))
        })?;

        let parsed_offset = parsed.offset;

        if parsed_offset >= target_offset && parsed_offset < max_offset {
            result.push(parsed);
        }

        if parsed_offset >= max_offset {
            break;
        }
    }
    Ok(result)
}

fn bytes_to_joined_string(data: &Bytes, base_offset: i64) -> anyhow::Result<(String, i64)> {
    let mut cursor = std::io::Cursor::new(data);
    let batch: RecordSet = RecordBatchDecoder::decode(&mut cursor)?;
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
    let current_offset = base_offset + batch.records.len() as i64;

    Ok((joined, current_offset))
}

fn decode_offset(data: Bytes) -> anyhow::Result<i64> {
    let reader = BufReader::new(data.reader());
    match  serde_json::from_reader::<_, Offset>(reader) {
        Ok(offset) => Ok(offset.offset),
        Err(e) => {
            if e.is_eof() || e.is_data() {
                Ok(-1)
            } else {
                Err(anyhow::anyhow!("Failed to read offset: {}", e))
            }
        },
    }
}

fn encode_offset(offset: i64) -> Bytes {
    let offset_entry = Offset { offset };
    serde_json::to_string(&offset_entry)
        .map(|s| Bytes::from(s))
        .unwrap_or_else(|_| Bytes::new())
}
