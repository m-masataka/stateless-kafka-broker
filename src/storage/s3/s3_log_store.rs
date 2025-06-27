use crate::traits::log_store::UnsendLogStore;
use bytes::Bytes;
use kafka_protocol::records::RecordBatchEncoder;
use crate::storage::s3::s3_client::S3Client;
use crate::common::record::{bytes_to_output, record_bytes_to_kafka_response_bytes};

pub struct S3LogStore {
    s3_client: S3Client,
    bucket: String,
    prefix: Option<String>,
    log_file_name_prefix: String,
}

impl S3LogStore {
    pub fn new(s3_client: S3Client, bucket: String, prefix: Option<String>) -> Self {
        Self {
            s3_client: s3_client,
            bucket,
            prefix,
            log_file_name_prefix: "log-file-".to_string(),
        }
    }
}

impl UnsendLogStore for S3LogStore {
    async fn write_records(&self, topic_id: &str, partition: i32, start_offset: i64, records: Option<&Bytes>) -> anyhow::Result<(i64, String)> {
        if let Some(data) = records {
            let object_key = self.log_key(topic_id, partition, start_offset);
            
            let (output, current_offset) = bytes_to_output(data, start_offset)?;
            
            self.s3_client.put_object(&self.bucket, &object_key, &output, None).await
                .map_err(|e| {
                    log::error!("Failed to put object to S3: {:?}", e);
                    e
                })?;
            
            log::debug!("Successfully wrote batch to S3 for topic: {}, partition: {}", topic_id, partition);
            Ok((current_offset, object_key))
        } else {
            Err(anyhow::anyhow!("No records to write"))
        }
    }

    async fn read_records(&self, keys: Vec<String>) -> anyhow::Result<Bytes> {
        // Read records from S3, and return Bytes encoded data.
        if keys.is_empty() {
            return Err(anyhow::anyhow!("No keys provided for reading records"));
        }
        let mut response = Vec::new();
        for key in keys {
            let data = match self.s3_client.get_object(&self.bucket, &key).await {
                Ok((data, _)) => data,
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

impl S3LogStore {
    fn log_key(&self, topic_id: &str, partition: i32, offset: i64) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}/{}/{}{}.log", prefix, topic_id, partition, self.log_file_name_prefix, offset),
            None => format!("{}/{}/{}{}.log", topic_id, partition, self.log_file_name_prefix, offset),
        }
    }
}
