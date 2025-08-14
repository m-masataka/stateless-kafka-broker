use crate::traits::log_store::UnsendLogStore;
use bytes::Bytes;
use kafka_protocol::records::RecordBatchEncoder;
use crate::storage::s3::s3_client::S3Client;
use crate::common::record::{bytes_to_output, record_bytes_to_kafka_response_bytes};
use kafka_protocol::records::Record;
use tokio::sync::mpsc;
use futures::future::try_join_all;

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
    async fn write_records(&self, topic_id: &str, partition: i32, start_offset: i64, records: Option<&Bytes>) -> anyhow::Result<(i64, String, u64)> {
        if let Some(data) = records {
            let object_key = self.log_key(topic_id, partition, start_offset);
            log::debug!("Writing records to S3 at key: {}", object_key);
            
            let (output, current_offset) = bytes_to_output(data, start_offset)?;
            
            self.s3_client.put_object(&self.bucket, &object_key, &output, None).await
                .map_err(|e| {
                    log::error!("Failed to put object to S3: {:?}", e);
                    e
                })?;
            
            log::debug!("Successfully wrote batch to S3 for topic: {}, partition: {}", topic_id, partition);
            Ok((current_offset, object_key, output.len() as u64))
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

        let (tx, mut rx) = mpsc::channel::<Vec<Record>>(keys.len());
        let bucket = self.bucket.clone();
        let tasks: Vec<_> = keys.into_iter().map(|key| {
            let tx = tx.clone();
            let s3_client = self.s3_client.clone(); // Assuming self.s3_client is Cloneable
            let bucket = bucket.clone();
            tokio::spawn(async move {
                let data = match s3_client.get_object(&bucket, &key).await {
                    Ok((data, _)) => data,
                    Err(e) => {
                        log::warn!("Log object not found for key {}. Initializing with empty log. Error: {:?}", key, e);
                        let init_log = Vec::new();
                        Bytes::from(init_log)
                    }
                };

                let records = match record_bytes_to_kafka_response_bytes(&data) {
                    Ok(records) => records,
                    Err(e) => {
                        log::error!("Failed to read record from S3 for key {}: {}", key, e);
                        return Err(anyhow::anyhow!("Error processing key: {}", key));
                    }
                };

                // Send the result to the main thread (receiver)
                if let Err(_) = tx.send(records).await {
                    log::error!("Failed to send processed records for key {}", key);
                }
                Ok::<(), anyhow::Error>(())
            })
        }).collect();

        // Wait for all tasks to complete
        let _ = try_join_all(tasks).await?; // Ensure all tasks complete
        drop(tx);

        // Now collect records from the receiver
        while let Some(records) = rx.recv().await {
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
