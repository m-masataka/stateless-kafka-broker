use std::time::Duration;

use aws_config::{timeout::TimeoutConfig, Region};
use aws_sdk_s3::{config::{Credentials, SharedCredentialsProvider}, primitives::ByteStream, Client};
use bytes::Bytes;
use anyhow::Result;

#[derive(Clone)]
pub struct S3Client {
    client: Client,
}

impl S3Client {
    pub async fn new(endpoint: &str, access_key: &str, secret_key: &str, region: &str) -> Result<Self> {
        log::debug!("Creating S3 client with endpoint: {}, region: {}", endpoint, region);
        let credentials = Credentials::new(access_key.to_string(), secret_key.to_string(), None, None, "custom");
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(20))
                    .operation_attempt_timeout(Duration::from_millis(1500))
                    .build()
            )
            .region(Region::new(region.to_string()))
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .force_path_style(true)
            .endpoint_url(endpoint.to_string())
            .build();
        let client = Client::from_conf(config);
        Ok(S3Client { client })
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(Bytes, String)> {
        let output = self.client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        let etag = output.e_tag()
            .ok_or_else(|| anyhow::anyhow!("ETag not found in response"))?
            .to_string();
        let data = output.body.collect().await?.into_bytes();
        Ok((data, etag))
    }

    pub async fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> Result<Vec<String>> {
        let mut objects = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self.client.list_objects_v2().bucket(bucket);
            if let Some(prefix) = prefix {
                request = request.prefix(prefix);
            }
            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;
            for object in response.contents() {
                if let Some(key) = object.key() {
                    objects.push(key.to_string());
                }
            }

            if response.is_truncated() == Some(false) || response.is_truncated().is_none() {
                break;
            }
            continuation_token = response.next_continuation_token().map(|token| token.to_owned());
        }

        Ok(objects)
    }

    pub async fn put_object(&self, bucket: &str, key: &str, body: &Vec<u8>, etag: Option<String>) -> Result<()> {
        let response = match etag {
            Some(etag) => {
                log::debug!("Putting object to S3 with ETag: {}", etag);
                self.client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body.clone()))
                    .set_if_match(Some(etag)) // CAS Lock
                    .send()
                    .await
            }
            None => {
                log::debug!("Putting object to S3 without ETag");
                self.client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body.clone()))
                    .send()
                    .await
            }
        };
        match response {
            Ok(_) => {
                log::debug!("Committing changes to S3 (CAS succeeded)");
                Ok(())
            }
            Err(ref e) if e.to_string().contains("PreconditionFailed") => {
                log::warn!("CAS failed due to ETag mismatch");
                Err(anyhow::anyhow!("CAS failed"))
            }
            Err(e) => {
                log::error!("S3 error: {:?}", e);
                Err(e.into())
            }
        }
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        log::debug!("Deleting object from S3: {}", key);
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                log::error!("Failed to delete object from S3: {:?}", e);
                anyhow::anyhow!("Failed to delete object from S3: {:?}", e)
            })?;
        log::debug!("Successfully deleted object from S3: {}", key);
        Ok(())
    }

    pub async fn acquire_lock(&self, bucket: &str, key: &str) -> Result<()> {
        log::debug!("Acquiring lock for key: {}", key);
        let response = self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from("lock".as_bytes().to_vec()))
            .send()
            .await;

        match response {
            Ok(_) => {
                log::debug!("Lock acquired successfully for key: {}", key);
                Ok(())
            }
            Err(e) if e.to_string().contains("PreconditionFailed") => {
                log::warn!("Lock already exists for key: {}, retrying...", key);
                Err(anyhow::anyhow!("Lock already exists"))
            }
            Err(e) => {
                log::error!("Failed to acquire lock for key: {}: {:?}", key, e);
                Err(e.into())
            }
        }
    }

    pub async fn release_lock(&self, bucket: &str, key: &str) -> Result<()> {
        log::debug!("Releasing lock for key: {}", key);
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                log::error!("Failed to release lock for key: {}: {:?}", key, e);
                anyhow::anyhow!("Failed to release lock for key: {}: {:?}", key, e)
            })?;
        log::debug!("Lock released successfully for key: {}", key);
        Ok(())
    }
}