use aws_config::Region;
use aws_sdk_s3::{config::{Credentials, SharedCredentialsProvider}, primitives::ByteStream, Client};
use bytes::Bytes;
use anyhow::Result;

pub struct S3Client {
    client: Client,
}

impl S3Client {
    pub async fn new(endpoint: &str, access_key: &str, secret_key: &str, region: &str) -> Result<Self> {
        log::info!("Creating S3 client with endpoint: {}, region: {}", endpoint, region);
        let credentials = Credentials::new(access_key.to_string(), secret_key.to_string(), None, None, "custom");
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
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
}