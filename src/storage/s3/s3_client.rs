use aws_sdk_s3::{primitives::ByteStream, Client};
use bytes::Bytes;
use anyhow::Result;

pub struct S3Client {
    client: Client,
}

impl S3Client {
    pub async fn new() -> Result<Self> {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
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

    pub async fn put_object(&self, bucket: &str, key: &str, body: &str, etag: Option<String>) -> Result<()> {
        let response = match etag {
            Some(etag) => {
                log::debug!("Putting object to S3 with ETag: {}", etag);
                self.client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body.as_bytes().to_vec()))
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
                    .body(ByteStream::from(body.as_bytes().to_vec()))
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