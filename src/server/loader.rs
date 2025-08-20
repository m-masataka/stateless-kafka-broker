use crate::storage::{
    file::file_meta_store::FileMetaStore,
    file::file_log_store::FileLogStore,
    log_store_impl::LogStoreImpl,
    meta_store_impl::MetaStoreImpl,
    s3::s3_meta_store::S3MetaStore,
    s3::s3_log_store::S3LogStore,
    s3::s3_client::S3Client,
    redis::redis_meta_store::RedisMetaStore,
    redis::redis_client::RedisClient,
};
use crate::storage::{index_store_impl::IndexStoreImpl};
use crate::storage::redis::redis_index_store::RedisIndexStore;
use crate::common::config::StorageType;
use anyhow::Result;
use tokio::sync::Mutex;
use std::{
    sync::Arc,
};
use redis::cluster::ClusterClient;
use crate::common::config::ServerConfig;

pub async fn load_log_store(server_config: &ServerConfig) -> Result<LogStoreImpl> {
    let log_store_load = match &server_config.log_store_type {
        StorageType::S3 => {
            log::debug!("Using S3 log store");
            let bucket = server_config.log_store_s3_bucket.clone().ok_or_else(|| anyhow::anyhow!("S3 bucket not configured"))?;
            let prefix = server_config.log_store_s3_prefix.clone();
            let endpoint = server_config.log_store_s3_endpoint.clone().unwrap_or_else(|| "https://s3.amazonaws.com".to_string());
            let access_key = server_config.log_store_s3_access_key.clone().ok_or_else(|| anyhow::anyhow!("S3 access key not configured"))?;
            let secret_key = server_config.log_store_s3_secret_key.clone().ok_or_else(|| anyhow::anyhow!("S3 secretkey not configured"))?;
            let region = server_config.log_store_s3_region.clone().unwrap_or_else(|| "us-east-1".to_string());
            let s3_client = S3Client::new(&endpoint, &access_key, &secret_key, &region).await?;
            LogStoreImpl::S3(S3LogStore::new(s3_client, bucket, prefix))
        },
        StorageType::File => {
            log::debug!("Using File log store");
            LogStoreImpl::File(FileLogStore::new())
        },
        _ => {
            return Err(anyhow::anyhow!("Unsupported log store backend"));
        }
    };
    Ok(log_store_load)
}

pub async fn load_index_store(server_config: &ServerConfig) -> Result<IndexStoreImpl> {
    let index_store_load: IndexStoreImpl = match &server_config.index_store_type {
        StorageType::Redis => {
            log::debug!("Using Redis index store");
            let redis_urls = server_config.index_store_redis_urls.clone()
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<String>>();
            let redis_client: RedisClient = if redis_urls.len() > 1 {
                log::debug!("Using Redis Cluster with URLs: {:?}", redis_urls);
                let client = ClusterClient::new(redis_urls)?;
                let conn = client.get_async_connection().await?;
                RedisClient::new(true, Some(Arc::new(Mutex::new(conn))), None)
            } else {
                log::debug!("Using single Redis instance at: {}", redis_urls[0]);
                let client = redis::Client::open(redis_urls[0].clone())?;
                let conn = client.get_multiplexed_async_connection().await?;
                RedisClient::new(false, None, Some(Arc::new(Mutex::new(conn))))
            };
            IndexStoreImpl::Redis(RedisIndexStore::new(redis_client))
        },
        _ => {
            return Err(anyhow::anyhow!("Unsupported index store backend"));
        }
    };
    Ok(index_store_load)
}

pub async fn load_meta_store(server_config: &ServerConfig) -> Result<MetaStoreImpl> {
    let meta_store_load =  match &server_config.meta_store_type {
        StorageType::S3 => {
            log::debug!("Using S3 meta store");
            let bucket = server_config.meta_store_s3_bucket.clone().ok_or_else(|| anyhow::anyhow!("S3 bucket not configured"))?;
            let prefix = server_config.meta_store_s3_prefix.clone();
            let endpoint = server_config.meta_store_s3_endpoint.clone().unwrap_or_else(|| "https://s3.amazonaws.com".to_string());
            let access_key = server_config.meta_store_s3_access_key.clone().ok_or_else(|| anyhow::anyhow!("S3 access key not configured"))?;
            let secret_key = server_config.meta_store_s3_secret_key.clone().ok_or_else(|| anyhow::anyhow!("S3 secretkey not configured"))?;
            let region = server_config.meta_store_s3_region.clone().unwrap_or_else(|| "us-east-1".to_string());
            let s3_client = S3Client::new(&endpoint, &access_key, &secret_key, &region).await?;
            MetaStoreImpl::S3(S3MetaStore::new(s3_client, bucket, prefix))
        },
        StorageType::File => {
            MetaStoreImpl::File(FileMetaStore::new())
        },
        StorageType::Redis => {
            log::debug!("Using Redis meta store");
            let redis_urls = server_config.meta_store_redis_urls
                .as_ref()
                .cloned()
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<String>>();
            let redis_client: RedisClient = if redis_urls.len() > 1 {
                log::debug!("Using Redis Cluster with URLs: {:?}", redis_urls);
                let client = ClusterClient::new(redis_urls)?;
                let conn = client.get_async_connection().await?;
                RedisClient::new(true, Some(Arc::new(Mutex::new(conn))), None)
            } else {
                log::debug!("Using single Redis instance at: {}", redis_urls[0]);
                let client = redis::Client::open(redis_urls[0].clone())?;
                let conn = client.get_multiplexed_async_connection().await?;
                RedisClient::new(false, None, Some(Arc::new(Mutex::new(conn))))
            };
            MetaStoreImpl::Redis(RedisMetaStore::new(redis_client))
        }
    };
    Ok(meta_store_load)
}
