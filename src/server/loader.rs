use crate::common::config::ServerConfig;
use crate::common::config::StorageType;
use crate::common::utils::jittered_delay;
use crate::storage::{
    file::{file_log_store::FileLogStore, file_meta_store::FileMetaStore},
    index_store_impl::IndexStoreImpl,
    log_store_impl::LogStoreImpl,
    meta_store_impl::MetaStoreImpl,
    redis::{redis_index_store::RedisIndexStore, redis_meta_store::RedisMetaStore},
    s3::{s3_client::S3Client, s3_log_store::S3LogStore, s3_meta_store::S3MetaStore},
    tikv::{tikv_index_store::TikvIndexStore, tikv_meta_store::TikvMetaStore},
};
use anyhow::Result;
use fred::prelude::Pool;
use fred::prelude::{Builder, ClientLike, Config, ReconnectPolicy, TcpConfig};
use std::time::Duration;
use tokio::time::sleep;

fn redis_uri(nodes: Vec<String>) -> String {
    assert!(!nodes.is_empty(), "nodes must not be empty");
    if nodes.len() == 1 {
        // single instance
        format!("redis://{}", nodes[0])
    } else {
        // cluster
        let mut uri = format!("redis-cluster://{}", nodes[0]);
        let query: String = nodes[1..]
            .iter()
            .map(|n| format!("node={}", n))
            .collect::<Vec<_>>()
            .join("&");
        if !query.is_empty() {
            uri.push('?');
            uri.push_str(&query);
        }
        log::debug!("Redis Cluster URI: {:?}", uri);
        uri
    }
}

pub async fn load_log_store(server_config: &ServerConfig) -> Result<LogStoreImpl> {
    let log_store_load = match &server_config.log_store_type {
        StorageType::S3 => {
            log::debug!("Using S3 log store");
            let bucket = server_config
                .log_store_s3_bucket
                .clone()
                .ok_or_else(|| anyhow::anyhow!("S3 bucket not configured"))?;
            let prefix = server_config.log_store_s3_prefix.clone();
            let endpoint = server_config
                .log_store_s3_endpoint
                .clone()
                .unwrap_or_else(|| "https://s3.amazonaws.com".to_string());
            let access_key = server_config
                .log_store_s3_access_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("S3 access key not configured"))?;
            let secret_key = server_config
                .log_store_s3_secret_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("S3 secretkey not configured"))?;
            let region = server_config
                .log_store_s3_region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_string());
            let s3_client = S3Client::new(&endpoint, &access_key, &secret_key, &region).await?;
            LogStoreImpl::S3(S3LogStore::new(s3_client, bucket, prefix))
        }
        StorageType::File => {
            log::debug!("Using File log store");
            LogStoreImpl::File(FileLogStore::new())
        }
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
            let redis_urls = server_config
                .index_store_redis_urls
                .clone()
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<String>>();

            let config =
                Config::from_url(&*redis_uri(redis_urls)).expect("Failed to create Redis config");
            let pool_size = server_config.index_store_redis_pool_size.unwrap_or(10);
            let redis_client = Builder::from_config(config)
                .with_connection_config(|c| {
                    c.connection_timeout = Duration::from_secs(10);
                    c.tcp = TcpConfig {
                        nodelay: Some(true),
                        ..Default::default()
                    };
                })
                .set_policy(ReconnectPolicy::new_exponential(1000, 1000, 30000, 3))
                .build_pool(pool_size)
                .expect("Failed to create redis pool");
            init_with_retry(&redis_client, 0, 1000, 30_000, 3).await?;
            // redis_client.init().await.expect("Failed to connect to redis");
            IndexStoreImpl::Redis(RedisIndexStore::new(redis_client))
        }
        StorageType::Tikv => {
            log::debug!("Using TiKV index store");
            let tikv_endpoints = server_config
                .index_store_tikv_endpoints
                .clone()
                .ok_or_else(|| anyhow::anyhow!("TiKV endpoints not configured"))?;
            let tikv_endpoint_vec = tikv_endpoints
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<String>>();
            let tikv_client = tikv_client::TransactionClient::new(tikv_endpoint_vec).await?;
            IndexStoreImpl::Tikv(TikvIndexStore::new(tikv_client))
        }
        _ => {
            return Err(anyhow::anyhow!("Unsupported index store backend"));
        }
    };
    Ok(index_store_load)
}

pub async fn load_meta_store(server_config: &ServerConfig) -> Result<MetaStoreImpl> {
    let meta_store_load = match &server_config.meta_store_type {
        StorageType::S3 => {
            log::debug!("Using S3 meta store");
            let bucket = server_config
                .meta_store_s3_bucket
                .clone()
                .ok_or_else(|| anyhow::anyhow!("S3 bucket not configured"))?;
            let prefix = server_config.meta_store_s3_prefix.clone();
            let endpoint = server_config
                .meta_store_s3_endpoint
                .clone()
                .unwrap_or_else(|| "https://s3.amazonaws.com".to_string());
            let access_key = server_config
                .meta_store_s3_access_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("S3 access key not configured"))?;
            let secret_key = server_config
                .meta_store_s3_secret_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("S3 secretkey not configured"))?;
            let region = server_config
                .meta_store_s3_region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_string());
            let s3_client = S3Client::new(&endpoint, &access_key, &secret_key, &region).await?;
            MetaStoreImpl::S3(S3MetaStore::new(s3_client, bucket, prefix))
        }
        StorageType::File => MetaStoreImpl::File(FileMetaStore::new()),
        StorageType::Redis => {
            log::debug!("Using Redis meta store");
            let redis_urls = server_config
                .meta_store_redis_urls
                .as_ref()
                .cloned()
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<String>>();
            let config =
                Config::from_url(&*redis_uri(redis_urls)).expect("Failed to create Redis config");
            let pool_size = server_config.meta_store_redis_pool_size.unwrap_or(10);
            let redis_client = Builder::from_config(config)
                .with_connection_config(|c| {
                    c.connection_timeout = Duration::from_secs(10);
                    c.tcp = TcpConfig {
                        nodelay: Some(true),
                        ..Default::default()
                    };
                })
                .set_policy(ReconnectPolicy::new_exponential(1000, 1000, 30000, 3))
                .build_pool(pool_size)
                .expect("Failed to create redis pool");
            init_with_retry(&redis_client, 0, 1000, 30_000, 3).await?;
            MetaStoreImpl::Redis(RedisMetaStore::new(redis_client))
        }
        StorageType::Tikv => {
            log::debug!("Using TiKV meta store");
            let tikv_endpoints = server_config
                .meta_store_tikv_endpoints
                .clone()
                .ok_or_else(|| anyhow::anyhow!("TiKV endpoints not configured"))?;
            let tikv_endpoint_vec = tikv_endpoints
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<String>>();
            let tikv_client = tikv_client::TransactionClient::new(tikv_endpoint_vec).await?;
            MetaStoreImpl::Tikv(TikvMetaStore::new(tikv_client))
        }
    };
    Ok(meta_store_load)
}

async fn init_with_retry(
    pool: &Pool,
    max_attempts: u32,
    min_delay_ms: u64,
    max_delay_ms: u64,
    base: u32,
) -> anyhow::Result<()> {
    let mut attempt = 0;
    let mut delay = min_delay_ms;

    loop {
        attempt += 1;
        match pool.init().await {
            Ok(_) => {
                log::info!("✅ Connected to Redis on attempt {}", attempt);
                return Ok(());
            }
            Err(e) => {
                if max_attempts != 0 && attempt >= max_attempts {
                    return Err(anyhow::anyhow!(
                        "Redis init failed after {} attempts: {e}",
                        attempt
                    ));
                }
                // backoff with jitter
                let jitter = jittered_delay(100);
                let sleep_ms = delay.saturating_add(jitter).min(max_delay_ms);
                log::warn!(
                    "⚠️ Redis init failed (attempt {}): {} -> retry in {} ms",
                    attempt,
                    e,
                    sleep_ms
                );
                sleep(Duration::from_millis(sleep_ms)).await;
                // next delay
                delay = (delay.saturating_mul(base as u64)).min(max_delay_ms);
            }
        }
    }
}
