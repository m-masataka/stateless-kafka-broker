use crate::storage::index_store_impl::IndexStoreImpl;
use crate::storage::redis::redis_index_store::RedisIndexStore;
use crate::storage::s3::s3_client::S3Client;
use crate::storage::s3::s3_log_store::S3LogStore;
use crate::storage::{
    file::file_meta_store::FileMetaStore,
    file::file_log_store::FileLogStore,
    log_store_impl::LogStoreImpl,
    meta_store_impl::MetaStoreImpl,
    s3::s3_meta_store::S3MetaStore,
    redis::redis_meta_store::RedisMetaStore,
    redis::redis_client::RedisClient,
};

use crate::handler::{
    api_versions::handle_api_versions_request,
    metadata::handle_metadata_request,
    heartbeat::handle_heartbeat_request,
    init_producer_id::handle_init_producer_id_request,
    produce::handle_produce_request,
    create_topics::handle_create_topics_request,
    find_coordinator::handle_find_coordinator_request,
    join_group::handle_join_group_request,
    sync_group::handle_sync_group_request,
    offset_fetch::handle_offset_fetch_request,
    fetch::handle_fetch_request,
    delete_topics::handle_delete_topics_request,
    offset_commit::handle_offset_commit_request,
    leave_group::handle_leave_group_request,
    consumer_group_heartbeat::handle_consumer_group_heartbeat_request,
};
use crate::common::config::{load_cluster_config, load_server_config, ClusterConfig, StorageType};
use anyhow::Result;
use tokio::sync::Mutex;
use std::{
    sync::Arc,
};
use tokio::io::AsyncReadExt;

use kafka_protocol::messages::{
    RequestHeader,
    ApiKey,
    ApiVersionsRequest,
    metadata_request::MetadataRequest,
    init_producer_id_request::InitProducerIdRequest,
    produce_request::ProduceRequest,
    create_topics_request::CreateTopicsRequest,
    find_coordinator_request::FindCoordinatorRequest,
    join_group_request::JoinGroupRequest,
    sync_group_request::SyncGroupRequest,
    offset_fetch_request::OffsetFetchRequest,
    fetch_request::FetchRequest,
    offset_commit_request::OffsetCommitRequest,
    delete_topics_request::DeleteTopicsRequest,
    leave_group_request::LeaveGroupRequest,
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
};
use kafka_protocol::protocol::Decodable;
use redis::cluster::ClusterClient;

pub async fn server_start(config_path: &str) -> anyhow::Result<()> {
    env_logger::init();
    log::info!("Starting Kafka-compatible server...");
    let cluster_conf_load = Arc::new(load_cluster_config(config_path)?);
    let server_config = load_server_config()?;


    let listener = tokio::net::TcpListener::bind(format!("{}:{}", server_config.host, server_config.port)).await?;
    log::info!("Kafka-compatible server listening on port {}:{}", server_config.host, server_config.port);

    let meta_store_load =  match &server_config.meta_store_type {
        StorageType::S3 => {
            log::info!("Using S3 meta store");
            let bucket = server_config.meta_store_s3_bucket.clone().ok_or_else(|| anyhow::anyhow!("S3 bucket not configured"))?;
            let prefix = server_config.meta_store_s3_prefix.clone();
            let endpoint = server_config.meta_store_s3_endpoint.clone().unwrap_or_else(|| "https://s3.amazonaws.com".to_string());
            let access_key = server_config.meta_store_s3_access_key.clone().ok_or_else(|| anyhow::anyhow!("S3 access key not configured"))?;
            let secret_key = server_config.meta_store_s3_secret_key.clone().ok_or_else(|| anyhow::anyhow!("S3 secretkey not configured"))?;
            let region = server_config.meta_store_s3_region.clone().unwrap_or_else(|| "us-east-1".to_string());
            let s3_client = S3Client::new(&endpoint, &access_key, &secret_key, &region).await?;
            Arc::new(MetaStoreImpl::S3(S3MetaStore::new(s3_client, bucket, prefix)))
        },
        StorageType::File => {
            Arc::new(MetaStoreImpl::File(FileMetaStore::new()))
        },
        StorageType::Redis => {
            log::info!("Using Redis meta store");
            let redis_urls = server_config.meta_store_redis_urls
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<String>>();
            let redis_client: RedisClient = if redis_urls.len() > 1 {
                log::info!("Using Redis Cluster with URLs: {:?}", redis_urls);
                let client = ClusterClient::new(redis_urls)?;
                let conn = client.get_async_connection().await?;
                RedisClient::new(true, Some(Arc::new(Mutex::new(conn))), None)
            } else {
                log::info!("Using single Redis instance at: {}", redis_urls[0]);
                let client = redis::Client::open(redis_urls[0].clone())?;
                let conn = client.get_multiplexed_async_connection().await?;
                RedisClient::new(false, None, Some(Arc::new(Mutex::new(conn))))
            };
            Arc::new(MetaStoreImpl::Redis(RedisMetaStore::new(redis_client)))
        }
    };

    let log_store_load = match &server_config.log_store_type {
        StorageType::S3 => {
            log::info!("Using S3 log store");
            let bucket = server_config.log_store_s3_bucket.clone().ok_or_else(|| anyhow::anyhow!("S3 bucket not configured"))?;
            let prefix = server_config.log_store_s3_prefix.clone();
            let endpoint = server_config.log_store_s3_endpoint.clone().unwrap_or_else(|| "https://s3.amazonaws.com".to_string());
            let access_key = server_config.log_store_s3_access_key.clone().ok_or_else(|| anyhow::anyhow!("S3 access key not configured"))?;
            let secret_key = server_config.log_store_s3_secret_key.clone().ok_or_else(|| anyhow::anyhow!("S3 secretkey not configured"))?;
            let region = server_config.log_store_s3_region.clone().unwrap_or_else(|| "us-east-1".to_string());
            let s3_client = S3Client::new(&endpoint, &access_key, &secret_key, &region).await?;
            Arc::new(LogStoreImpl::S3(S3LogStore::new(s3_client, bucket, prefix)))
        },
        StorageType::File => {
            log::info!("Using File log store");
            Arc::new(LogStoreImpl::File(FileLogStore::new()))
        },
        _ => {
            return Err(anyhow::anyhow!("Unsupported log store backend"));
        }
    };

    let index_store_load = match &server_config.index_store_type {
        StorageType::Redis => {
            log::info!("Using Redis index store");
            let redis_urls = server_config.index_store_redis_urls
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<String>>();
            let redis_client: RedisClient = if redis_urls.len() > 1 {
                log::info!("Using Redis Cluster with URLs: {:?}", redis_urls);
                let client = ClusterClient::new(redis_urls)?;
                let conn = client.get_async_connection().await?;
                RedisClient::new(true, Some(Arc::new(Mutex::new(conn))), None)
            } else {
                log::info!("Using single Redis instance at: {}", redis_urls[0]);
                let client = redis::Client::open(redis_urls[0].clone())?;
                let conn = client.get_multiplexed_async_connection().await?;
                RedisClient::new(false, None, Some(Arc::new(Mutex::new(conn))))
            };
            Arc::new(IndexStoreImpl::Redis(RedisIndexStore::new(redis_client)))
        },
        _ => {
            return Err(anyhow::anyhow!("Unsupported index store backend"));
        }
    };
    
    loop {
        let (stream, addr) = listener.accept().await?;
        log::info!("Accepted connection from {:?}", addr);
        let cluster_config = cluster_conf_load.clone();
        let meta_store = meta_store_load.clone();
        let log_store = log_store_load.clone();
        let index_store = index_store_load.clone();

        // Spawn a new task for each connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, cluster_config, meta_store, log_store, index_store).await {
                log::error!("Connection error: {:?}", e);
            }
        });
    }
}

async fn handle_connection(stream: tokio::net::TcpStream, 
    cluster_config: Arc<ClusterConfig>,
    meta_store: Arc<MetaStoreImpl>,
    log_store: Arc<LogStoreImpl>,
    index_store: Arc<IndexStoreImpl>,
) -> Result<()> {
    let stream = Arc::new(Mutex::new(stream));
    loop {
        // Read length prefix
        let mut length_buf = [0u8; 4];
        let mut locked = stream.lock().await;
        if let Err(e) = locked.read_exact(&mut length_buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                log::info!("Client disconnected gracefully");
                return Ok(());
            } else {
                return Err(e.into());
            }
        }

        let total_len = u32::from_be_bytes(length_buf);
        // Read full request
        let mut buffer = vec![0u8; total_len as usize];
        let mut locked = stream.lock().await;
        locked.read_exact(&mut buffer).await?;

        let mut peek = std::io::Cursor::new(&buffer);
        let api_key = AsyncReadExt::read_i16(&mut peek).await?;
        let api_version = AsyncReadExt::read_i16(&mut peek).await?;
        let header_version = ApiKey::try_from(api_key).unwrap().request_header_version(api_version);

        let mut buf = std::io::Cursor::new(&buffer);
        let header = RequestHeader::decode(&mut buf, header_version).unwrap();
        let api_key = ApiKey::try_from(header.request_api_key).unwrap();
        log::debug!(
            "api_key {:?}, api_version {}, header_version {}, correlation_id {:?}",
            api_key, api_version, header_version, header.correlation_id
        );
        // clone the stream to pass it to the handler
        let stream_clone = stream.clone();
        let cluster_config = cluster_config.clone();
        let meta_store = meta_store.clone();
        let log_store = log_store.clone();
        let index_store = index_store.clone();

        tokio::spawn(async move {
            log::debug!(
                "api_key {:?}, api_version {}, header_version {}, correlation_id {:?}",
                api_key, api_version, header_version, header.correlation_id
            );

            let mut buf = std::io::Cursor::new(&buffer); // 再decode用

            let result = match api_key {
                ApiKey::ApiVersions => {
                    let req = ApiVersionsRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_api_versions_request(&mut *s, &header, &req).await
                }
                ApiKey::Metadata => {
                    let req = MetadataRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_metadata_request(&mut *s, &header, &req, &cluster_config, &*meta_store).await
                }
                ApiKey::DeleteTopics => {
                    let req = DeleteTopicsRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_delete_topics_request(&mut *s, &header, &req, &*meta_store).await
                }
                ApiKey::FindCoordinator => {
                    let req = FindCoordinatorRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_find_coordinator_request(&mut *s, &header, &req, &cluster_config).await
                }
                ApiKey::JoinGroup => {
                    let req = JoinGroupRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_join_group_request(&mut *s, &header, &req, &*meta_store).await
                }
                ApiKey::LeaveGroup => {
                    let req = LeaveGroupRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_leave_group_request(&mut *s, &header, &req, &*meta_store).await
                }
                ApiKey::SyncGroup => {
                    let req = SyncGroupRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_sync_group_request(&mut *s, &header, &req, &*meta_store).await
                }
                ApiKey::OffsetCommit => {
                    let req = OffsetCommitRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_offset_commit_request(&mut *s, &header, &req, &*meta_store).await
                }
                ApiKey::InitProducerId => {
                    let req = InitProducerIdRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_init_producer_id_request(&mut *s, &header, &req, &*meta_store).await
                }
                ApiKey::OffsetFetch => {
                    let req = OffsetFetchRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_offset_fetch_request(&mut *s, &header, &req, &*meta_store).await
                }
                ApiKey::Fetch => {
                    let req = FetchRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_fetch_request(&mut *s, &header, &req, &*log_store, &*meta_store, &*index_store).await
                }
                ApiKey::Produce => {
                    let req = ProduceRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_produce_request(&mut *s, &header, &req, &cluster_config, &*meta_store, &*log_store, &*index_store).await
                }
                ApiKey::CreateTopics => {
                    let req = CreateTopicsRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_create_topics_request(&mut *s, &header, &req, &*meta_store).await
                }
                ApiKey::ConsumerGroupHeartbeat => {
                    let req = ConsumerGroupHeartbeatRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_consumer_group_heartbeat_request(&mut *s, &header, &req, &*meta_store).await
                }
                ApiKey::Heartbeat => {
                    // let req = HeartbeatRequest::decode(&mut buf, header.request_api_version).unwrap();
                    let mut s = stream_clone.lock().await;
                    handle_heartbeat_request(&mut *s, &header).await
                }
                _ => {
                    log::error!("Unsupported API Key: {:?}", api_key);
                    Ok(())
                }
            };

            if let Err(e) = result {
                log::error!("Request handling error: {:?}", e);
            }
        });
    }
}
