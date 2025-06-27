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
    RequestKind,
    RequestHeader,
    ApiKey,
    ApiVersionsRequest,
    metadata_request::MetadataRequest,
    heartbeat_request::HeartbeatRequest,
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
use redis::Client;

pub async fn server_start() -> anyhow::Result<()> {
    env_logger::init();
    log::info!("Starting Kafka-compatible server...");
    let cluster_conf_load = Arc::new(load_cluster_config("config/cluster.json")?);
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
            let url = server_config.index_store_redis_url.clone().ok_or_else(|| anyhow::anyhow!("Redis URL not configured"))?;
            let client = Client::open(url)?;
            let conn = client.get_multiplexed_async_connection().await?;
            Arc::new(MetaStoreImpl::Redis(RedisMetaStore::new(Arc::new(Mutex::new(conn)))))
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
            let url = server_config.index_store_redis_url.clone().ok_or_else(|| anyhow::anyhow!("Redis URL not configured"))?;
            let client = Client::open(url)?;
            let conn = client.get_multiplexed_async_connection().await?;
            Arc::new(IndexStoreImpl::Redis(RedisIndexStore::new(Arc::new(Mutex::new(conn)))))
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

async fn handle_connection(mut stream: tokio::net::TcpStream, 
    cluster_config: Arc<ClusterConfig>,
    meta_store: Arc<MetaStoreImpl>,
    log_store: Arc<LogStoreImpl>,
    index_store: Arc<IndexStoreImpl>,
) -> Result<()> {
    loop {
        // Read length prefix
        let mut length_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut length_buf).await {
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
        stream.read_exact(&mut buffer).await?;

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
        match api_key {
            ApiKey::ApiVersions => {
                log::debug!("ApiVersion Request");
                if let RequestKind::ApiVersions(ref req) =
                    RequestKind::ApiVersions(ApiVersionsRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_api_versions_request(&mut stream, &header, req).await?;
                }
            }
            ApiKey::Metadata => {
                log::debug!("Metadata Request");
                if let RequestKind::Metadata(ref req) =
                    RequestKind::Metadata(MetadataRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_metadata_request(&mut stream, &header, req, &cluster_config, &*meta_store).await?;
                }
            }
            ApiKey::DeleteTopics => {
                log::debug!("DeleteTopics Request");
                if let RequestKind::DeleteTopics(ref req) =
                    RequestKind::DeleteTopics(DeleteTopicsRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_delete_topics_request(&mut stream, &header, req, &*meta_store).await?;
                }
            }
            ApiKey::FindCoordinator => {
                log::debug!("FindCoordinator Request");
                if let RequestKind::FindCoordinator(ref req) =
                    RequestKind::FindCoordinator(FindCoordinatorRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_find_coordinator_request(&mut stream, &header, req, &cluster_config).await?;
                }
            }
            ApiKey::JoinGroup => {
                log::debug!("JoinGroup Request");
                if let RequestKind::JoinGroup(ref req) =
                    RequestKind::JoinGroup(JoinGroupRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_join_group_request(&mut stream, &header, req, &*meta_store).await?;
                }
            }
            ApiKey::LeaveGroup => {
                log::debug!("LeaveGroup Request");
                if let RequestKind::LeaveGroup(ref req) =
                    RequestKind::LeaveGroup(LeaveGroupRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_leave_group_request(&mut stream, &header, req, &*meta_store).await?;
                }
            }
            ApiKey::SyncGroup => {
                log::debug!("SyncGroup Request");
                if let RequestKind::SyncGroup(ref req) =
                    RequestKind::SyncGroup(SyncGroupRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_sync_group_request(&mut stream, &header, req, &*meta_store).await?;
                }
            }
            ApiKey::OffsetCommit => {
                log::debug!("OffsetCommit Request");
                if let RequestKind::OffsetCommit(ref req) =
                    RequestKind::OffsetCommit(OffsetCommitRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_offset_commit_request(&mut stream, &header, req, &*meta_store).await?;
                }
            }
            ApiKey::InitProducerId => {
                log::debug!("InitProducerId Request");
                if let RequestKind::InitProducerId(ref req) =
                    RequestKind::InitProducerId(InitProducerIdRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_init_producer_id_request(&mut stream, &header, req, &*meta_store).await?;
                }
            }
            ApiKey::OffsetFetch => {
                log::debug!("OffsetFetch Request");
                if let RequestKind::OffsetFetch(ref req) =
                    RequestKind::OffsetFetch(OffsetFetchRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_offset_fetch_request(&mut stream, &header, req, &*meta_store).await?;
                }
            },
            ApiKey::Fetch => {
                log::debug!("Fetch Request");
                if let RequestKind::Fetch(ref req) =
                    RequestKind::Fetch(FetchRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_fetch_request(&mut stream, &header, req, &*log_store, &*meta_store, &*index_store).await?;
                }
            },
            ApiKey::Produce => {
                log::debug!("Produce Request");
                if let RequestKind::Produce(ref req) =
                    RequestKind::Produce(ProduceRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_produce_request(&mut stream, &header, req, &cluster_config, &*meta_store, &*log_store, &*index_store).await?;
                }
            }
            ApiKey::CreateTopics => {
                log::debug!("CreateTopics Request");
                if let RequestKind::CreateTopics(ref req) =
                    RequestKind::CreateTopics(CreateTopicsRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_create_topics_request(&mut stream, &header, req, &*meta_store).await?;
                }
            }
            ApiKey::ConsumerGroupHeartbeat => {
                log::debug!("ConsumerGroupHeartbeat Request");
                if let RequestKind::ConsumerGroupHeartbeat(ref req) =
                    RequestKind::ConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_consumer_group_heartbeat_request(&mut stream, &header, req, &*meta_store).await?;
                }
            }
            ApiKey::Heartbeat =>  {
                log::debug!("Heartbeat Request");
                let _ = RequestKind::Heartbeat(HeartbeatRequest::decode(&mut buf, header.request_api_version).unwrap());
                handle_heartbeat_request(&mut stream, &header).await?;
            }
            _ => {
                log::error!("Unsupported API Key: {:?}", api_key);
                continue; // or return Err(...)
            }
        };
    }
}
