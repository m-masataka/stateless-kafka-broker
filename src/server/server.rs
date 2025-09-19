use crate::server::loader::{load_index_store, load_log_store, load_meta_store};

use crate::handler::{
    context::HandlerContext,
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
use crate::common::config::{load_node_config, load_server_config};
use crate::common::cluster::Node;
use crate::common::utils::jittered_delay;
use crate::storage::log_store_impl::LogStoreImpl;
use crate::storage::meta_store_impl::MetaStoreImpl;
use crate::storage::index_store_impl::IndexStoreImpl;
use crate::server::cluster_heartbeat::send_heartbeat;
use anyhow::{Ok, Result};
use nix::sys::socket::{setsockopt, sockopt};
use tokio::{
    net::TcpStream,
    io::AsyncReadExt,
    time::{self, Duration},
};
use std::sync::Arc;

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
use tokio::io::AsyncWriteExt;

pub async fn server_start(config_path: &str) -> anyhow::Result<()> {
    env_logger::init();
    log::info!("Starting Kafka-compatible server...");
    let node_conf_load = Arc::new(load_node_config(config_path)?);
    let server_config_load = Arc::new(load_server_config()?);

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", server_config_load.host, server_config_load.port)).await?;
    log::info!("Kafka-compatible server listening on port {}:{}", server_config_load.host, server_config_load.port);

    let meta_store = Arc::new(load_meta_store(&server_config_load).await.unwrap());
    let log_store = Arc::new(load_log_store(&server_config_load).await.unwrap());
    let index_store = Arc::new(load_index_store(&server_config_load).await.unwrap());

    // Start cluster heartbeat task
    log::info!("A: before spawning heartbeat");
    {
        let meta_store = meta_store.clone();
        let node_config =  node_conf_load.clone();
        tokio::spawn(async move {
            // Update cluster status immediately on startup
            if let Err(e) = send_heartbeat(&node_config, meta_store.clone()).await {
                log::warn!("heartbeat failed: {:?}", e);
            }
            // Then start periodic updates
            let jitter = jittered_delay(10); // 10 seconds base
            let mut ticker = time::interval(Duration::from_secs(jitter));
            loop {
                ticker.tick().await;
                if let Err(e) = send_heartbeat(&node_config, meta_store.clone()).await {
                    log::warn!("heartbeat failed: {:?}", e);
                }
            }
        });
    }
    log::info!("B: after spawning heartbeat, entering accept loop");
    
    loop {
        let (stream, addr) = listener.accept().await?;

        let std_stream = stream.into_std()?;
        // Set socket options
        if let Some(send_buf) = server_config_load.tcp_send_buffer_bytes {
            setsockopt(&std_stream, sockopt::SndBuf, &send_buf)
                .map_err(|e| anyhow::anyhow!("Failed to set SO_SNDBUF: {:?}", e))?;
        }

        if let Some(recv_buf) = server_config_load.tcp_recv_buffer_bytes {
            setsockopt(&std_stream, sockopt::RcvBuf, &recv_buf)
                .map_err(|e| anyhow::anyhow!("Failed to set SO_RCVBUF: {:?}", e))?;
        }

        if server_config_load.tcp_nodelay.unwrap_or(true) {
            std_stream
                .set_nodelay(true)
                .map_err(|e| anyhow::anyhow!("Failed to set TCP_NODELAY: {:?}", e))?;
        }

        // Convert std::net::TcpStream back to tokio::net::TcpStream
        let stream = TcpStream::from_std(std_stream)?;

        if let Err(e) = stream.set_nodelay(true) {
            log::warn!("Failed to set TCP_NODELAY: {:?}", e);
        }
        log::info!("Accepted connection from {:?}", addr);
        let node_config = node_conf_load.clone();
        let meta_store = meta_store.clone();
        let log_store = log_store.clone();
        let index_store = index_store.clone();

        // Spawn a new task for each connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(
                stream,
                node_config,
                meta_store,
                log_store,
                index_store,
            ).await {
                log::error!("Connection error: {:?}", e);
            }
        });
    }
}

async fn handle_connection(stream: tokio::net::TcpStream, 
    node_config: Arc<Node>,
    meta_store: Arc<MetaStoreImpl>,
    log_store: Arc<LogStoreImpl>,
    index_store: Arc<IndexStoreImpl>,
) -> Result<()> {
    // spawn writer task
    let (mut reader, mut writer) = tokio::io::split(stream);
    loop {
        // Read length prefix
        let mut length_buf = [0u8; 4];
        if let Err(e) = reader.read_exact(&mut length_buf).await {
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
        reader.read_exact(&mut buffer).await?;

        let mut peek = std::io::Cursor::new(&buffer);
        let api_key = AsyncReadExt::read_i16(&mut peek).await.unwrap();
        let api_version = AsyncReadExt::read_i16(&mut peek).await.unwrap();
        let header_version = ApiKey::try_from(api_key).unwrap().request_header_version(api_version);

        let mut buf = std::io::Cursor::new(&buffer);
        let header = RequestHeader::decode(&mut buf, header_version).unwrap();
        let api_key = ApiKey::try_from(header.request_api_key).unwrap();
        log::debug!(
            "api_key {:?}, api_version {}, header_version {}, correlation_id {:?}",
            api_key, api_version, header_version, header.correlation_id
        );

        let handler_ctx = HandlerContext {
            log_store: log_store.clone(),
            meta_store: meta_store.clone(),
            index_store: index_store.clone(),
            node_config: node_config.clone(),
        };

        let response = match api_key {
            ApiKey::ApiVersions => {
                log::debug!("ApiVersion Request");
                if let RequestKind::ApiVersions(ref req) =
                    RequestKind::ApiVersions(ApiVersionsRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_api_versions_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode ApiVersionsRequest");
                    Err(anyhow::anyhow!("Failed to decode ApiVersionsRequest"))
                }
            }
            ApiKey::Metadata => {
                log::debug!("Metadata Request");
                if let RequestKind::Metadata(ref req) =
                    RequestKind::Metadata(MetadataRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_metadata_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode MetadataRequest");
                    Err(anyhow::anyhow!("Failed to decode MetadataRequest"))
                }
            }
            ApiKey::DeleteTopics => {
                log::debug!("DeleteTopics Request");
                if let RequestKind::DeleteTopics(ref req) =
                    RequestKind::DeleteTopics(DeleteTopicsRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_delete_topics_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode DeleteTopicsRequest");
                    Err(anyhow::anyhow!("Failed to decode DeleteTopicsRequest"))
                }
            }
            ApiKey::FindCoordinator => {
                log::debug!("FindCoordinator Request");
                if let RequestKind::FindCoordinator(ref req) =
                    RequestKind::FindCoordinator(FindCoordinatorRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_find_coordinator_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode FindCoordinatorRequest");
                    Err(anyhow::anyhow!("Failed to decode FindCoordinatorRequest"))
                }
            }
            ApiKey::JoinGroup => {
                log::debug!("JoinGroup Request");
                if let RequestKind::JoinGroup(ref req) =
                    RequestKind::JoinGroup(JoinGroupRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_join_group_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode JoinGroupRequest");
                    Err(anyhow::anyhow!("Failed to decode JoinGroupRequest"))
                }
            }
            ApiKey::LeaveGroup => {
                log::debug!("LeaveGroup Request");
                if let RequestKind::LeaveGroup(ref req) =
                    RequestKind::LeaveGroup(LeaveGroupRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_leave_group_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode LeaveGroupRequest");
                    Err(anyhow::anyhow!("Failed to decode LeaveGroupRequest"))
                }
            }
            ApiKey::SyncGroup => {
                log::debug!("SyncGroup Request");
                if let RequestKind::SyncGroup(ref req) =
                    RequestKind::SyncGroup(SyncGroupRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_sync_group_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode SyncGroupRequest");
                    Err(anyhow::anyhow!("Failed to decode SyncGroupRequest"))
                }
            }
            ApiKey::OffsetCommit => {
                log::debug!("OffsetCommit Request");
                if let RequestKind::OffsetCommit(ref req) =
                    RequestKind::OffsetCommit(OffsetCommitRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_offset_commit_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode OffsetCommitRequest");
                    Err(anyhow::anyhow!("Failed to decode OffsetCommitRequest"))
                }
            }
            ApiKey::InitProducerId => {
                log::debug!("InitProducerId Request");
                if let RequestKind::InitProducerId(ref req) =
                    RequestKind::InitProducerId(InitProducerIdRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_init_producer_id_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode InitProducerIdRequest");
                    Err(anyhow::anyhow!("Failed to decode InitProducerIdRequest"))
                }
            }
            ApiKey::OffsetFetch => {
                log::debug!("OffsetFetch Request");
                if let RequestKind::OffsetFetch(ref req) =
                    RequestKind::OffsetFetch(OffsetFetchRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_offset_fetch_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode OffsetFetchRequest");
                    Err(anyhow::anyhow!("Failed to decode OffsetFetchRequest"))
                }
            },
            ApiKey::Fetch => {
                log::debug!("Fetch Request");
                if let RequestKind::Fetch(ref req) =
                    RequestKind::Fetch(FetchRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_fetch_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode FetchRequest");
                    Err(anyhow::anyhow!("Failed to decode FetchRequest"))
                }
            },
            ApiKey::Produce => {
                log::debug!("Produce Request");
                if let RequestKind::Produce(ref req) =
                    RequestKind::Produce(ProduceRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_produce_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode ProduceRequest");
                    Err(anyhow::anyhow!("Failed to decode ProduceRequest"))
                }
            }
            ApiKey::CreateTopics => {
                log::debug!("CreateTopics Request");
                if let RequestKind::CreateTopics(ref req) =
                    RequestKind::CreateTopics(CreateTopicsRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_create_topics_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode CreateTopicsRequest");
                    Err(anyhow::anyhow!("Failed to decode CreateTopicsRequest"))
                }
            }
            ApiKey::ConsumerGroupHeartbeat => {
                log::debug!("ConsumerGroupHeartbeat Request");
                if let RequestKind::ConsumerGroupHeartbeat(ref req) =
                    RequestKind::ConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_consumer_group_heartbeat_request(&header, req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode ConsumerGroupHeartbeatRequest");
                    Err(anyhow::anyhow!("Failed to decode ConsumerGroupHeartbeatRequest"))
                }
            }
            ApiKey::Heartbeat =>  {
                log::debug!("Heartbeat Request");
                if let RequestKind::Heartbeat(ref req) =
                    RequestKind::Heartbeat(HeartbeatRequest::decode(&mut buf, header.request_api_version).unwrap())
                {
                    handle_heartbeat_request(&header,req, &handler_ctx).await
                } else {
                    log::error!("Failed to decode HeartbeatRequest");
                    Err(anyhow::anyhow!("Failed to decode HeartbeatRequest"))
                }
            }
            _ => {
                log::error!("Unsupported API Key: {:?}", api_key);
                Err(anyhow::anyhow!("Unsupported API Key: {:?}", api_key))
            }
        };
        match response {
            std::result::Result::Ok(bytes) => {
                log::debug!("Response length: {}", bytes.len());

                if bytes.len() >= 8 {
                    let total_len2 = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    let cid = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                    let tag = *bytes.get(8).unwrap_or(&0);
                    log::debug!(
                        "TX len_total={}, correlation_id={}, tag=0x{:02X}",
                        total_len2, cid, tag
                    );
                }

                writer.write_all(&bytes).await?;
                writer.flush().await?;
                log::debug!("Response sent successfully");
            }
            Err(e) => {
                log::error!("Failed to handle request for api_key {:?}: {:?}", api_key, e);
            }
        }
    }
}
