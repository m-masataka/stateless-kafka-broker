use std::io::{Cursor, Read};
use anyhow::Context;
use futures::future::BoxFuture;
use tracing::debug;

// Local alias to the user's context type.

// --- Kafka protocol imports ---
use kafka_protocol::protocol::Decodable;
use kafka_protocol::messages::RequestHeader;

// Bring request message types that your handlers expect.
// NOTE: If your crate uses different paths or names, adjust the imports below.
use kafka_protocol::messages::{
    ApiKey,
    ApiVersionsRequest,
    MetadataRequest,
    FetchRequest,
    ProduceRequest,
    InitProducerIdRequest,
    ConsumerGroupHeartbeatRequest,
    FindCoordinatorRequest,
    JoinGroupRequest,
    HeartbeatRequest,
    SyncGroupRequest,
    LeaveGroupRequest,
    OffsetCommitRequest,
    OffsetFetchRequest,
    CreateTopicsRequest,
    DeleteTopicsRequest,
};

use crate::handler::{
    api_versions::handle_api_versions_request,
    consumer_group_heartbeat::handle_consumer_group_heartbeat_request, context::HandlerContext,
    create_topics::handle_create_topics_request, delete_topics::handle_delete_topics_request,
    fetch::handle_fetch_request, find_coordinator::handle_find_coordinator_request,
    heartbeat::handle_heartbeat_request, init_producer_id::handle_init_producer_id_request,
    join_group::handle_join_group_request, leave_group::handle_leave_group_request,
    metadata::handle_metadata_request, offset_commit::handle_offset_commit_request,
    offset_fetch::handle_offset_fetch_request, produce::handle_produce_request,
    sync_group::handle_sync_group_request,
};

use std::result::Result::Ok as StdOk;

// Decode a concrete Kafka request type and forward it to its handler.
// The handler returns a BoxFuture so it can borrow the request/header safely across lifetimes.
async fn decode_and_call<Req, F>(
    buf: &mut std::io::Cursor<Vec<u8>>,
    header: &kafka_protocol::messages::RequestHeader,
    handler_ctx: &HandlerContext,
    name: &'static str,
    handler: F,
) -> anyhow::Result<Vec<u8>>
where
    Req: kafka_protocol::protocol::Decodable + core::fmt::Debug,
    // HRTB: the handler must work for any lifetimes of the borrowed inputs
    F: for<'a> Fn(
        &'a kafka_protocol::messages::RequestHeader,
        &'a Req,
        &'a HandlerContext,
    ) -> BoxFuture<'a, anyhow::Result<Vec<u8>>>,
{
    let req = match Req::decode(buf, header.request_api_version) {
        StdOk(req) => req,
        Err(e) => {
            log::error!("Failed to decode {}: {:?}", name, e);
            return Err(anyhow::anyhow!("Failed to decode {}", name));
        }
    };

    // The future can borrow `header`, `req`, and `handler_ctx` safely.
    handler(header, &req, handler_ctx).await
}

macro_rules! call {
    ($req_ty:ty, $name:literal, $handler:path, $buf:expr, $hdr:expr, $ctx:expr) => {
        decode_and_call::<$req_ty, _>(
            $buf, $hdr, $ctx, $name, |h, r, c| Box::pin($handler(h, r, c))
        ).await
    };
}

/// Attempt to peek the correlation id from a raw Kafka frame.
/// This is "best-effort" and must never panic.
pub fn peek_correlation_id(frame: &[u8]) -> Option<i32> {
    // We need to interpret the first 2*i16 to get api_key / api_version,
    // then map to header_version and decode RequestHeader to read correlation_id.
    fn read_i16(cur: &mut Cursor<&[u8]>) -> Option<i16> {
        let mut b = [0u8; 2];
        cur.read_exact(&mut b).ok()?;
        Some(i16::from_be_bytes(b))
    }
    let mut cur = Cursor::new(frame);
    let api_key = read_i16(&mut cur)?;
    let api_version = read_i16(&mut cur)?;
    let api_key = ApiKey::try_from(api_key).ok()?;
    let header_version = api_key.request_header_version(api_version);

    // Rewind to start and decode header
    let mut cur2 = Cursor::new(frame);
    let header = RequestHeader::decode(&mut cur2, header_version).ok()?;
    Some(header.correlation_id)
}

/// Main entry point: decode the frame and dispatch to the appropriate handler.
pub async fn dispatch_frame(frame: bytes::Bytes, handler_ctx: &HandlerContext) -> anyhow::Result<Vec<u8>> {
    // We need the header_version, which depends on (api_key, api_version).
    // Peek them first, then rewind and decode the header.
    let frame_vec = frame.to_vec();
    let mut peek = std::io::Cursor::new(frame_vec.as_slice());
    let api_key_raw = {
        let mut b = [0u8; 2];
        peek.read_exact(&mut b).context("short frame (api_key)")?;
        i16::from_be_bytes(b)
    };
    let api_version_raw = {
        let mut b = [0u8; 2];
        peek.read_exact(&mut b).context("short frame (api_version)")?;
        i16::from_be_bytes(b)
    };
    let api_key = ApiKey::try_from(api_key_raw).map_err(|_| anyhow::anyhow!("unknown ApiKey"))?;
    let header_version = api_key.request_header_version(api_version_raw);

    // Now decode the header with the correct version.
    let mut buf = Cursor::new(frame_vec);
    let header = RequestHeader::decode(&mut buf, header_version).context("failed to decode RequestHeader")?;

    debug!(?api_key, api_version = header.request_api_version, correlation_id = header.correlation_id, "decoded header");
    let out = match api_key {
        ApiKey::ApiVersions => call!(ApiVersionsRequest, "ApiVersionsRequest",
                handle_api_versions_request, &mut buf, &header, &handler_ctx),
        ApiKey::Metadata => call!(MetadataRequest, "MetadataRequest",
                handle_metadata_request, &mut buf, &header, &handler_ctx),
        ApiKey::DeleteTopics => call!(DeleteTopicsRequest, "DeleteTopicsRequest",
                handle_delete_topics_request, &mut buf, &header, &handler_ctx),
        ApiKey::FindCoordinator => call!(FindCoordinatorRequest, "FindCoordinatorRequest",
                handle_find_coordinator_request, &mut buf, &header, &handler_ctx),
        ApiKey::JoinGroup => call!(JoinGroupRequest, "JoinGroupRequest",
                handle_join_group_request, &mut buf, &header, &handler_ctx),
        ApiKey::LeaveGroup => call!(LeaveGroupRequest, "LeaveGroupRequest",
                handle_leave_group_request, &mut buf, &header, &handler_ctx),
        ApiKey::SyncGroup => call!(SyncGroupRequest, "SyncGroupRequest",
                handle_sync_group_request, &mut buf, &header, &handler_ctx),
        ApiKey::OffsetCommit => call!(OffsetCommitRequest, "OffsetCommitRequest",
                handle_offset_commit_request, &mut buf, &header, &handler_ctx),
        ApiKey::InitProducerId => call!(InitProducerIdRequest, "InitProducerIdRequest",
                handle_init_producer_id_request, &mut buf, &header, &handler_ctx),
        ApiKey::OffsetFetch => call!(OffsetFetchRequest, "OffsetFetchRequest",
                handle_offset_fetch_request, &mut buf, &header, &handler_ctx),
        ApiKey::Fetch => call!(FetchRequest, "FetchRequest",
                handle_fetch_request, &mut buf, &header, &handler_ctx),
        ApiKey::Produce => call!(ProduceRequest, "ProduceRequest",
                handle_produce_request, &mut buf, &header, &handler_ctx),
        ApiKey::CreateTopics => call!(CreateTopicsRequest, "CreateTopicsRequest",
                handle_create_topics_request, &mut buf, &header, &handler_ctx),
        ApiKey::ConsumerGroupHeartbeat => call!(ConsumerGroupHeartbeatRequest, "ConsumerGroupHeartbeatRequest",
                handle_consumer_group_heartbeat_request, &mut buf, &header, &handler_ctx),
        ApiKey::Heartbeat => call!(HeartbeatRequest, "HeartbeatRequest",
                handle_heartbeat_request, &mut buf, &header, &handler_ctx),
        _ => {
            log::error!("Unsupported API Key: {:?}", api_key);
            Err(anyhow::anyhow!("Unsupported API Key: {:?}", api_key))
        }
    }?;
    Ok(out)
}
