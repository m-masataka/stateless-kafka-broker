
use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::messages::{
    RequestHeader,
    leave_group_request::LeaveGroupRequest,
    leave_group_response::{LeaveGroupResponse, MemberResponse},
};

use crate::common::response::send_kafka_response;
use crate::traits::meta_store::MetaStore;


pub async fn handle_leave_group_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &LeaveGroupRequest,
    meta_store: &dyn MetaStore,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling LeaveGroupRequest API VERSION {}", header.request_api_version);
    log::debug!("LeaveGroupRequest: {:?}", request);
    let mut response = LeaveGroupResponse::default();
    if request.member_id.is_empty() {
        // if version 5-8, use members for leave
        log::error!("Member ID is empty in LeaveGroupRequest");
        response.members = request.members
            .iter()
            .map(|member| {
                let mut response_member = MemberResponse::default();
                response_member.member_id = member.member_id.clone();
                match meta_store.leave_group(request.group_id.as_str(), member.member_id.as_str()) {
                    Ok(()) => {
                        log::info!("Successfully left group: {}", request.group_id.as_str());
                        response_member.error_code = 0; // 0 means no error
                    },
                    Err(e) => {
                        log::error!("Failed to leave group {}: {:?}", request.group_id.as_str(), e);
                        response_member.error_code = kafka_protocol::error::ResponseError::UnknownServerError.code();
                    },
                }
                response_member
            })
            .collect();
    } else {
        log::info!("Member ID: {}", request.member_id);
        match meta_store.leave_group(request.group_id.as_str(), request.member_id.as_str()) {
            Ok(()) => {
                log::info!("Successfully left group: {}", request.group_id.as_str());
                response.error_code = 0; // 0 means no error
            },
            Err(e) => {
                log::error!("Failed to leave group {}: {:?}", request.group_id.as_str(), e);
                response.error_code = kafka_protocol::error::ResponseError::UnknownServerError.code();
            },
        }
    }
    response.throttle_time_ms = 0;

    // Check heartbeat for the group
    meta_store.check_heartbeat(request.group_id.as_str())?;
    
    log::debug!("LeaveGroupResponse: {:?}", response);
    send_kafka_response(stream, header, &response).await?;
    log::debug!("Handled LeaveGroupRequest successfully");
    Ok(())
}