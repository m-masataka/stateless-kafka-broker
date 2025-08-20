use anyhow::Result;
use kafka_protocol::messages::{
    RequestHeader,
    leave_group_request::LeaveGroupRequest,
    leave_group_response::{LeaveGroupResponse, MemberResponse},
};

use crate::common::response::send_kafka_response;
use crate::traits::meta_store::MetaStore;
use crate::handler::context::HandlerContext;


pub async fn handle_leave_group_request(
    header: &RequestHeader,
    request: &LeaveGroupRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>>
{
    log::info!("Handling LeaveGroupRequest API VERSION {}", header.request_api_version);
    log::debug!("LeaveGroupRequest: {:?}", request);

    let meta_store = handler_ctx.meta_store.clone();
    let mut response = LeaveGroupResponse::default();
    if request.member_id.is_empty() {
        // if version 5-8, use members for leave
        log::warn!("Member ID is empty in LeaveGroupRequest");
        let mut member_responses = Vec::new();
        for member in &request.members {
            log::debug!("Leaving group for member: {}", member.member_id);
            let mut response_member = MemberResponse::default();
            response_member.member_id = member.member_id.clone();
            match meta_store.leave_group(request.group_id.as_str(), member.member_id.as_str()).await {
                Ok(()) => {
                    log::info!("Successfully left group: {}", request.group_id.as_str());
                    response_member.error_code = 0; // 0 means no error
                },
                Err(e) => {
                    log::error!("Failed to leave group {}: {:?}", request.group_id.as_str(), e);
                    response_member.error_code = kafka_protocol::error::ResponseError::UnknownServerError.code();
                },
            }
            member_responses.push(response_member);
        }
        response.error_code = 0;
        response.members = member_responses;
    } else {
        log::info!("Member ID: {}", request.member_id);
        match meta_store.leave_group(request.group_id.as_str(), request.member_id.as_str()).await {
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

    log::debug!("LeaveGroupResponse: {:?}", response);
    log::debug!("Handled LeaveGroupRequest successfully");
    send_kafka_response(header, &response).await
}