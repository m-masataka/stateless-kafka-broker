use anyhow::Result;
use kafka_protocol::error::ResponseError::UnknownServerError;
use kafka_protocol::messages::ProducerId;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use kafka_protocol::messages::init_producer_id_response::InitProducerIdResponse;

use crate::common::response::send_kafka_response;
use crate::handler::context::HandlerContext;
use crate::traits::meta_store::MetaStore;

pub async fn handle_init_producer_id_request(
    header: &RequestHeader,
    _request: &InitProducerIdRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>> {
    log::info!("Handling InitProducerId {}", header.request_api_version);

    let meta_store = handler_ctx.meta_store.clone();
    let response = match meta_store.gen_producer_id().await {
        Ok(producer_id) => {
            log::info!("Generated Producer ID: {}", producer_id);
            let mut res = InitProducerIdResponse::default();
            res.producer_id = ProducerId(producer_id);
            res.error_code = 0;
            res.producer_epoch = 0;
            res.throttle_time_ms = 0;
            res
        }
        Err(e) => {
            log::error!("Failed to generate Producer ID: {:?}", e);
            let mut res = InitProducerIdResponse::default();
            res.error_code = UnknownServerError.code();
            res
        }
    };

    log::debug!("InitProducerIdResponse: {:?}", response);
    log::debug!("Sent InitProducerIdResponse");
    send_kafka_response(header, &response).await
}
