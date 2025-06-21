use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::init_producer_id_response::{
    InitProducerIdResponse
};
use kafka_protocol::messages::init_producer_id_request::{
    InitProducerIdRequest
};
use kafka_protocol::messages::ProducerId;
use kafka_protocol::error::ResponseError::UnknownServerError;

use crate::common::response::send_kafka_response;
use crate::storage::meta_store_impl::MetaStoreImpl;
use crate::traits::meta_store::MetaStore;



pub async fn handle_init_producer_id_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    _request: &InitProducerIdRequest,
    meta_store: &MetaStoreImpl,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling InitProducerId {}", header.request_api_version);
    let response = match meta_store.gen_producer_id().await {
        Ok(producer_id) => {
            log::info!("Generated Producer ID: {}", producer_id);
            let mut res = InitProducerIdResponse::default();
            res.producer_id = ProducerId(producer_id);
            res.error_code = 0;
            res.producer_epoch = 0;
            res.throttle_time_ms = 0;
            res
        },
        Err(e) => {
            log::error!("Failed to generate Producer ID: {:?}", e);
            let mut res = InitProducerIdResponse::default();
            res.error_code = UnknownServerError.code();
            res
        }
    };

    log::debug!("InitProducerIdResponse: {:?}", response);
    send_kafka_response(stream, header, &response).await?;
    log::debug!("Sent InitProducerIdResponse");
    Ok(())
}