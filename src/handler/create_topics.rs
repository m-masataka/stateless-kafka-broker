use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::{
    messages::{
        RequestHeader,
        create_topics_request::CreateTopicsRequest,
        create_topics_response::CreateTopicsResponse,
        create_topics_response::CreatableTopicResult,
    },
};
use uuid::Uuid;
use crate::{storage::meta_store_impl::MetaStoreImpl, traits::meta_store::MetaStore};
use crate::common::topic_partition::Topic;
use crate::common::response::send_kafka_response;

pub async fn handle_create_topics_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &CreateTopicsRequest,
    meta_store: &MetaStoreImpl,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling CreateTopicsRequest API VERSION {}", header.request_api_version);
    log::debug!("CreateTopicsRequest: {:?}", request);

    let mut response = CreateTopicsResponse::default();
    response.throttle_time_ms = 0;
    let mut response_topics = Vec::new();
    for topic in &request.topics {
        let mut topic_result = CreatableTopicResult::default();
        match meta_store.get_topic_info(Some(topic.name.as_ref()), None).await {
            Ok(Some(store_topic)) => {
                log::info!("Topic already exists: {}", store_topic.name.as_ref().unwrap());
                topic_result.name = topic.name.clone();
                topic_result.topic_id = store_topic.topic_id;
                topic_result.num_partitions = topic.num_partitions;
                topic_result.replication_factor = topic.replication_factor;
                topic_result.error_code = 0; // 0 means no error
                // TODO: set topic config
                let topic_metadata = Topic {
                    name: Some(topic.name.clone().to_string()),
                    topic_id: store_topic.topic_id,
                    is_internal: false,
                    num_partitions: topic.num_partitions,
                    replication_factor: topic.replication_factor,
                    partitions: None,
                    topic_authorized_operations: None, 
                };
                meta_store.save_topic_partition_info(&topic_metadata).await?;
            },
            Ok(None) => {
                log::info!("Creating new topic: {}", topic.name.as_str());
                let new_id = Uuid::new_v4();
                topic_result.name = topic.name.clone();
                topic_result.error_code = 0; // 0 means no error
                topic_result.topic_id = new_id; // topic_id is not used in this version

                let topic_metadata = Topic {
                    name: Some(topic.name.clone().to_string()),
                    topic_id: new_id,
                    is_internal: false,
                    num_partitions: topic.num_partitions,
                    replication_factor: topic.replication_factor,
                    partitions: None,
                    topic_authorized_operations: None, 
                };
                meta_store.save_topic_partition_info(&topic_metadata).await?;
            },
            Err(e) => {
                log::error!("Error checking topic existence: {}", e);
                topic_result.error_code = 1; // Unknown error code
                topic_result.topic_id = Uuid::new_v4(); // topic_id is not used in this version
            }
        }
        response_topics.push(topic_result);
    }
    response.topics = response_topics;
    log::debug!("CreateTopicsResponse: {:?}", response);
    send_kafka_response(stream, header, &response).await?;
    log::debug!("Sent CreateTopicsResponse");
    Ok(())
}
