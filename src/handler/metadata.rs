use anyhow::Result;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponseTopic, MetadataResponsePartition
};
use kafka_protocol::messages::metadata_request::{
    MetadataRequest,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::BrokerId;
use kafka_protocol::error::ResponseError::UnknownTopicOrPartition;

use crate::{
    common::{
        response::send_kafka_response,
        topic_partition::Topic
    }
};
use crate::traits::meta_store::MetaStore;
use crate::handler::context::HandlerContext;

pub async fn handle_metadata_request(
    header: &RequestHeader,
    request: &MetadataRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>>
{
    log::info!("Handling MetadataRequest API VERSION {}", header.request_api_version);
    log::debug!("MetadataRequest: {:?}", request);

    let meta_store = handler_ctx.meta_store.clone();
    let cluster_config = handler_ctx.cluster_config.clone();
    let mut response = MetadataResponse::default();
    let brokers = cluster_config
        .brokers
        .iter()
        .map(|b| {
            let mut broker = MetadataResponseBroker::default();
            broker.node_id = BrokerId(b.node_id);
            broker.host = StrBytes::from_string(b.host.clone());
            broker.port = b.port;
            broker
        })
        .collect::<Vec<_>>();
    response.brokers = brokers;
    response.cluster_id = Some(cluster_config.cluster_id.clone().into());
    response.controller_id = BrokerId(cluster_config.controller_id);
    response.throttle_time_ms = 0;

    let leader_id = cluster_config.controller_id;


    response.topics = match &request.topics {
        None => {
            // get all topics
            meta_store.get_topics().await
                .unwrap_or_default()
                .into_iter()
                .map(|topic| to_metadata_response_topic(&topic, leader_id))
                .collect::<Vec<_>>()
        },
        Some(topics) if topics.is_empty() => {
            // get all topics
            meta_store.get_topics().await
                .unwrap_or_default()
                .into_iter()
                .map(|topic| to_metadata_response_topic(&topic, leader_id))
                .collect::<Vec<_>>()
        },
        Some(topics) => {
            // Received specific topics
            let mut response_topics = Vec::new();
            for request_topic in topics.iter() {
                if let Some(name) = &request_topic.name {
                    log::debug!("Requesting metadata for topic: {}", name.as_str());
                    let topic_id = meta_store.get_topic_id_by_topic_name(name.as_str()).await?;
                    if let Some(id) = topic_id {
                        match meta_store.get_topic(&id).await {
                            Ok(topic_info) => {
                                response_topics.push(to_metadata_response_topic(&topic_info, leader_id));
                            }
                            Err(e) => {
                                log::error!("Failed to get topic {}: {}", name.as_str(), e);
                                let mut topic_response = MetadataResponseTopic::default();
                                topic_response.name = Some(name.clone());
                                topic_response.error_code = UnknownTopicOrPartition.code();
                                response_topics.push(topic_response);
                            }
                        }
                        continue;
                    } else {
                        log::warn!("Topic {} not found", name.as_str());
                        let mut topic_response = MetadataResponseTopic::default();
                        topic_response.name = Some(name.clone());
                        topic_response.error_code = UnknownTopicOrPartition.code();
                        response_topics.push(topic_response); 
                    }
                } else {
                    log::debug!("If request_topic.name is None, it should not happen in practice.");
                }
            }
            response_topics
        }
    };

    log::debug!("MetadataResponse: {:?}", response);
    log::debug!("Sent MetadataResponse");
    send_kafka_response(header, &response).await
}

pub fn to_metadata_response_topic(topic: &Topic, leader_id: i32) -> MetadataResponseTopic {
    let mut topic_response = MetadataResponseTopic::default();
    topic_response.name = topic.name.clone().map(|s| TopicName::from(StrBytes::from(s)));
    topic_response.is_internal = topic.is_internal;
    topic_response.topic_id = topic.topic_id;
    topic_response.error_code = 0;

    topic_response.partitions = (0..topic.num_partitions)
        .map(|i| {
            let mut partition_response = MetadataResponsePartition::default();
            partition_response.partition_index = i;
            partition_response.leader_id = BrokerId(leader_id);
            partition_response.replica_nodes = vec![BrokerId(leader_id)]; // TODO: Implement proper replica handling
            partition_response.isr_nodes = vec![BrokerId(leader_id)]; // TODO: Implement proper ISR handling;
            partition_response.leader_epoch = 0;
            partition_response.error_code = 0;
            partition_response
        })
        .collect();

    topic_response
}