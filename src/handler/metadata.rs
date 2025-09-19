use std::hash::{DefaultHasher, Hash, Hasher};

use anyhow::Result;
use kafka_protocol::error::ResponseError::UnknownTopicOrPartition;
use kafka_protocol::messages::BrokerId;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::protocol::StrBytes;

use crate::common::{cluster::Node, response::send_kafka_response, topic_partition::Topic};
use crate::handler::context::HandlerContext;
use crate::traits::meta_store::MetaStore;

pub async fn handle_metadata_request(
    header: &RequestHeader,
    request: &MetadataRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>> {
    log::info!(
        "Handling MetadataRequest API VERSION {}",
        header.request_api_version
    );
    log::debug!("MetadataRequest: {:?}", request);

    let meta_store = handler_ctx.meta_store.clone();
    let node_config = handler_ctx.node_config.clone();
    let mut response = MetadataResponse::default();
    let brokers = match meta_store.get_cluster_status().await {
        Ok(brokers) => brokers,
        Err(e) => {
            log::error!("Failed to get cluster status: {}", e);
            vec![]
        }
    };
    let response_brokers = brokers
        .iter()
        .map(|b| {
            let mut broker = MetadataResponseBroker::default();
            broker.node_id = BrokerId(b.node_id);
            broker.host = b.host.clone().into();
            broker.port = b.port;
            broker
        })
        .collect::<Vec<_>>();
    response.brokers = response_brokers;
    response.cluster_id = Some(node_config.cluster_id.clone().into());
    response.controller_id = BrokerId(node_config.controller_id);
    response.throttle_time_ms = 0;

    // let leader_id = node_config.controller_id;

    response.topics = match &request.topics {
        None => {
            // get all topics
            meta_store
                .get_topics()
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|topic| to_metadata_response_topic(&topic, brokers.clone()))
                .collect::<Vec<_>>()
        }
        Some(topics) if topics.is_empty() => {
            // get all topics
            meta_store
                .get_topics()
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|topic| to_metadata_response_topic(&topic, brokers.clone()))
                .collect::<Vec<_>>()
        }
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
                                response_topics
                                    .push(to_metadata_response_topic(&topic_info, brokers.clone()));
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

pub fn to_metadata_response_topic(topic: &Topic, brokers: Vec<Node>) -> MetadataResponseTopic {
    let mut topic_response = MetadataResponseTopic::default();
    topic_response.name = topic
        .name
        .clone()
        .map(|s| TopicName::from(StrBytes::from(s)));
    topic_response.is_internal = topic.is_internal;
    topic_response.topic_id = topic.topic_id;
    topic_response.error_code = 0;

    let n = brokers.len();
    let start = topic_hash_seed(topic, n);

    topic_response.partitions = (0..topic.num_partitions)
        .map(|i| {
            // Simple round-robin leader assignment
            let leader_idx = (start + (i as usize)) % n;
            let leader_id = brokers[leader_idx].node_id;
            let mut partition_response = MetadataResponsePartition::default();
            partition_response.partition_index = i;
            partition_response.leader_id = BrokerId(leader_id);
            partition_response.replica_nodes =
                brokers.iter().map(|b| BrokerId(b.node_id)).collect();
            partition_response.isr_nodes = brokers.iter().map(|b| BrokerId(b.node_id)).collect();
            partition_response.leader_epoch = 0;
            partition_response.error_code = 0;
            partition_response
        })
        .collect();

    topic_response
}

fn topic_hash_seed(topic: &Topic, n: usize) -> usize {
    let mut h = DefaultHasher::new();
    // use topic name if available, otherwise use topic_id
    if let Some(ref name) = topic.name {
        name.hash(&mut h);
    } else {
        topic.topic_id.hash(&mut h);
    }
    (h.finish() as usize) % n
}
