use tokio::io::AsyncWrite;
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

use crate::storage::meta_store_impl::MetaStoreImpl;
use crate::{common::{config::ClusterConfig, response::send_kafka_response, topic_partition::Topic}};
use crate::traits::meta_store::MetaStore;

pub async fn handle_metadata_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &MetadataRequest,
    cluster_config: &ClusterConfig,
    meta_store: &MetaStoreImpl,
) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling MetadataRequest API VERSION {}", header.request_api_version);
    log::debug!("MetadataRequest: {:?}", request);

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
            // すべてのトピックのメタデータを返す（meta_store.list_topics() など使う）
            meta_store.get_all_topics().await
                .unwrap_or_default()
                .into_iter()
                .map(|topic| to_metadata_response_topic(&topic, leader_id))
                .collect::<Vec<_>>()
        },
        Some(topics) if topics.is_empty() => {
            // Kafka の実装的には "all topics" 扱いされることもあるので同様に全件返してもOK
            meta_store.get_all_topics().await
                .unwrap_or_default()
                .into_iter()
                .map(|topic| to_metadata_response_topic(&topic, leader_id))
                .collect::<Vec<_>>()
        },
        Some(topics) => {
            // 指定されたトピックのみ返す
            let mut response_topics = Vec::new();
            for request_topic in topics.iter() {
                if let Some(name) = &request_topic.name {
                    log::debug!("Requesting metadata for topic: {}", name.as_str());
                    match meta_store.get_topic_info(Some(name), None).await {
                        Ok(Some(topic_info)) => {
                            response_topics.push(to_metadata_response_topic(&topic_info, leader_id));
                        }
                        Ok(None) => {
                            log::warn!("Topic {} not found", name.as_str());
                            let mut topic_response = MetadataResponseTopic::default();
                            topic_response.name = Some(name.clone());
                            topic_response.error_code = UnknownTopicOrPartition.code();
                            response_topics.push(topic_response);
                        }
                        Err(e) => {
                            log::error!("Failed to fetch topic {}: {}", name.as_str(), e);
                            let mut topic_response = MetadataResponseTopic::default();
                            topic_response.name = Some(name.clone());
                            topic_response.error_code = UnknownTopicOrPartition.code();
                            response_topics.push(topic_response);
                        }
                    }
                } else {
                    log::debug!("If request_topic.name is None, it should not happen in practice.");
                }
            }
            response_topics
        }
    };

    send_kafka_response(stream, header, &response).await?;
    log::debug!("MetadataResponse: {:?}", response);
    log::debug!("Sent MetadataResponse");
    Ok(())
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
            partition_response.isr_nodes = vec![];
            partition_response.error_code = 0;
            partition_response
        })
        .collect();

    topic_response
}