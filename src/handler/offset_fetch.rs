use tokio::io::AsyncWrite;
use anyhow::Result;
use kafka_protocol::messages::{
    offset_fetch_request::{
        OffsetFetchRequest,
    },
    offset_fetch_response::{
        OffsetFetchResponse,
        OffsetFetchResponseTopic,
        OffsetFetchResponseTopics,
        OffsetFetchResponsePartition,
        OffsetFetchResponsePartitions,
        OffsetFetchResponseGroup,
    },
    RequestHeader,
};
use kafka_protocol::protocol::StrBytes;
use crate::common::response::{send_kafka_response};
use crate::traits::meta_store::MetaStore;


pub async fn handle_offset_fetch_request<W>(
    stream: &mut W,
    header: &RequestHeader,
    request: &OffsetFetchRequest,
    meta_store: &dyn MetaStore,
) -> Result<()> 
where
    W: AsyncWrite + Unpin + Send,
{
    log::info!("Handling OffsetFetchRequest API VERSION {}", header.request_api_version);
    log::debug!("OffsetFetchRequest: {:?}", request);

    // グループIDとメンバーIDを取得
    let response: OffsetFetchResponse = if request.group_id.is_empty() {
        // version 8-9 では group_id では groupsのみで判定
        log::error!("Group ID is empty in OffsetFetchRequest");
        let mut res = OffsetFetchResponse::default();
        res.groups = request.groups 
            .iter()
            .map(|group|{
                match meta_store.get_consumer_group(group.group_id.as_str()) {
                    Ok(Some(store)) => {
                        log::info!("Found existing consumer group");
                        let mut  response_group = OffsetFetchResponseGroup::default();
                        response_group.group_id = group.group_id.clone();
                        response_group.error_code = 0;
                        response_group.topics = if let Some(topics) = group.topics.as_ref() {
                            topics
                            .iter()
                            .map(|topic| {
                                let mut response_topics = OffsetFetchResponseTopics::default();
                                response_topics.name = topic.name.clone();
                                response_topics.partitions = topic.partition_indexes
                                    .iter()
                                    .map(|partition_index| {
                                        let mut response_partition = OffsetFetchResponsePartitions::default();
                                        response_partition.partition_index = *partition_index;
                                        if let Some(partition_store) = store.get_partition_by_topic_and_index(topic.name.as_str(), *partition_index) {
                                            log::info!("Found partition: {} for topic: {}", partition_index, topic.name.as_str());
        
                                            response_partition.partition_index = partition_store.partition_index;
                                            response_partition.committed_offset = partition_store.committed_offset;
                                            response_partition.metadata = Some(StrBytes::from_string(
                                                partition_store
                                                .metadata.clone()
                                                .map(|b| String::from_utf8_lossy(&b).to_string()).unwrap_or_default()));
                                            response_partition.error_code = 0; // 成功
                                        } else {
                                            log::warn!("Partition {} not found for topic: {}", partition_index, topic.name.as_str());
                                            response_partition.committed_offset = 0; // オフセットが見つからない場合は0
                                            response_partition.error_code = 0;
                                        }
                                        response_partition
                                    })
                                    .collect();
                                response_topics
                            })
                            .collect()
                        } else {
                            vec![]
                        };
                        response_group
                    },
                    Ok(None) => {
                        // group not found
                        OffsetFetchResponseGroup::default()
                    },
                    Err(e) => {
                        // ここでエラーを propagate できないなら panic or unwrap
                        panic!("Failed to get consumer group: {}", e);
                    }
                }
            })
            .collect();
        res
    } else {
        let group_id = request.group_id.clone();
        if let Some(consumer_group_store) = meta_store.get_consumer_group(group_id.as_str())? {
            log::info!("Found existing consumer group: {}", *group_id.clone());
            let mut res = OffsetFetchResponse::default();
            res.topics = if let Some(request_topics) = &request.topics {
                let res_topics = request_topics
                    .iter()
                    .map(|request_topic| {
                        let mut response_topic = OffsetFetchResponseTopic::default();
                        response_topic.name = request_topic.name.clone();
                        // 各トピックのパーティションを取得
                        response_topic.partitions = request_topic
                            .partition_indexes
                            .iter()
                            .map(|partition_index| {
                                if let Some(partition_store) = consumer_group_store.get_partition_by_topic_and_index(request_topic.name.as_str(), *partition_index) {
                                    log::info!("Found partition: {} for topic: {}", partition_index, request_topic.name.as_str());
                                    let mut response_partition = OffsetFetchResponsePartition::default();
                                    response_partition.partition_index = partition_store.partition_index;
                                    response_partition.committed_offset = partition_store.committed_offset;
                                    response_partition.metadata = Some(StrBytes::from_string(
                                        partition_store
                                        .metadata.clone()
                                        .map(|b| String::from_utf8_lossy(&b).to_string()).unwrap_or_default()));
                                    response_partition.error_code = 0; // 成功
                                    response_partition
                                } else {
                                    log::warn!("Partition {} not found for topic: {}", partition_index, request_topic.name.as_str());
                                    let mut response_partition = OffsetFetchResponsePartition::default();
                                    response_partition.partition_index = *partition_index;
                                    response_partition.committed_offset = 0; // オフセットが見つからない場合は0
                                    response_partition.error_code = 0;
                                    response_partition
                                }
                            })
                            .collect::<Vec<_>>();
                        response_topic
                    }).collect();
                res_topics
            } else {
                vec![]
            };
            res.throttle_time_ms = 0; // レスポンスのスロットル時間
            res
        } else {
            log::info!("Consumer group not found, creating new group: {}", *group_id.clone());
            OffsetFetchResponse::default()
        }
    };

    // レスポンスをエンコードして送信
    log::debug!("OffsetFetchResponse: {:?}", response);
    send_kafka_response(stream, header, &response).await?;
    log::debug!("Sent OffsetFetchResponse");
    Ok(())
}
