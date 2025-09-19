use crate::handler::context::HandlerContext;
use crate::traits::meta_store::MetaStore;
use crate::{common::response::send_kafka_response, storage::meta_store_impl::MetaStoreImpl};
use anyhow::{Error, Result};
use kafka_protocol::messages::{
    RequestHeader,
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup},
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
        OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
};
use kafka_protocol::protocol::StrBytes;

pub async fn handle_offset_fetch_request(
    header: &RequestHeader,
    request: &OffsetFetchRequest,
    handler_ctx: &HandlerContext,
) -> Result<Vec<u8>> {
    log::info!(
        "Handling OffsetFetchRequest API VERSION {}",
        header.request_api_version
    );
    log::debug!("OffsetFetchRequest: {:?}", request);

    let meta_store = handler_ctx.meta_store.clone();
    // let mut response = OffsetFetchResponse::default();
    let response = if request.group_id.is_empty() {
        log::debug!("Group ID is empty in OffsetFetchRequest");
        // if version 8-9 not support group_id, only groups
        let mut offset_fetch_response = OffsetFetchResponse::default();
        let mut offset_responses = Vec::new();
        for group in &request.groups {
            offset_responses.push(build_response_group(&meta_store, group).await);
        }
        offset_fetch_response.groups = offset_responses;
        offset_fetch_response.throttle_time_ms = 0;
        offset_fetch_response
    } else {
        log::debug!(
            "Group ID is provided in OffsetFetchRequest: {}",
            request.group_id.as_str()
        );
        build_response_with_group_id(&meta_store, request).await?
    };

    // レスポンスをエンコードして送信
    log::debug!("OffsetFetchResponse: {:?}", response);
    log::debug!("Sent OffsetFetchResponse");
    send_kafka_response(header, &response).await
}

async fn build_response_group(
    meta_store: &MetaStoreImpl,
    group: &OffsetFetchRequestGroup,
) -> OffsetFetchResponseGroup {
    let mut group_response = OffsetFetchResponseGroup::default();
    group_response.group_id = group.group_id.clone();

    match meta_store.get_consumer_group(&group.group_id).await {
        Ok(Some(consumer_group)) => {
            log::debug!("Consumer group: {:?}", consumer_group);

            if let Some(topics) = &group.topics {
                group_response.topics = topics
                    .iter()
                    .map(|topic| {
                        let mut topic_response = OffsetFetchResponseTopics::default();
                        topic_response.name = topic.name.clone();

                        topic_response.partitions = topic
                            .partition_indexes
                            .iter()
                            .map(|&index| {
                                let mut partition = OffsetFetchResponsePartitions::default();
                                partition.partition_index = index;
                                log::debug!(
                                    "Process topic: {}, partition: {}",
                                    topic.name.as_str(),
                                    index
                                );

                                if let Some(store_partition) = consumer_group
                                    .get_partition_by_topic_and_index(&topic.name, index)
                                {
                                    log::info!(
                                        "Found partition: {} for topic: {}",
                                        index,
                                        topic.name.as_str()
                                    );
                                    partition.committed_offset = store_partition.committed_offset;
                                    partition.metadata = Some(StrBytes::from_string(
                                        store_partition
                                            .metadata
                                            .as_ref()
                                            .map(|b| String::from_utf8_lossy(b).to_string())
                                            .unwrap_or_default(),
                                    ));
                                    partition.error_code = 0;
                                } else {
                                    log::warn!(
                                        "Partition {} not found for topic: {}",
                                        index,
                                        topic.name.as_str()
                                    );
                                    partition.committed_offset = 0;
                                    partition.error_code = 0;
                                }
                                partition
                            })
                            .collect();

                        topic_response
                    })
                    .collect();
            }
        }
        Ok(None) => {
            log::info!("Consumer group not found: {}", group.group_id.as_str());
        }
        Err(e) => {
            panic!("Failed to get consumer group: {}", e);
        }
    }

    group_response
}

async fn build_response_with_group_id(
    meta_store: &MetaStoreImpl,
    request: &OffsetFetchRequest,
) -> Result<OffsetFetchResponse, Error> {
    let mut response = OffsetFetchResponse::default();
    let group_id = &request.group_id;

    if let Some(store) = meta_store.get_consumer_group(group_id).await? {
        log::info!("Found existing consumer group: {}", group_id.as_str());

        if let Some(topics) = &request.topics {
            response.topics = topics
                .iter()
                .map(|topic| {
                    let mut topic_response = OffsetFetchResponseTopic::default();
                    topic_response.name = topic.name.clone();

                    topic_response.partitions = topic
                        .partition_indexes
                        .iter()
                        .map(|&index| {
                            let mut partition = OffsetFetchResponsePartition::default();
                            partition.partition_index = index;

                            if let Some(store_partition) =
                                store.get_partition_by_topic_and_index(&topic.name, index)
                            {
                                log::info!(
                                    "Found partition: {} for topic: {}",
                                    index,
                                    topic.name.as_str()
                                );
                                partition.committed_offset = store_partition.committed_offset;
                                partition.metadata = Some(StrBytes::from_string(
                                    store_partition
                                        .metadata
                                        .as_ref()
                                        .map(|b| String::from_utf8_lossy(b).to_string())
                                        .unwrap_or_default(),
                                ));
                                partition.error_code = 0;
                            } else {
                                log::warn!(
                                    "Partition {} not found for topic: {}",
                                    index,
                                    topic.name.as_str()
                                );
                                partition.committed_offset = 0;
                                partition.error_code = 0;
                            }

                            partition
                        })
                        .collect();

                    topic_response
                })
                .collect();
        }
    } else {
        log::info!(
            "Consumer group not found, creating new group: {}",
            group_id.as_str()
        );
    }

    response.throttle_time_ms = 0;
    Ok(response)
}
