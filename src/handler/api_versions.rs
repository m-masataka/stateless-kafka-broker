use crate::common::response::send_kafka_response;
use crate::handler::context::HandlerContext;
use anyhow::Result;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::api_versions_request::ApiVersionsRequest;
use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};

pub async fn handle_api_versions_request(
    header: &RequestHeader,
    request: &ApiVersionsRequest,
    _: &HandlerContext,
) -> Result<Vec<u8>> {
    log::info!(
        "Handling ApiVersionRequest API VERSION {}",
        header.request_api_version
    );
    log::debug!("ApiVersionRequest: {:?}", request);
    // Send ApiVersionsResponse
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    response.throttle_time_ms = 0;
    response.api_keys = all_supported_api_versions();

    log::info!(
        "Sent ApiVersionsResponse with {} API versions",
        response.api_keys.len()
    );
    send_kafka_response(header, &response).await
}

fn all_supported_api_versions() -> Vec<ApiVersion> {
    vec![
        (0, 0, 9),  // Produce
        (1, 0, 12), // Fetch
        (2, 0, 6),  // ListOffsets
        (3, 0, 12), // Metadata
        (4, 0, 1),  // LeaderAndIsr
        (5, 0, 2),  // StopReplica
        (6, 0, 3),  // UpdateMetadata
        (7, 0, 1),  // ControlledShutdown
        (8, 0, 8),  // OffsetCommit
        (9, 0, 12), // OffsetFetch
        (10, 0, 7), // FindCoordinator
        (11, 0, 9), // JoinGroup
        (12, 0, 5), // Heartbeat
        (13, 0, 5), // LeaveGroup
        (14, 0, 5), // SyncGroup
        (15, 0, 5), // DescribeGroups
        (16, 0, 4), // ListGroups
        (17, 0, 6), // SaslHandshake
        (18, 0, 7), // ApiVersions
        (19, 0, 7), // CreateTopics
        (20, 0, 6), // DeleteTopics
        (21, 0, 3), // DeleteRecords
        (22, 0, 5), // InitProducerId
        (23, 0, 3), // AddPartitionsToTxn
        (24, 0, 3), // AddOffsetsToTxn
        (25, 0, 3), // EndTxn
        (26, 0, 0), // WriteTxnMarkers
        (27, 0, 3), // TxnOffsetCommit
        (28, 0, 3), // DescribeAcls
        (29, 0, 3), // CreateAcls
        (30, 0, 3), // DeleteAcls
        (31, 0, 3), // DescribeConfigs
        (32, 0, 2), // AlterConfigs
        (33, 0, 1), // AlterReplicaLogDirs
        (34, 0, 1), // DescribeLogDirs
        (35, 0, 2), // SaslAuthenticate
        (36, 0, 3), // CreatePartitions
        (37, 0, 2), // CreateDelegationToken
        (38, 0, 2), // RenewDelegationToken
        (39, 0, 2), // ExpireDelegationToken
        (40, 0, 2), // DescribeDelegationToken
        (41, 0, 2), // DeleteGroups
        (42, 0, 2), // ElectLeaders
        (43, 0, 1), // IncrementalAlterConfigs
        (44, 0, 1), // AlterPartitionReassignments
        (45, 0, 1), // ListPartitionReassignments
        (46, 0, 1), // OffsetDelete
        (47, 0, 3), // DescribeClientQuotas
        (48, 0, 1), // AlterClientQuotas
        (49, 0, 2), // DescribeUserScramCredentials
        (50, 0, 1), // AlterUserScramCredentials
        (51, 0, 1), // Vote
        (52, 0, 1), // BeginQuorumEpoch
        (53, 0, 1), // EndQuorumEpoch
        (54, 0, 1), // DescribeQuorum
        (55, 0, 1), // AlterIsr
        (56, 0, 1), // UpdateFeatures
        (57, 0, 1), // Envelope
        (58, 0, 1), // DescribeCluster
        (59, 0, 1), // DescribeProducers
        (60, 0, 1), // DescribeTransactions
        (61, 0, 1), // ListTransactions
        (62, 0, 1), // AllocateProducerIds
    ]
    .into_iter()
    .map(|(api_key, min, max)| {
        let mut v = ApiVersion::default();
        v.api_key = api_key;
        v.min_version = min;
        v.max_version = max;
        v
    })
    .collect()
}
