use kafka_protocol::{messages::RequestHeader, protocol::Encodable};
use anyhow::Result;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub async fn send_kafka_response<T, W>(
    stream: &mut W,
    header: &RequestHeader,
    response: &T,
) -> Result<()>
where 
    T: Encodable,
    W: AsyncWrite + Unpin,
{
    let is_flexible = is_flexible_version(header.request_api_key, header.request_api_version);
    send_kafka_response_insert_prefix(stream, header, response, is_flexible).await
}

pub async fn send_kafka_response_insert_prefix<T, W>(
    stream: &mut W,
    header: &RequestHeader,
    response: &T,
    insert_flag: bool,
) -> Result<()>
where 
    T: Encodable,
    W: AsyncWrite + Unpin,
{
    let mut response_buf = vec![];
    response.encode(&mut response_buf, header.request_api_version)?;

    if insert_flag {
        response_buf.insert(0, 0u8); // insert buffer prefix
    }

    let mut full_response = vec![];
    full_response.extend_from_slice(&(response_buf.len() as u32 + 4).to_be_bytes());
    full_response.extend_from_slice(&header.correlation_id.to_be_bytes());
    full_response.extend_from_slice(&response_buf);

    stream.write_all(&full_response).await?;
    stream.flush().await?;

    Ok(())
}

pub fn is_flexible_version(api_key: i16, version: i16) -> bool {
    match api_key {
        0 => version >= 9,   // Produce
        1 => version >= 12,  // Fetch
        3 => version >= 12,  // Metadata
        8 => version >= 7,   // OffsetCommit
        9 => version >= 8,   // OffsetFetch
        10 => version >= 3,  // FindCoordinator
        11 => version >= 7,  // JoinGroup
        12 => version >= 4,  // Heartbeat
        13 => version >= 3,  // LeaveGroup
        14 => version >= 4,  // SyncGroup
        15 => version >= 3,  // DescribeGroups
        18 => version >= 3,  // ApiVersions
        19 => version >= 5,  // CreateTopics
        20 => version >= 3,  // DeleteTopics
        21 => version >= 1,  // DeleteRecords
        22 => version >= 3,  // InitProducerId
        23 => version >= 1,  // AddPartitionsToTxn
        24 => version >= 1,  // AddOffsetsToTxn
        25 => version >= 1,  // EndTxn
        26 => version >= 0,  // WriteTxnMarkers
        27 => version >= 2,  // TxnOffsetCommit
        28 => version >= 1,  // DescribeAcls
        29 => version >= 1,  // CreateAcls
        30 => version >= 1,  // DeleteAcls
        31 => version >= 1,  // DescribeConfigs
        32 => version >= 1,  // AlterConfigs
        33 => version >= 1,  // AlterReplicaLogDirs
        34 => version >= 1,  // DescribeLogDirs
        35 => version >= 1,  // SaslAuthenticate
        36 => version >= 1,  // CreatePartitions
        37 => version >= 1,  // CreateDelegationToken
        38 => version >= 1,  // RenewDelegationToken
        39 => version >= 1,  // ExpireDelegationToken
        40 => version >= 1,  // DescribeDelegationToken
        41 => version >= 1,  // DeleteGroups
        42 => version >= 1,  // ElectLeaders
        43 => version >= 0,  // IncrementalAlterConfigs
        // ... 必要に応じて追加
        _ => false,
    }
}