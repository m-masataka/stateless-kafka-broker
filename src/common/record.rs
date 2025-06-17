use serde::{Serialize, Deserialize};
use base64::{engine::general_purpose, Engine as _};
use bytes::Bytes;
use indexmap::IndexMap;
use kafka_protocol::records::TimestampType;
use kafka_protocol::protocol::StrBytes;

#[derive(Serialize, Deserialize)]
pub struct RecordEntry {
    pub transactional: bool,
    pub control: bool,
    pub partition_leader_epoch: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub timestamp_type: String,
    pub offset: i64,
    pub sequence: i32,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Option<String>)>,
}

impl RecordEntry {
    pub fn convert_to_kafka_record(
        &self
    ) -> Option<kafka_protocol::records::Record> {
        let headers: IndexMap<StrBytes, Option<Bytes>> = IndexMap::new(); // Use Bytes instead of StrBytes
        Some(kafka_protocol::records::Record {
            transactional: self.transactional,
            control: self.control,
            partition_leader_epoch: self.partition_leader_epoch,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            timestamp_type: string_to_timestamp_type(&self.timestamp_type).ok()?,
            offset: self.offset,
            sequence: self.sequence,
            timestamp: self.timestamp,
            key: decode_bytes_base64(self.key.as_ref()).ok()?,
            value: decode_bytes_base64(self.value.as_ref()).ok()?,
            headers,
        })
    }
}

pub fn string_to_timestamp_type(
    timestamp_type: &str,
) -> Result<TimestampType, String> {
    match timestamp_type {
        "Creation" | "CreateTime" => Ok(TimestampType::Creation),
        "LogAppend" | "LogAppendTime" => Ok(TimestampType::LogAppend),
        _ => Err(format!("Unknown timestamp type: {}", timestamp_type)),
    }
}

pub fn convert_kafka_record_to_record_entry(
    record: &kafka_protocol::records::Record,
    offset: i64
) -> RecordEntry {
    let headers: Vec<(String, Option<String>)>  = vec![];
    RecordEntry {
        transactional: record.transactional,
        control: record.control,
        partition_leader_epoch: record.partition_leader_epoch,
        producer_id: record.producer_id,
        producer_epoch: record.producer_epoch,
        timestamp_type: format!("{:?}", record.timestamp_type),
        offset,
        sequence: record.sequence,
        timestamp: record.timestamp,
        key: encode_bytes_base64(record.key.as_ref()),
        value: encode_bytes_base64(record.value.as_ref()),
        headers,
    }
}

#[derive(Serialize, Deserialize)]
pub struct Offset {
    pub offset: i64
}

pub fn encode_bytes_base64(input: Option<&Bytes>) -> Option<Bytes> {
    input.map(|data| {
        let encoded = general_purpose::STANDARD.encode(data);
        Bytes::from(encoded)
    })
}

pub fn decode_bytes_base64(input: Option<&Bytes>) -> Result<Option<Bytes>, base64::DecodeError> {
    match input {
        Some(data) => {
            let as_str = std::str::from_utf8(data)
                .map_err(|_| base64::DecodeError::InvalidByte(0, b'?'))?;
            let decoded = general_purpose::STANDARD.decode(as_str)?;
            Ok(Some(Bytes::from(decoded)))
        }
        None => Ok(None),
    }
}