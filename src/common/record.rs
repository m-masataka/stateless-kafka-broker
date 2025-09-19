use base64::{Engine as _, engine::general_purpose};
use bincode::config;
use bincode::{Decode, Encode};
use bytes::Bytes;
use indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::TimestampType;
use kafka_protocol::records::{RecordBatchDecoder, RecordSet};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Decode, Encode)]
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
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<String>)>,
}

impl RecordEntry {
    pub fn convert_to_kafka_record(&self) -> Option<kafka_protocol::records::Record> {
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
            key: decode_bytes_base64_vec(self.key.as_ref()).ok()?,
            value: decode_bytes_base64_vec(self.value.as_ref()).ok()?,
            headers,
        })
    }
}

pub fn string_to_timestamp_type(timestamp_type: &str) -> Result<TimestampType, String> {
    match timestamp_type {
        "Creation" | "CreateTime" => Ok(TimestampType::Creation),
        "LogAppend" | "LogAppendTime" => Ok(TimestampType::LogAppend),
        _ => Err(format!("Unknown timestamp type: {}", timestamp_type)),
    }
}

pub fn convert_kafka_record_to_record_entry(
    record: &kafka_protocol::records::Record,
    offset: i64,
) -> RecordEntry {
    let headers: Vec<(String, Option<String>)> = vec![];
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
        key: encode_bytes_base64_vec(record.key.as_ref()),
        value: encode_bytes_base64_vec(record.value.as_ref()),
        headers,
    }
}

#[derive(Serialize, Deserialize)]
pub struct Offset {
    pub offset: i64,
}

pub fn encode_bytes_base64_vec(input: Option<&Bytes>) -> Option<Vec<u8>> {
    input.map(|data| {
        let encoded = general_purpose::STANDARD.encode(data);
        encoded.into_bytes()
    })
}

pub fn decode_bytes_base64_vec(
    input: Option<&Vec<u8>>,
) -> Result<Option<Bytes>, base64::DecodeError> {
    match input {
        Some(data) => {
            let as_str =
                std::str::from_utf8(data).map_err(|_| base64::DecodeError::InvalidByte(0, b'?'))?;
            let decoded = general_purpose::STANDARD.decode(as_str)?;
            Ok(Some(Bytes::from(decoded)))
        }
        None => Ok(None),
    }
}

pub fn bytes_to_output(data: &Bytes, base_offset: i64) -> anyhow::Result<(Vec<u8>, i64)> {
    let mut cursor = std::io::Cursor::new(data);
    let batch: RecordSet = RecordBatchDecoder::decode(&mut cursor)?;
    let mut entries = Vec::with_capacity(batch.records.len());
    batch.records.iter().enumerate().for_each(|(i, r)| {
        let entry = convert_kafka_record_to_record_entry(r, base_offset as i64 + i as i64 + 1);
        entries.push(entry);
    });

    let config = config::standard();
    let encoded = bincode::encode_to_vec(&entries, config)?;
    let current_offset = base_offset + batch.records.len() as i64;
    Ok((encoded, current_offset))
}

pub fn record_bytes_to_kafka_response_bytes(
    input_data: &Bytes,
) -> anyhow::Result<Vec<kafka_protocol::records::Record>> {
    // Decode the input data from Bytes to RecordEntry
    let config = config::standard();
    let (decoded_entries, _len): (Vec<RecordEntry>, usize) =
        bincode::decode_from_slice(&input_data, config)?;

    // Convert RecordEntry to kafka_protocol::records::Record
    let records: Vec<kafka_protocol::records::Record> = decoded_entries
        .into_iter()
        .filter_map(|record_entry| record_entry.convert_to_kafka_record())
        .collect();
    Ok(records)
}
