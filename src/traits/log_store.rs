use bytes::Bytes;

#[trait_variant::make(LogStore: Send)]
pub trait UnsendLogStore{
    async fn write_records(&self, topic: &str, partition: i32, start_offset: i64, records: Option<&Bytes>) -> anyhow::Result<(i64, String)>;
    async fn read_records(&self, keys: Vec<String>) -> anyhow::Result<Bytes>;
}

pub struct MockLogStore;
impl UnsendLogStore for MockLogStore {
    async fn write_records(&self, _topic: &str, _partition: i32, _start_offset: i64, _records: Option<&Bytes>) -> anyhow::Result<(i64, String)> {
        Ok((1, "mock_key".to_string()))
    }

    async fn read_records(&self, _keys: Vec<String>) -> anyhow::Result<Bytes> {
        Ok(Bytes::from("mocked data"))
    }
}