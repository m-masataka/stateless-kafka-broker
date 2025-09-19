use bytes::Bytes;

#[trait_variant::make(LogStore: Send)]
pub trait UnsendLogStore {
    async fn write_records(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        records: Option<&Bytes>,
    ) -> anyhow::Result<(i64, String, u64)>;
    async fn read_records(&self, keys: Vec<String>) -> anyhow::Result<Bytes>;
}
