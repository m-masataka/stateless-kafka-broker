use bytes::Bytes;

#[trait_variant::make(LogStore: Send)]
pub trait UnsendLogStore{
    async fn write_batch(&self, topic: &str, partition: i32, records: Option<&Bytes>) -> anyhow::Result<i64>;
    async fn read_records(&self, topic: &str, partition: i32, offset: i64, max_offset: i64) -> anyhow::Result<Bytes>;
    async fn read_offset(&self, topic: &str, partition: i32) -> anyhow::Result<i64>;
}