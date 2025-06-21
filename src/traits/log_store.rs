use bytes::Bytes;

pub trait LogStore: Send + Sync {
    async fn write_batch(&self, topic: &str, partition: i32, records: Option<&Bytes>) -> anyhow::Result<i64>;
    async fn read_records(&self, topic: &str, partition: i32, offset: i64, max_offset: i64) -> anyhow::Result<Bytes>;
    async fn read_offset(&self, topic: &str, partition: i32) -> anyhow::Result<i64>;
    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> anyhow::Result<()>;
    async fn delete_topic_by_name(&self, topic_name: &str) -> anyhow::Result<()>;
}