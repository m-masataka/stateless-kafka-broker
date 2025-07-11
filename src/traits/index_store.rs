
#[trait_variant::make(IndexStore: Send)]
pub trait UnsendIndexStore{
    async fn write_offset(&self, topic: &str, partition: i32, offset: i64) -> anyhow::Result<()>;
    async fn read_offset(&self, topic: &str, partition: i32) -> anyhow::Result<i64>;
    async fn lock_exclusive(&self, topic: &str, partition: i32, timeout: i64) -> anyhow::Result<bool>;
    async fn unlock_exclusive(&self, topic: &str, partition: i32) -> anyhow::Result<()>;
    async fn set_index(&self, topic: &str, partition: i32, start_offset: i64, data_path: &str) -> anyhow::Result<()>;
    async fn get_index_from_start_offset(&self, topic: &str, partition: i32, start_offset: i64) -> anyhow::Result<Vec<String>>;
}