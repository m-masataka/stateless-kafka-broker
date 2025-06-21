use anyhow::Result;
use redis::{aio::MultiplexedConnection, AsyncCommands};
use redis::Client;

#[derive(Clone)]
pub struct RedisClient {
    connection: MultiplexedConnection,
}

impl RedisClient {
    pub async fn new() -> Result<Self> {
        let client = Client::open("redis://127.0.0.1/")?;
        let conn = client.get_multiplexed_tokio_connection().await?;
        Ok(RedisClient { connection: conn })
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.connection.clone();
        let val: Option<String> = conn.get(key).await?;
        Ok(val)
    }

    pub async fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let mut conn = self.connection.clone();
        let keys: Vec<String> = conn.keys(pattern).await?;
        Ok(keys)
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<()> {
        let mut conn = self.connection.clone();
        let _: () = conn.set(key, value).await?;
        Ok(())
    }

    pub async fn sadd(&self, key: &str, member: &str) -> Result<()> {
        let mut conn = self.connection.clone();
        let _: () = conn.sadd(key, member).await?;
        Ok(())
    }

    pub async fn smembers(&self, key: &str) -> Result<Vec<String>> {
        let mut conn = self.connection.clone();
        let members: Vec<String> = conn.smembers(key).await?;
        Ok(members)
    }

    pub async fn del(&self, key: &str) -> Result<()> {
        let mut conn = self.connection.clone();
        let _: () = conn.del(key).await?;
        Ok(())
    }

    pub async fn lock_exclusive(&self, key: &str, timeout: i64) -> Result<(bool)> {
        let mut conn = self.connection.clone();
        let acquired: bool = conn.set_nx(&key, "lock").await?;
        if acquired {
            // Set an expiration time for the lock
            let _: () = conn.expire(key, timeout).await?;
        }
        Ok(acquired)
    }

    pub async fn unlock(&self, key: &str) -> Result<()> {
        let mut conn = self.connection.clone();
        let _: () = conn.del(key).await?;
        Ok(())
    }

    pub async fn incr(&self, key: &str, increment: i64) -> Result<i64> {
        let mut conn = self.connection.clone();
        let new_value: i64 = conn.incr(key, increment).await?;
        Ok(new_value)
    }
}