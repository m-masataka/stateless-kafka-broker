use std::sync::Arc;
use tokio::sync::Mutex;
use crate::traits::index_store::UnsendIndexStore;
use redis::{aio::MultiplexedConnection, AsyncCommands};
pub struct RedisIndexStore {
    conn: Arc<Mutex<MultiplexedConnection>>,
}

impl RedisIndexStore {
    pub fn new(conn: Arc<Mutex<MultiplexedConnection>>) -> Self {
        Self {
            conn,
        }
    }
}

impl UnsendIndexStore for RedisIndexStore {
    async fn write_offset(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        let key = format!("offset:{}:{}", topic, partition);
        let _: () = conn.set(&key, offset).await?;
        Ok(())
    }

    async fn read_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> anyhow::Result<i64> {
        let mut conn = self.conn.lock().await;
        let key = format!("offset:{}:{}", topic, partition);
        let offset: Option<i64> = conn.get(&key).await?;
        match offset {
            Some(value) => Ok(value),
            None => Ok(-1), // Return -1 if no offset is found
        }
    }

    async fn lock_exclusive(
        &self,
        topic: &str,
        partition: i32,
        timeout_secs: i64,
    ) -> anyhow::Result<bool> {
        let mut conn = self.conn.lock().await;
        let key = format!("lock:{}:{}", topic, partition);
        let value = uuid::Uuid::new_v4().to_string(); // unique token for reentrant safety if needed
    
        let result: bool = conn
            .set_nx(&key, &value)
            .await?;
    
        if result {
            // Set expiration to avoid deadlock
            let _: () = conn.expire(&key, timeout_secs).await?;
        }
    
        Ok(result)
    }

    async fn unlock_exclusive(
        &self,
        topic: &str,
        partition: i32,
    ) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        let key = format!("lock:{}:{}", topic, partition);
        let _: () = conn.del(&key).await?;
        Ok(())
    }


    async fn set_index(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        data_path: &str,
    ) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        let key = format!("index:{}:{}", topic, partition);
        let _: () = conn.zadd(key, data_path, start_offset).await?;
        Ok(())
    }

    async fn get_index_from_start_offset(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
    ) -> anyhow::Result<Vec<String>> {
        let mut conn = self.conn.lock().await;
        let key = format!("index:{}:{}", topic, partition);
        let results: Vec<String> = conn
            .zrangebyscore(key, start_offset, i64::MAX)
            .await?;
        Ok(results)
    }
}

impl RedisIndexStore {
}