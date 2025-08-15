use redis::cluster_async::ClusterConnection;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::Mutex;
use redis::ToRedisArgs;
use redis::FromRedisValue;
use tokio::time::sleep;
use std::time::Duration;
use crate::common::utils::jittered_delay;

#[derive(Clone)]
pub struct RedisClient {
    cluster: bool,
    cluster_conn: Option<Arc<Mutex<ClusterConnection>>>,
    single_conn: Option<Arc<Mutex<MultiplexedConnection>>>,
}

impl RedisClient {
    pub fn new(cluster: bool, cluster_conn: Option<Arc<Mutex<ClusterConnection>>>, single_conn: Option<Arc<Mutex<MultiplexedConnection>>>) -> Self {
        Self {
            cluster,
            cluster_conn,
            single_conn,
        }
    }

    pub async fn set<T>(&self, key: &str, value: T) -> anyhow::Result<()>
    where
        T: ToRedisArgs + Send + Sync,
    {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let _: () = conn.set(key, value).await?;
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let _: () = conn.set(key, value).await?;
            }
        }
        Ok(())
    }

    pub async fn get<T>(&self, key: &str) -> anyhow::Result<Option<T>>
    where
        T: FromRedisValue + Send,
    {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let value: Option<T> = conn.get(key).await?;
                return Ok(value);
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let value: Option<T> = conn.get(key).await?;
                return Ok(value);
            }
        }
        Ok(None)
    }

    pub async fn del(&self, key: &str) -> anyhow::Result<usize> {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let result: usize = conn.del(key).await?;
                return Ok(result);
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let result: usize = conn.del(key).await?;
                return Ok(result);
            }
        }
        Ok(0)
    }

    pub async fn keys(&self, pattern: &str) -> anyhow::Result<Vec<String>> {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let keys: Vec<String> = conn.keys(pattern).await?;
                return Ok(keys);
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let keys: Vec<String> = conn.keys(pattern).await?;
                return Ok(keys);
            }
        }
        Ok(vec![])
    }

    pub async fn lock_exclusive(&self, key: &str, value: &str, timeout_secs: i64) -> anyhow::Result<bool> {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let result: bool = conn.set_nx(key, value).await?;
                if result {
                    // Set expiration to avoid deadlock
                    let _: () = conn.expire(key, timeout_secs).await?;
                }
                return Ok(result);
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let result: bool = conn.set_nx(key, value).await?;
                if result {
                    // Set expiration to avoid deadlock
                    let _: () = conn.expire(key, timeout_secs).await?;
                }
                return Ok(result);
            }
        }
        Ok(false)
    }

    pub async fn try_acquire_lock(&self, lock_key: &str, max_retries: i64, retry_delay_ms: u64, ttl_secs: i64) -> anyhow::Result<bool> {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let mut acquired = false;
                for attempt in 0..max_retries {
                    match conn.set_nx(lock_key, "lock").await {
                        Ok(true) => {
                            log::debug!("✅ Lock acquired: {} (attempt {})", lock_key, attempt + 1);
                            let _: () = conn.expire(lock_key, ttl_secs).await?;
                            acquired = true;
                            break;
                        }
                        Ok(false) => {
                            log::debug!(
                                "🔒 Lock busy (attempt {}/{}): {}. Retrying...",
                                attempt + 1,
                                max_retries,
                                lock_key
                            );
                        }
                        Err(e) => {
                            log::error!("❌ Redis error while acquiring lock {}: {}", lock_key, e);
                            return Err(anyhow::anyhow!("Redis error while acquiring lock: {}", e));
                        }
                    }
                    // drop(c); // Not necessary in async scope; will drop on scope exit
                    sleep(Duration::from_millis(jittered_delay(retry_delay_ms))).await;
                }
                return Ok(acquired);
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let mut acquired = false;
                for attempt in 0..max_retries {
                    match conn.set_nx(lock_key, "lock").await {
                        Ok(true) => {
                            log::debug!("✅ Lock acquired: {} (attempt {})", lock_key, attempt + 1);
                            let _: () = conn.expire(lock_key, ttl_secs).await?;
                            acquired = true;
                            break;
                        }
                        Ok(false) => {
                            log::debug!(
                                "🔒 Lock busy (attempt {}/{}): {}. Retrying...",
                                attempt + 1,
                                max_retries,
                                lock_key
                            );
                        }
                        Err(e) => {
                            log::error!("❌ Redis error while acquiring lock {}: {}", lock_key, e);
                            return Err(anyhow::anyhow!("Redis error while acquiring lock: {}", e));
                        }
                    }
                    // drop(c); // Not necessary in async scope; will drop on scope exit
                    sleep(Duration::from_millis(retry_delay_ms)).await;
                }
                return Ok(acquired);
            }
        }
        Ok(false)
    }


    pub async fn unlock_exclusive(&self, key: &str) -> anyhow::Result<()> {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let _: () = conn.del(key).await?;
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let _: () = conn.del(key).await?;
            }
        }
        Ok(())
    }

    pub async fn sadd(&self, key: &str, member: &str) -> anyhow::Result<()> {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let _: () = conn.sadd(key, member).await?;
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let _: () = conn.sadd(key, member).await?;
            }
        }
        Ok(())
    }

    pub async fn smembers(&self, key: &str) -> anyhow::Result<Vec<String>> {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let values: Vec<String> = conn.smembers(key).await?;
                return Ok(values);
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let values: Vec<String> = conn.smembers(key).await?;
                return Ok(values);
            }
        }
        Ok(vec![])
    }

    pub async fn zadd(&self, key: &str, member: &str, score: i64) -> anyhow::Result<()> {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let _: () = conn.zadd(key, member, score).await?;
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let _: () = conn.zadd(key, member, score).await?;
            }
        }
        Ok(())
    }
    
    pub async fn zrangebyscore(&self, key: &str, min: i64, max: i64) -> anyhow::Result<Vec<String>> {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let values: Vec<String> = conn.zrangebyscore(key, min, max).await?;
                return Ok(values);
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let values: Vec<String> = conn.zrangebyscore(key, min, max).await?;
                return Ok(values);
            }
        }
        Ok(vec![])
    }

    pub async fn incr(&self, key: &str, increment: i64) -> anyhow::Result<i64> {
        if self.cluster {
            if let Some(conn) = &self.cluster_conn {
                let mut conn = conn.lock().await;
                let value: i64 = conn.incr(key, increment).await?;
                return Ok(value);
            }
        } else {
            if let Some(conn) = &self.single_conn {
                let mut conn = conn.lock().await;
                let value: i64 = conn.incr(key, increment).await?;
                return Ok(value);
            }
        }
        Ok(0)
    }

}