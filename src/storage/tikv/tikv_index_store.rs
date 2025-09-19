use crate::common::index::IndexData;
use crate::traits::index_store::UnsendIndexStore;
use anyhow::{Result, anyhow};
use std::ops::Bound;
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};
use tikv_client::{Error as TikvError, Key, KvPair, TransactionClient};

// ========== Keyspace ==========
const NS: &str = "ms:";
fn k_offset(topic: &str, partition: i32) -> String {
    format!("{NS}idx:offset:{topic}:{partition}")
}
fn k_lock(topic: &str, partition: i32) -> String {
    format!("{NS}idx:lock:{topic}:{partition}")
}
fn k_index_prefix(topic: &str, partition: i32) -> String {
    format!("{NS}idx:index:{topic}:{partition}:")
}
fn k_index(topic: &str, partition: i32, enc_off: &str) -> String {
    format!("{}{}", k_index_prefix(topic, partition), enc_off)
}

fn prefix_end(mut prefix: Vec<u8>) -> Vec<u8> {
    if let Some(last) = prefix.last_mut() {
        if *last != 0xff {
            *last += 1;
            return prefix;
        }
    }
    Vec::new()
}

// Sort offset as string so that lexicographical order == numerical order
fn enc_offset(off: i64) -> String {
    // off_in_u128 = off - i64::MIN (== off + 2^63)
    let off_u = (off as i128) - (i64::MIN as i128);
    format!("{off_u:020}")
}

pub struct TikvIndexStore {
    client: TransactionClient,
}

impl TikvIndexStore {
    pub fn new(client: TransactionClient) -> Self {
        Self { client }
    }

    // auto retry with exponential backoff
    async fn with_txn_retry<F, T>(&self, mut f: F) -> Result<T>
    where
        F: for<'a> FnMut(
                &'a mut tikv_client::Transaction,
            )
                -> Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>
            + Send,
    {
        const MAX_RETRY: usize = 10;
        let mut backoff_ms = 20u64;
        let mut last_err: Option<anyhow::Error> = None;

        for _ in 0..MAX_RETRY {
            let mut txn = self.client.begin_optimistic().await?;
            match f(&mut txn).await {
                Ok(v) => {
                    txn.commit().await?;
                    return Ok(v);
                }
                Err(e) => {
                    let _ = txn.rollback().await;

                    if let Some(te) = e.downcast_ref::<TikvError>() {
                        if matches!(te, TikvError::KvError { message } if message.contains("conflict"))
                        {
                            last_err = Some(e);
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                            backoff_ms = (backoff_ms * 2).min(1000);
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("transaction retries exhausted")))
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}

impl UnsendIndexStore for TikvIndexStore {
    async fn write_offset(&self, topic: &str, partition: i32, offset: i64) -> anyhow::Result<()> {
        let key = k_offset(topic, partition);
        self.with_txn_retry(|txn| {
            let key = key.clone();
            Box::pin(async move {
                txn.put(key, offset.to_string().into_bytes()).await?;
                Ok(())
            })
        })
        .await
    }

    async fn read_offset(&self, topic: &str, partition: i32) -> anyhow::Result<i64> {
        let key = k_offset(topic, partition);
        self.with_txn_retry(|txn| {
            let key = key.clone();
            Box::pin(async move {
                let v = txn.get(key).await?;
                let ans = match v {
                    Some(bytes) => String::from_utf8(bytes)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(-1),
                    None => -1,
                };
                Ok(ans)
            })
        })
        .await
    }

    async fn lock_exclusive(
        &self,
        topic: &str,
        partition: i32,
        timeout_secs: i64,
    ) -> anyhow::Result<Option<String>> {
        let key = k_lock(topic, partition);
        let lock_id = uuid::Uuid::new_v4().to_string();
        let expires = Self::now_ms() + timeout_secs * 1000;
        let value = format!("{lock_id}:{expires}");

        let got = self
            .with_txn_retry(|txn| {
                let key = key.clone();
                let value = value.clone();
                Box::pin(async move {
                    if let Some(bytes) = txn.get(key.clone()).await? {
                        if let Ok(s) = String::from_utf8(bytes) {
                            if let Some((_id, exp)) = s.split_once(':') {
                                if exp.parse::<i64>().unwrap_or(0) > Self::now_ms() {
                                    return Ok(false);
                                }
                            }
                        }
                    }
                    txn.put(key, value.into_bytes()).await?;
                    Ok(true)
                })
            })
            .await?;

        Ok(if got { Some(lock_id) } else { None })
    }

    async fn unlock_exclusive(
        &self,
        topic: &str,
        partition: i32,
        lock_id: &str,
    ) -> anyhow::Result<bool> {
        let key = k_lock(topic, partition);
        self.with_txn_retry(|txn| {
            let key = key.clone();
            let expect = lock_id.to_string();
            Box::pin(async move {
                if let Some(bytes) = txn.get(key.clone()).await? {
                    if let Ok(s) = String::from_utf8(bytes) {
                        if let Some((cur_id, _exp)) = s.split_once(':') {
                            if cur_id == expect {
                                txn.delete(key).await?;
                                return Ok(true);
                            }
                        }
                    }
                }
                Ok(false)
            })
        })
        .await
    }

    async fn set_index(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        data: &IndexData,
    ) -> anyhow::Result<()> {
        let enc = enc_offset(start_offset);
        let key = k_index(topic, partition, &enc);
        let val = serde_json::to_vec(data)?;
        self.with_txn_retry(|txn| {
            let key = key.clone();
            let val = val.clone();
            Box::pin(async move {
                if txn.get(key.clone()).await?.is_none() {
                    txn.put(key, val).await?;
                }
                Ok(())
            })
        })
        .await
    }

    async fn get_index_from_start_offset(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
    ) -> anyhow::Result<Vec<IndexData>> {
        let prefix = k_index_prefix(topic, partition);
        let start_key =
            Key::from(k_index(topic, partition, &enc_offset(start_offset)).into_bytes());
        let end_raw = prefix_end(prefix.as_bytes().to_vec());
        let range = if end_raw.is_empty() {
            (Bound::Included(start_key), Bound::Unbounded)
        } else {
            (
                Bound::Included(start_key),
                Bound::Excluded(Key::from(end_raw)),
            )
        };

        let (kvs, _finalized) = {
            let mut txn = self.client.begin_optimistic().await?;
            let stream = txn.scan(range, u32::MAX).await?;
            let kvs: Vec<KvPair> = stream.collect();
            txn.rollback().await?;
            (kvs, ())
        };

        let mut out = Vec::new();
        for kv in kvs {
            if let Ok(idx) = serde_json::from_slice::<IndexData>(&kv.1) {
                out.push(idx);
            }
        }
        Ok(out)
    }
}
