use anyhow::{anyhow, Result};
use std::ops::Bound;
use tikv_client::{Key, KvPair, TransactionClient, Error as TikvError};

use crate::traits::meta_store::UnsendMetaStore;
use crate::common::topic_partition::Topic;
use crate::common::consumer::ConsumerGroup;
use std::{future::Future, pin::Pin};

// ========== Keyspace ==========
const NS: &str = "ms:"; // meta-store namespace

fn k_topic(id: &str) -> String { format!("{NS}topic:data:{}", id) }
fn k_topic_index_name_prefix(name: &str) -> String { format!("{NS}topic:index:name:{}:", name) }
fn k_topic_index_name_item(name: &str, id: &str) -> String { format!("{NS}topic:index:name:{}:{}", name, id) }
fn k_cg(id: &str) -> String { format!("{NS}consumer_group:{}", id) }
fn k_seq(name: &str) -> String { format!("{NS}seq:{}", name) }

fn prefix_end(mut prefix: Vec<u8>) -> Vec<u8> {
    if let Some(last) = prefix.last_mut() {
        if *last != 0xff { *last += 1; return prefix; }
    }
    Vec::new()
}

pub struct TikvMetaStore {
    client: TransactionClient,
}

impl TikvMetaStore {
    pub fn new(client: TransactionClient) -> Self {
        Self {
            client,
        }
    }

    // auto retry with exponential backoff
    async fn with_txn_retry<F, T>(&self, mut f: F) -> Result<T>
    where
        F: for<'a> FnMut(
            &'a mut tikv_client::Transaction
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>
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
                        if matches!(te, TikvError::KvError { message } if message.contains("conflict")) {
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

    /// prefix scan with limit
    async fn scan_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<KvPair>> {
        let start_key = Key::from(prefix.as_bytes().to_vec());
        let end_raw = prefix_end(prefix.as_bytes().to_vec());
        let end_bound = if end_raw.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(Key::from(end_raw))
        };

        let mut out = Vec::new();
        let mut txn = self.client.begin_optimistic().await?;
        let mut start_bound = Bound::Included(start_key);

        loop {
            let batch: Vec<KvPair> = txn.scan((start_bound.clone(), end_bound.clone()), limit as u32)
                .await?
                .collect();

            if batch.is_empty() { break; }
            // 次回は最後のキーの「直後」から
            let last_key = batch.last().unwrap().0.clone();
            start_bound = Bound::Excluded(last_key);

            out.extend(batch);
            if out.len() >= limit { break; }
        }
        txn.commit().await?;
        Ok(out)
    }
}

impl UnsendMetaStore for TikvMetaStore {
    async fn put_topic(&self, data: &Topic) -> Result<()> {
        let id = data.topic_id.to_string();
        let key_topic = k_topic(&id);
        let idx_items: Vec<String> = match &data.name {
            Some(name) => vec![k_topic_index_name_item(name, &id)],
            None => vec![],
        };
        let val = serde_json::to_vec(data)?;

        self.with_txn_retry(|txn| {
            let key_topic = key_topic.clone();
            let val = val.clone();
            let idx_items = idx_items.clone();
            Box::pin(async move {
                txn.put(key_topic, val).await?;
                for idx in &idx_items { txn.put(idx.clone(), b"".to_vec()).await?; }
                Ok(())
            })
        }).await
    }

    async fn get_topic(&self, topic_id: &str) -> Result<Topic> {
        let key = k_topic(topic_id);
        self.with_txn_retry(|txn| {
            let key = key.clone();
            Box::pin(async move {
                let key_for_err = key.clone();
                let v = txn.get(key).await?;
                let bytes = v.ok_or_else(|| anyhow!("Topic not found with key: {}", key_for_err))?;
                let t: Topic = serde_json::from_slice(&bytes)?;
                Ok(t)
            })
        }).await
    }

    async fn delete_topic_by_id(&self, topic_id: uuid::Uuid) -> Result<()> {
        let id = topic_id.to_string();
        let key = k_topic(&id);

        self.with_txn_retry(|txn| {
            let key = key.clone();
            let id = id.clone();
            Box::pin(async move {
                // To delete index, first get topic to find its name
                if let Some(bytes) = txn.get(key.clone()).await? {
                    if let Ok(t) = serde_json::from_slice::<Topic>(&bytes) {
                        if let Some(name) = t.name.as_deref() {
                            let idx = k_topic_index_name_item(name, &id);
                            txn.delete(idx).await?;
                        }
                    }
                }
                let deleted_prev = txn.delete(key).await;
                deleted_prev?;
                Ok(())
            })
        }).await
    }

    async fn get_topics(&self) -> Result<Vec<Topic>> {
        // topic:data: scan prefix
        let prefix = format!("{NS}topic:data:");
        // set a high limit, but in real use cases the number of topics is expected to be manageable
        let kvs = self.scan_prefix(&prefix, 20_000).await?;
        let mut out = Vec::new();
        for kv in kvs {
            if let Ok(t) = serde_json::from_slice::<Topic>(&kv.1) { out.push(t); }
        }
        Ok(out)
    }

    async fn get_topic_id_by_topic_name(&self, topic_name: &str) -> Result<Option<String>> {
        let prefix = k_topic_index_name_prefix(topic_name);
        let kvs = self.scan_prefix(&prefix, 16).await?;
        let id = kvs
            .get(0)
            .and_then(|kv| {
                // key = ...:{name}:{id} → extract id
                let key_bytes: &[u8] = (&kv.0).into();
                let key_str = std::str::from_utf8(key_bytes).ok()?;
                key_str.strip_prefix(&prefix).map(|s| s.to_string())
            });
        Ok(id)
    }

    async fn save_consumer_group(&self, data: &ConsumerGroup) -> Result<()> {
        let key = k_cg(&data.group_id);
        let val = serde_json::to_vec(data)?;
        self.with_txn_retry(|txn| {
            let key = key.clone();
            let val = val.clone();
            Box::pin(async move {
                txn.put(key, val).await?;
                Ok(())
            })
        }).await
    }

    async fn get_consumer_groups(&self) -> Result<Vec<ConsumerGroup>> {
        let prefix = format!("{NS}consumer_group:");
        let kvs = self.scan_prefix(&prefix, 20000).await?;
        let mut out = Vec::new();
        for kv in kvs { if let Ok(cg) = serde_json::from_slice::<ConsumerGroup>(&kv.1) { out.push(cg); } }
        Ok(out)
    }

    async fn get_consumer_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        let key = k_cg(group_id);
        self.with_txn_retry(|txn| {
            let key = key.clone();
            Box::pin(async move {
                let v = txn.get(key).await?;
                let ans = match v { Some(bytes) => Some(serde_json::from_slice::<ConsumerGroup>(&bytes)?), None => None };
                Ok(ans)
            })
        }).await
    }

    async fn gen_producer_id(&self) -> Result<i64> {
        let seq_key = k_seq("producer_id_counter01");
        self.with_txn_retry({
            let seq_key = seq_key.clone();
            move |txn| {
                let seq_key = seq_key.clone();
                Box::pin(async move {
                    // if without exclusive lock, it may conflict with others
                    let cur = txn.get(seq_key.clone()).await?;
                    let mut v: i64 = match cur { Some(b) => String::from_utf8(b).ok().and_then(|s| s.parse().ok()).unwrap_or(0), None => 0 };
                    v += 1;
                    txn.put(seq_key.clone(), v.to_string().into_bytes()).await?;
                    Ok(v)
                })
            }
        }).await
    }

    async fn update_consumer_group<F, Fut>(&self, group_id: &str, update_fn: F) -> Result<Option<ConsumerGroup>>
    where
        F: FnOnce(ConsumerGroup) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<ConsumerGroup>> + Send + 'static,
    {
        let key = k_cg(group_id);
        // Wrap update_fn in Option so it can be moved into the async block
        let mut update_fn = Some(update_fn);
        self.with_txn_retry(|txn| {
            let key = key.clone();
            // Take update_fn out of the Option
            let update_fn = update_fn.take().expect("update_fn already taken");
            Box::pin(async move {
                let v = match txn.get(key.clone()).await? { Some(b) => b, None => { txn.commit().await?; return Ok(None); } };
                let cg: ConsumerGroup = serde_json::from_slice(&v)?;

                // Warning: short-lived transactions are recommended
                let cg = update_fn(cg).await?;

                let updated = serde_json::to_vec(&cg)?;
                txn.put(key.clone(), updated).await?;
                Ok(Some(cg))
            })
        }).await
    }
}
