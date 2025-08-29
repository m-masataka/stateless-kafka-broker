use axum::{
    extract::State,
    Json,
};
use crate::traits::meta_store::MetaStore;
use crate::traits::index_store::IndexStore;
use crate::{server::rest_server::AppState};
use crate::server::loader::{
    load_meta_store,
    load_index_store,
};

pub async fn get_tp_offsets(State(st): State<AppState>) -> Json<serde_json::Value>{
    let meta_store = load_meta_store(&st.config).await.unwrap();
    let index_store = load_index_store(&st.config).await.unwrap();
    let topics = meta_store.get_topics().await.unwrap_or_default();
    log::debug!("Retrieved topics: {:?}", topics);
    let mut res_topics = Vec::new();
    for topic in &topics {
        let mut offsets = Vec::new();
        for i in 0..topic.num_partitions {
            log::debug!("Processing partition: {}", i);
            let topic_id = topic.topic_id.to_string();
            let partition_index = i;
            let offset = index_store.read_offset(&topic_id, partition_index).await.unwrap_or(-1);
            offsets.push(
                serde_json::json!({
                    "partitionIndex": partition_index,
                    "offset": offset,
                })
            );
        }
        res_topics.push(
            serde_json::json!({
                "topic": topic.name,
                "topicId": topic.topic_id,
                "partitions": offsets,
            })
        );
    }
    return Json(serde_json::json!({
        "topics": res_topics
    }));
}