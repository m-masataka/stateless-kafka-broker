use std::sync::Arc;

use crate::common::cluster::Node;
use crate::storage::meta_store_impl::MetaStoreImpl;
use crate::traits::meta_store::MetaStore;
use anyhow::Result;

pub async fn send_heartbeat(node_config: &Arc<Node>, meta_store: Arc<MetaStoreImpl>) -> Result<()> {
    // Send heartbeat to the cluster
    match meta_store.update_cluster_status(node_config).await {
        std::result::Result::Ok(()) => {
            log::debug!("Cluster Heartbeat sent successfully");
            Ok(())
        }
        Err(e) => {
            log::warn!("Failed to send heartbeat: {:?}", e);
            Err(e)
        }
    }
}
