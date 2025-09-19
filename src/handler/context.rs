use std::sync::Arc;

use crate::common::cluster::Node;
use crate::storage::index_store_impl::IndexStoreImpl;
use crate::storage::log_store_impl::LogStoreImpl;
use crate::storage::meta_store_impl::MetaStoreImpl;

#[derive(Clone)]
pub struct HandlerContext {
    pub log_store: Arc<LogStoreImpl>,
    pub meta_store: Arc<MetaStoreImpl>,
    pub index_store: Arc<IndexStoreImpl>,
    pub node_config: Arc<Node>,
}
