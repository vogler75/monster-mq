use super::traits::*;
use crate::config::StoreType;
use std::sync::Arc;

/// Storage holds Arc references to all the per-feature stores. It is the type
/// passed to the broker hooks, GraphQL resolvers, archive manager, etc. Every
/// backend (sqlite/postgres/mongodb) builds its own Storage.
#[derive(Clone)]
pub struct Storage {
    pub backend: StoreType,
    pub sessions: Arc<dyn SessionStore>,
    pub queue: Arc<dyn QueueStore>,
    pub retained: Arc<dyn MessageStore>,
    pub users: Arc<dyn UserStore>,
    pub archive_config: Arc<dyn ArchiveConfigStore>,
    pub device_config: Arc<dyn DeviceConfigStore>,
    pub metrics: Arc<dyn MetricsStore>,
}
