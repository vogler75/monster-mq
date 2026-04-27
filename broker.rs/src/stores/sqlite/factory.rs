use super::*;
use crate::config::{Config, StoreType};
use crate::stores::storage::Storage;
use crate::stores::traits::*;
use std::sync::Arc;
use std::time::Duration;

/// Wires up a full Storage backed by a single SQLite file. The schema is
/// byte-compatible with the JVM/Go MonsterMQ broker.
///
/// Returns (Storage, Db) so the archive Manager can re-use the connection
/// when creating per-group last-value/archive tables.
pub async fn build(cfg: &Config) -> anyhow::Result<(Storage, db::Db)> {
    let db = db::Db::open(&cfg.sqlite.path).await?;

    let retained = Arc::new(message_store::SqliteMessageStore::new("retainedmessages", db.clone()));
    let sessions = Arc::new(session_store::SqliteSessionStore::new(db.clone()));
    let queue    = Arc::new(queue_store::SqliteQueueStore::new(db.clone(), Duration::from_secs(30)));
    let users    = Arc::new(user_store::SqliteUserStore::new(db.clone()));
    let archives = Arc::new(archive_config_store::SqliteArchiveConfigStore::new(db.clone()));
    let devices  = Arc::new(device_config_store::SqliteDeviceConfigStore::new(db.clone()));
    let metrics  = Arc::new(metrics_store::SqliteMetricsStore::new(db.clone()));

    retained.ensure_table().await?;
    sessions.ensure_table().await?;
    queue.ensure_table().await?;
    users.ensure_table().await?;
    archives.ensure_table().await?;
    devices.ensure_table().await?;
    metrics.ensure_table().await?;

    let storage = Storage {
        backend: StoreType::Sqlite,
        sessions: sessions.clone() as Arc<dyn SessionStore>,
        queue: queue.clone() as Arc<dyn QueueStore>,
        retained: retained.clone() as Arc<dyn MessageStore>,
        users: users.clone() as Arc<dyn UserStore>,
        archive_config: archives.clone() as Arc<dyn ArchiveConfigStore>,
        device_config: devices.clone() as Arc<dyn DeviceConfigStore>,
        metrics: metrics.clone() as Arc<dyn MetricsStore>,
    };
    Ok((storage, db))
}
