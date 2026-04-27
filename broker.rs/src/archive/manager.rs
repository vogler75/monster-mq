use super::group::Group;
use crate::config::{Config, StoreType};
use crate::stores::memory::MemoryMessageStore;
use crate::stores::sqlite::Db as SqliteDb;
use crate::stores::sqlite::{SqliteMessageArchive, SqliteMessageStore};
use crate::stores::storage::Storage;
use crate::stores::traits::{MessageArchive, MessageStore};
use crate::stores::types::{ArchiveGroupConfig, BrokerMessage, MessageArchiveType, MessageStoreType, PayloadFormat};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

pub fn last_val_name(group: &str) -> String { format!("{}Lastval", group) }
pub fn archive_name(group: &str) -> String { format!("{}Archive", group) }

static VALID_GROUP_NAME: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[A-Za-z][A-Za-z0-9_]{0,62}$").unwrap());

/// Validate a group name — names flow into CREATE TABLE / DROP TABLE via
/// string interpolation so anything outside this set is rejected.
pub fn validate_group_name(name: &str) -> anyhow::Result<()> {
    if !VALID_GROUP_NAME.is_match(name) {
        anyhow::bail!("invalid group name {:?}: must match [A-Za-z][A-Za-z0-9_]{{0,62}}", name);
    }
    Ok(())
}

/// Manager owns the active set of archive groups and dispatches every
/// published message to all matching groups.
pub struct Manager {
    cfg: Config,
    storage: Storage,
    sqlite_db: Option<SqliteDb>,
    groups: RwLock<HashMap<String, Arc<Group>>>,
    deploy_errors: RwLock<HashMap<String, String>>,
}

impl Manager {
    pub fn new(cfg: Config, storage: Storage, sqlite_db: Option<SqliteDb>) -> Arc<Self> {
        Arc::new(Self {
            cfg, storage, sqlite_db,
            groups: RwLock::new(HashMap::new()),
            deploy_errors: RwLock::new(HashMap::new()),
        })
    }

    pub fn deploy_error(&self, name: &str) -> Option<String> {
        self.deploy_errors.read().get(name).cloned()
    }

    fn default_last_val_type(&self) -> MessageStoreType {
        match self.cfg.default_store_type {
            StoreType::Postgres => MessageStoreType::Postgres,
            StoreType::Mongodb => MessageStoreType::Mongodb,
            _ => MessageStoreType::Sqlite,
        }
    }

    fn default_archive_type(&self) -> MessageArchiveType {
        match self.cfg.default_store_type {
            StoreType::Postgres => MessageArchiveType::Postgres,
            StoreType::Mongodb => MessageArchiveType::Mongodb,
            _ => MessageArchiveType::Sqlite,
        }
    }

    pub async fn load(&self) -> anyhow::Result<()> {
        let mut configs = self.storage.archive_config.get_all().await?;
        if !configs.iter().any(|c| c.name == "Default") {
            let def = ArchiveGroupConfig {
                name: "Default".to_string(),
                enabled: true,
                topic_filters: vec!["#".to_string()],
                last_val_type: self.default_last_val_type(),
                archive_type: self.default_archive_type(),
                payload_format: PayloadFormat::Default,
                ..Default::default()
            };
            self.storage.archive_config.save(&def).await?;
            configs.push(def);
        }
        for c in configs {
            if let Err(e) = self.start_group(c.clone()).await {
                self.record_error(&c.name, Some(e.to_string()));
                tracing::error!(name = c.name, err = %e, "archive group start failed");
            }
        }
        Ok(())
    }

    fn record_error(&self, name: &str, err: Option<String>) {
        let mut w = self.deploy_errors.write();
        match err {
            Some(s) => { w.insert(name.to_string(), s); }
            None => { w.remove(name); }
        }
    }

    async fn build_last_val_store(&self, c: &ArchiveGroupConfig) -> anyhow::Result<Option<Arc<dyn MessageStore>>> {
        let name = last_val_name(&c.name);
        match c.last_val_type {
            MessageStoreType::Memory => Ok(Some(Arc::new(MemoryMessageStore::new(name)) as Arc<dyn MessageStore>)),
            MessageStoreType::Sqlite => {
                let db = self.sqlite_db.clone().ok_or_else(|| anyhow::anyhow!("group {}: lastValType=SQLITE but no SQLite DB configured", c.name))?;
                let s = Arc::new(SqliteMessageStore::new(name, db));
                s.ensure_table().await?;
                Ok(Some(s as Arc<dyn MessageStore>))
            }
            MessageStoreType::None => Ok(None),
            other => anyhow::bail!("group {}: lastValType {:?} not implemented in broker.rs yet", c.name, other),
        }
    }

    async fn build_archive_store(&self, c: &ArchiveGroupConfig) -> anyhow::Result<Option<Arc<dyn MessageArchive>>> {
        let name = archive_name(&c.name);
        match c.archive_type {
            MessageArchiveType::Sqlite => {
                let db = self.sqlite_db.clone().ok_or_else(|| anyhow::anyhow!("group {}: archiveType=SQLITE but no SQLite DB configured", c.name))?;
                let a = Arc::new(SqliteMessageArchive::new(name, db, c.payload_format));
                a.ensure_table().await?;
                Ok(Some(a as Arc<dyn MessageArchive>))
            }
            MessageArchiveType::None => Ok(None),
            other => anyhow::bail!("group {}: archiveType {:?} not implemented in broker.rs yet", c.name, other),
        }
    }

    async fn start_group(&self, c: ArchiveGroupConfig) -> anyhow::Result<()> {
        if !c.enabled { return Ok(()); }
        let lv = self.build_last_val_store(&c).await?;
        let ar = self.build_archive_store(&c).await?;
        let g = Group::new(c.clone(), lv, ar);
        g.start();
        self.groups.write().insert(c.name.clone(), g);
        self.record_error(&c.name, None);
        tracing::info!(name = c.name, filters = ?c.topic_filters, last_val = c.last_val_type.as_str(), archive = c.archive_type.as_str(), "archive group started");
        Ok(())
    }

    pub async fn reload(&self) -> anyhow::Result<()> {
        let configs = self.storage.archive_config.get_all().await?;
        let wanted: HashMap<String, ArchiveGroupConfig> = configs.into_iter()
            .filter(|c| c.enabled)
            .map(|c| (c.name.clone(), c))
            .collect();

        let to_stop: Vec<(String, Arc<Group>)> = {
            let r = self.groups.read();
            r.iter().filter_map(|(k, v)| {
                let stop = match wanted.get(k) {
                    None => true,
                    Some(w) => !configs_equal(v.config(), w),
                };
                if stop { Some((k.clone(), v.clone())) } else { None }
            }).collect()
        };
        for (name, g) in to_stop {
            g.stop().await;
            self.groups.write().remove(&name);
        }
        for (name, c) in wanted {
            if self.groups.read().contains_key(&name) { continue; }
            if let Err(e) = self.start_group(c).await {
                self.record_error(&name, Some(e.to_string()));
                tracing::error!(name, err = %e, "archive group reload failed");
            }
        }
        Ok(())
    }

    /// Deliver `msg` to every matching live group.
    pub fn dispatch(&self, msg: BrokerMessage) {
        let r = self.groups.read();
        for g in r.values() {
            if g.matches(&msg.topic_name, msg.is_retain) {
                g.submit(msg.clone());
            }
        }
    }

    pub async fn stop(&self) {
        let groups: Vec<Arc<Group>> = self.groups.read().values().cloned().collect();
        for g in groups { g.stop().await; }
        self.groups.write().clear();
    }

    pub fn snapshot(&self) -> Vec<Arc<Group>> {
        self.groups.read().values().cloned().collect()
    }
}

fn configs_equal(a: &ArchiveGroupConfig, b: &ArchiveGroupConfig) -> bool {
    a.name == b.name
        && a.enabled == b.enabled
        && a.retained_only == b.retained_only
        && a.last_val_type == b.last_val_type
        && a.archive_type == b.archive_type
        && a.payload_format == b.payload_format
        && a.last_val_retention == b.last_val_retention
        && a.archive_retention == b.archive_retention
        && a.topic_filters == b.topic_filters
}
