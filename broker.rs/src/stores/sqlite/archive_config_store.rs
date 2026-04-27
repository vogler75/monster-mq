use super::db::Db;
use crate::stores::traits::{ArchiveConfigStore, StoreResult};
use crate::stores::types::{ArchiveGroupConfig, MessageArchiveType, MessageStoreType, PayloadFormat};
use async_trait::async_trait;
use sqlx::Row;

pub struct SqliteArchiveConfigStore { db: Db }

impl SqliteArchiveConfigStore { pub fn new(db: Db) -> Self { Self { db } } }

fn split_filter(s: &str) -> Vec<String> {
    if s.is_empty() { return vec![]; }
    s.split(',').map(|s| s.trim().to_string()).collect()
}

fn join_filter(s: &[String]) -> String { s.join(",") }

fn null_str(s: &str) -> Option<&str> { if s.is_empty() { None } else { Some(s) } }

#[async_trait]
impl ArchiveConfigStore for SqliteArchiveConfigStore {
    async fn ensure_table(&self) -> StoreResult<()> {
        sqlx::query(r#"CREATE TABLE IF NOT EXISTS archiveconfigs (
            name TEXT PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 0,
            topic_filter TEXT NOT NULL,
            retained_only INTEGER NOT NULL DEFAULT 0,
            last_val_type TEXT NOT NULL,
            archive_type TEXT NOT NULL,
            last_val_retention TEXT,
            archive_retention TEXT,
            purge_interval TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            payload_format TEXT DEFAULT 'DEFAULT'
        )"#).execute(&self.db.pool).await?;
        Ok(())
    }

    async fn get_all(&self) -> StoreResult<Vec<ArchiveGroupConfig>> {
        let rows = sqlx::query("SELECT name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format FROM archiveconfigs ORDER BY name")
            .fetch_all(&self.db.pool).await?;
        Ok(rows.into_iter().map(|r| ArchiveGroupConfig {
            name: r.try_get("name").unwrap_or_default(),
            enabled: r.try_get::<i64, _>("enabled").unwrap_or(0) == 1,
            topic_filters: split_filter(&r.try_get::<String, _>("topic_filter").unwrap_or_default()),
            retained_only: r.try_get::<i64, _>("retained_only").unwrap_or(0) == 1,
            last_val_type: MessageStoreType::parse(&r.try_get::<String, _>("last_val_type").unwrap_or_default()),
            archive_type: MessageArchiveType::parse(&r.try_get::<String, _>("archive_type").unwrap_or_default()),
            last_val_retention: r.try_get::<Option<String>, _>("last_val_retention").ok().flatten().unwrap_or_default(),
            archive_retention: r.try_get::<Option<String>, _>("archive_retention").ok().flatten().unwrap_or_default(),
            purge_interval: r.try_get::<Option<String>, _>("purge_interval").ok().flatten().unwrap_or_default(),
            payload_format: PayloadFormat::parse(&r.try_get::<Option<String>, _>("payload_format").ok().flatten().unwrap_or_else(|| "DEFAULT".to_string())),
        }).collect())
    }

    async fn get(&self, name: &str) -> StoreResult<Option<ArchiveGroupConfig>> {
        let row = sqlx::query("SELECT name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format FROM archiveconfigs WHERE name = ?")
            .bind(name).fetch_optional(&self.db.pool).await?;
        Ok(row.map(|r| ArchiveGroupConfig {
            name: r.try_get("name").unwrap_or_default(),
            enabled: r.try_get::<i64, _>("enabled").unwrap_or(0) == 1,
            topic_filters: split_filter(&r.try_get::<String, _>("topic_filter").unwrap_or_default()),
            retained_only: r.try_get::<i64, _>("retained_only").unwrap_or(0) == 1,
            last_val_type: MessageStoreType::parse(&r.try_get::<String, _>("last_val_type").unwrap_or_default()),
            archive_type: MessageArchiveType::parse(&r.try_get::<String, _>("archive_type").unwrap_or_default()),
            last_val_retention: r.try_get::<Option<String>, _>("last_val_retention").ok().flatten().unwrap_or_default(),
            archive_retention: r.try_get::<Option<String>, _>("archive_retention").ok().flatten().unwrap_or_default(),
            purge_interval: r.try_get::<Option<String>, _>("purge_interval").ok().flatten().unwrap_or_default(),
            payload_format: PayloadFormat::parse(&r.try_get::<Option<String>, _>("payload_format").ok().flatten().unwrap_or_else(|| "DEFAULT".to_string())),
        }))
    }

    async fn save(&self, cfg: &ArchiveGroupConfig) -> StoreResult<()> {
        sqlx::query(r#"INSERT INTO archiveconfigs (name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (name) DO UPDATE SET
                enabled = excluded.enabled,
                topic_filter = excluded.topic_filter,
                retained_only = excluded.retained_only,
                last_val_type = excluded.last_val_type,
                archive_type = excluded.archive_type,
                last_val_retention = excluded.last_val_retention,
                archive_retention = excluded.archive_retention,
                purge_interval = excluded.purge_interval,
                payload_format = excluded.payload_format,
                updated_at = CURRENT_TIMESTAMP"#)
            .bind(&cfg.name)
            .bind(if cfg.enabled { 1i64 } else { 0 })
            .bind(join_filter(&cfg.topic_filters))
            .bind(if cfg.retained_only { 1i64 } else { 0 })
            .bind(cfg.last_val_type.as_str())
            .bind(cfg.archive_type.as_str())
            .bind(null_str(&cfg.last_val_retention))
            .bind(null_str(&cfg.archive_retention))
            .bind(null_str(&cfg.purge_interval))
            .bind(cfg.payload_format.as_str())
            .execute(&self.db.pool).await?;
        Ok(())
    }

    async fn update(&self, cfg: &ArchiveGroupConfig) -> StoreResult<()> { self.save(cfg).await }

    async fn delete(&self, name: &str) -> StoreResult<()> {
        sqlx::query("DELETE FROM archiveconfigs WHERE name = ?").bind(name).execute(&self.db.pool).await?;
        Ok(())
    }
}
