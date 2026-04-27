use super::db::Db;
use crate::stores::traits::{DeviceConfigStore, StoreResult};
use crate::stores::types::DeviceConfig;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Row;

pub struct SqliteDeviceConfigStore { db: Db }

impl SqliteDeviceConfigStore { pub fn new(db: Db) -> Self { Self { db } } }

fn parse_dt(s: Option<String>) -> Option<DateTime<Utc>> {
    let s = s?;
    let s = s.replace(' ', "T");
    if let Ok(t) = DateTime::parse_from_rfc3339(&s) { return Some(t.with_timezone(&Utc)); }
    if let Ok(t) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") { return Some(DateTime::<Utc>::from_naive_utc_and_offset(t, Utc)); }
    None
}

fn row_to_device(r: sqlx::sqlite::SqliteRow) -> DeviceConfig {
    DeviceConfig {
        name: r.try_get("name").unwrap_or_default(),
        namespace: r.try_get("namespace").unwrap_or_default(),
        node_id: r.try_get("node_id").unwrap_or_default(),
        config: r.try_get("config").unwrap_or_default(),
        enabled: r.try_get::<i64, _>("enabled").unwrap_or(0) == 1,
        r#type: r.try_get("type").unwrap_or_default(),
        created_at: parse_dt(r.try_get("created_at").ok()),
        updated_at: parse_dt(r.try_get("updated_at").ok()),
    }
}

#[async_trait]
impl DeviceConfigStore for SqliteDeviceConfigStore {
    async fn ensure_table(&self) -> StoreResult<()> {
        sqlx::query(r#"CREATE TABLE IF NOT EXISTS deviceconfigs (
            name TEXT PRIMARY KEY,
            namespace TEXT NOT NULL,
            node_id TEXT NOT NULL,
            config TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            type TEXT DEFAULT 'MQTT_CLIENT',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )"#).execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_deviceconfigs_node_id ON deviceconfigs (node_id)").execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_deviceconfigs_enabled ON deviceconfigs (enabled)").execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_deviceconfigs_namespace ON deviceconfigs (namespace)").execute(&self.db.pool).await?;
        Ok(())
    }

    async fn get_all(&self) -> StoreResult<Vec<DeviceConfig>> {
        let rows = sqlx::query("SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM deviceconfigs ORDER BY name")
            .fetch_all(&self.db.pool).await?;
        Ok(rows.into_iter().map(row_to_device).collect())
    }

    async fn get_by_node(&self, node_id: &str) -> StoreResult<Vec<DeviceConfig>> {
        let rows = sqlx::query("SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM deviceconfigs WHERE node_id = ? ORDER BY name")
            .bind(node_id).fetch_all(&self.db.pool).await?;
        Ok(rows.into_iter().map(row_to_device).collect())
    }

    async fn get_enabled_by_node(&self, node_id: &str) -> StoreResult<Vec<DeviceConfig>> {
        let rows = sqlx::query("SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM deviceconfigs WHERE node_id = ? AND enabled = 1 ORDER BY name")
            .bind(node_id).fetch_all(&self.db.pool).await?;
        Ok(rows.into_iter().map(row_to_device).collect())
    }

    async fn get(&self, name: &str) -> StoreResult<Option<DeviceConfig>> {
        let row = sqlx::query("SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM deviceconfigs WHERE name = ?")
            .bind(name).fetch_optional(&self.db.pool).await?;
        Ok(row.map(row_to_device))
    }

    async fn save(&self, d: &DeviceConfig) -> StoreResult<()> {
        sqlx::query(r#"INSERT INTO deviceconfigs (name, namespace, node_id, config, enabled, type, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT (name) DO UPDATE SET
                namespace = excluded.namespace,
                node_id = excluded.node_id,
                config = excluded.config,
                enabled = excluded.enabled,
                type = excluded.type,
                updated_at = datetime('now')"#)
            .bind(&d.name).bind(&d.namespace).bind(&d.node_id).bind(&d.config)
            .bind(if d.enabled { 1i64 } else { 0 })
            .bind(&d.r#type)
            .execute(&self.db.pool).await?;
        Ok(())
    }

    async fn delete(&self, name: &str) -> StoreResult<()> {
        sqlx::query("DELETE FROM deviceconfigs WHERE name = ?").bind(name).execute(&self.db.pool).await?;
        Ok(())
    }

    async fn toggle(&self, name: &str, enabled: bool) -> StoreResult<Option<DeviceConfig>> {
        sqlx::query("UPDATE deviceconfigs SET enabled = ?, updated_at = datetime('now') WHERE name = ?")
            .bind(if enabled { 1i64 } else { 0 }).bind(name).execute(&self.db.pool).await?;
        self.get(name).await
    }

    async fn reassign(&self, name: &str, node_id: &str) -> StoreResult<Option<DeviceConfig>> {
        sqlx::query("UPDATE deviceconfigs SET node_id = ?, updated_at = datetime('now') WHERE name = ?")
            .bind(node_id).bind(name).execute(&self.db.pool).await?;
        self.get(name).await
    }
}
