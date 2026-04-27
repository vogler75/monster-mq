use super::db::Db;
use crate::stores::traits::{MetricsStore, StoreResult};
use crate::stores::types::{MetricKind, MetricsRow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;

pub struct SqliteMetricsStore { db: Db }

impl SqliteMetricsStore { pub fn new(db: Db) -> Self { Self { db } } }

#[async_trait]
impl MetricsStore for SqliteMetricsStore {
    async fn ensure_table(&self) -> StoreResult<()> {
        sqlx::query(r#"CREATE TABLE IF NOT EXISTS metrics (
            timestamp TEXT NOT NULL,
            metric_type TEXT NOT NULL,
            identifier TEXT NOT NULL,
            metrics TEXT NOT NULL,
            PRIMARY KEY (timestamp, metric_type, identifier)
        )"#).execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS metrics_timestamp_idx ON metrics (timestamp)").execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS metrics_type_identifier_idx ON metrics (metric_type, identifier, timestamp)").execute(&self.db.pool).await?;
        Ok(())
    }

    async fn store_metrics(&self, kind: MetricKind, ts: DateTime<Utc>, identifier: &str, payload: &str) -> StoreResult<()> {
        sqlx::query("INSERT OR REPLACE INTO metrics (timestamp, metric_type, identifier, metrics) VALUES (?, ?, ?, ?)")
            .bind(ts.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
            .bind(kind.as_str()).bind(identifier).bind(payload)
            .execute(&self.db.pool).await?;
        Ok(())
    }

    async fn get_latest(&self, kind: MetricKind, identifier: &str) -> StoreResult<Option<(DateTime<Utc>, String)>> {
        let row = sqlx::query("SELECT timestamp, metrics FROM metrics WHERE metric_type = ? AND identifier = ? ORDER BY timestamp DESC LIMIT 1")
            .bind(kind.as_str()).bind(identifier).fetch_optional(&self.db.pool).await?;
        Ok(row.map(|r| {
            let ts_s: String = r.try_get("timestamp").unwrap_or_default();
            let payload: String = r.try_get("metrics").unwrap_or_default();
            let ts = DateTime::parse_from_rfc3339(&ts_s).map(|t| t.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now());
            (ts, payload)
        }))
    }

    async fn get_history(&self, kind: MetricKind, identifier: &str, from: DateTime<Utc>, to: DateTime<Utc>, limit: i64) -> StoreResult<Vec<MetricsRow>> {
        let limit = if limit <= 0 { 1000 } else { limit };
        let rows = sqlx::query("SELECT timestamp, metrics FROM metrics WHERE metric_type = ? AND identifier = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC LIMIT ?")
            .bind(kind.as_str()).bind(identifier)
            .bind(from.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
            .bind(to.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
            .bind(limit).fetch_all(&self.db.pool).await?;
        Ok(rows.into_iter().map(|r| {
            let ts_s: String = r.try_get("timestamp").unwrap_or_default();
            let payload: String = r.try_get("metrics").unwrap_or_default();
            let ts = DateTime::parse_from_rfc3339(&ts_s).map(|t| t.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now());
            MetricsRow { timestamp: ts, payload }
        }).collect())
    }

    async fn purge_older_than(&self, t: DateTime<Utc>) -> StoreResult<i64> {
        let r = sqlx::query("DELETE FROM metrics WHERE timestamp < ?")
            .bind(t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
            .execute(&self.db.pool).await?;
        Ok(r.rows_affected() as i64)
    }
}
