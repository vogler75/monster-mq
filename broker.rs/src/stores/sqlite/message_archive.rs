use super::db::Db;
use crate::stores::traits::{MessageArchive, StoreResult};
use crate::stores::types::{ArchivedMessage, BrokerMessage, MessageArchiveType, PayloadFormat, PurgeResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;

pub struct SqliteMessageArchive {
    name: String,
    table: String,
    db: Db,
    format: PayloadFormat,
}

impl SqliteMessageArchive {
    pub fn new(name: impl Into<String>, db: Db, format: PayloadFormat) -> Self {
        let name = name.into();
        let table = name.to_ascii_lowercase();
        Self { name, table, db, format }
    }
}

fn is_probably_json(b: &[u8]) -> bool {
    for &c in b {
        if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' { continue; }
        return c == b'{' || c == b'[' || c == b'"' || (c >= b'0' && c <= b'9') || c == b'-' || c == b't' || c == b'f' || c == b'n';
    }
    false
}

#[async_trait]
impl MessageArchive for SqliteMessageArchive {
    fn name(&self) -> &str { &self.name }
    fn archive_type(&self) -> MessageArchiveType { MessageArchiveType::Sqlite }

    async fn ensure_table(&self) -> StoreResult<()> {
        let create = format!(
            "CREATE TABLE IF NOT EXISTS {t} (
                topic TEXT NOT NULL,
                time TEXT NOT NULL,
                payload_blob BLOB,
                payload_json TEXT,
                qos INTEGER,
                retained BOOLEAN,
                client_id TEXT,
                message_uuid TEXT,
                PRIMARY KEY (topic, time)
            )", t = self.table
        );
        sqlx::query(&create).execute(&self.db.pool).await?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS {t}_time_idx ON {t} (time)", t = self.table))
            .execute(&self.db.pool).await?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS {t}_topic_time_idx ON {t} (topic, time)", t = self.table))
            .execute(&self.db.pool).await?;
        Ok(())
    }

    async fn add_history(&self, msgs: &[BrokerMessage]) -> StoreResult<()> {
        if msgs.is_empty() { return Ok(()); }
        let q = format!(
            "INSERT INTO {t} (topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (topic, time) DO NOTHING",
            t = self.table
        );
        let mut tx = self.db.pool.begin().await?;
        for m in msgs {
            let (blob, json): (Option<&[u8]>, Option<String>) =
                if matches!(self.format, PayloadFormat::Json) && is_probably_json(&m.payload) {
                    (None, Some(String::from_utf8_lossy(&m.payload).into_owned()))
                } else {
                    (Some(m.payload.as_slice()), None)
                };
            sqlx::query(&q)
                .bind(&m.topic_name)
                .bind(m.time.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
                .bind(blob.map(|b| b.to_vec()))
                .bind(json)
                .bind(m.qos as i64)
                .bind(m.is_retain)
                .bind(&m.client_id)
                .bind(&m.message_uuid)
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn get_history(&self, topic_filter: &str, from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>, limit: i64) -> StoreResult<Vec<ArchivedMessage>> {
        let limit = if limit <= 0 { 100 } else { limit };
        let pattern = topic_filter.replace('#', "%").replace('+', "%");
        let mut q = format!("SELECT topic, time, payload_blob, qos, client_id FROM {} WHERE topic LIKE ?", self.table);
        if from.is_some() { q.push_str(" AND time >= ?"); }
        if to.is_some()   { q.push_str(" AND time <= ?"); }
        q.push_str(" ORDER BY time DESC LIMIT ?");

        let mut sql_q = sqlx::query(&q).bind(pattern);
        if let Some(f) = from { sql_q = sql_q.bind(f.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)); }
        if let Some(t) = to   { sql_q = sql_q.bind(t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)); }
        sql_q = sql_q.bind(limit);

        let rows = sql_q.fetch_all(&self.db.pool).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let topic: String = r.try_get("topic")?;
            let time_s: String = r.try_get("time")?;
            let payload: Vec<u8> = r.try_get("payload_blob").unwrap_or_default();
            let qos: i64 = r.try_get("qos").unwrap_or(0);
            let cid: Option<String> = r.try_get("client_id").ok();
            let ts = DateTime::parse_from_rfc3339(&time_s).map(|d| d.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now());
            out.push(ArchivedMessage { topic, timestamp: ts, payload, qos: qos as u8, client_id: cid.unwrap_or_default() });
        }
        Ok(out)
    }

    async fn purge_older_than(&self, t: DateTime<Utc>) -> StoreResult<PurgeResult> {
        let r = sqlx::query(&format!("DELETE FROM {} WHERE time < ?", self.table))
            .bind(t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
            .execute(&self.db.pool).await?;
        Ok(PurgeResult { deleted_rows: r.rows_affected() as i64 })
    }
}
