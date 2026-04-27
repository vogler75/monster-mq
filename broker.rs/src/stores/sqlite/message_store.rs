use super::db::Db;
use crate::stores::topic::{match_topic, split_topic, MAX_FIXED_TOPIC_LEVELS};
use crate::stores::traits::{MessageStore, StoreResult};
use crate::stores::types::{BrokerMessage, MessageStoreType, PurgeResult};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use sqlx::Row;

/// SQLite implementation of MessageStore. Schema is byte-compatible with the
/// Kotlin/Go MessageStoreSQLite (topic PK + topic_1..topic_9 + topic_r JSON +
/// payload_blob/payload_json + qos/retained/client_id/message_uuid +
/// creation_time/message_expiry_interval).
pub struct SqliteMessageStore {
    name: String,
    table: String,
    db: Db,
}

impl SqliteMessageStore {
    pub fn new(name: impl Into<String>, db: Db) -> Self {
        let name = name.into();
        let table = name.to_ascii_lowercase();
        Self { name, table, db }
    }
}

#[async_trait]
impl MessageStore for SqliteMessageStore {
    fn name(&self) -> &str { &self.name }
    fn store_type(&self) -> MessageStoreType { MessageStoreType::Sqlite }

    async fn ensure_table(&self) -> StoreResult<()> {
        let mut cols = String::new();
        for i in 1..=MAX_FIXED_TOPIC_LEVELS {
            cols.push_str(&format!("topic_{} TEXT NOT NULL DEFAULT '', ", i));
        }
        let mut idx_cols = String::new();
        for i in 1..=MAX_FIXED_TOPIC_LEVELS {
            if i > 1 { idx_cols.push_str(", "); }
            idx_cols.push_str(&format!("topic_{}", i));
        }
        idx_cols.push_str(", topic_r");

        let create = format!(
            "CREATE TABLE IF NOT EXISTS {t} (
                topic TEXT PRIMARY KEY,
                {cols}
                topic_r TEXT,
                topic_l TEXT NOT NULL,
                time TEXT,
                payload_blob BLOB,
                payload_json TEXT,
                qos INTEGER,
                retained BOOLEAN,
                client_id TEXT,
                message_uuid TEXT,
                creation_time INTEGER,
                message_expiry_interval INTEGER
            )", t = self.table, cols = cols
        );
        sqlx::query(&create).execute(&self.db.pool).await?;
        let idx = format!("CREATE INDEX IF NOT EXISTS {t}_topic ON {t} ({c})", t = self.table, c = idx_cols);
        sqlx::query(&idx).execute(&self.db.pool).await?;
        Ok(())
    }

    async fn get(&self, topic: &str) -> StoreResult<Option<BrokerMessage>> {
        let q = format!(
            "SELECT payload_blob, qos, retained, client_id, message_uuid, creation_time, message_expiry_interval FROM {} WHERE topic = ?",
            self.table
        );
        let row = sqlx::query(&q).bind(topic).fetch_optional(&self.db.pool).await?;
        let Some(row) = row else { return Ok(None) };
        let payload: Vec<u8> = row.try_get("payload_blob").unwrap_or_default();
        let qos: i64 = row.try_get("qos").unwrap_or(0);
        let retained: bool = row.try_get("retained").unwrap_or(false);
        let client_id: Option<String> = row.try_get("client_id").unwrap_or(None);
        let muuid: Option<String> = row.try_get("message_uuid").unwrap_or(None);
        let creation: Option<i64> = row.try_get("creation_time").unwrap_or(None);
        let expiry: Option<i64> = row.try_get("message_expiry_interval").unwrap_or(None);
        let mut msg = BrokerMessage {
            message_uuid: muuid.unwrap_or_default(),
            topic_name: topic.to_string(),
            payload,
            qos: qos as u8,
            is_retain: retained,
            client_id: client_id.unwrap_or_default(),
            time: creation
                .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
                .unwrap_or_else(Utc::now),
            ..Default::default()
        };
        if let Some(e) = expiry {
            msg.message_expiry_interval = Some(e as u32);
        }
        Ok(Some(msg))
    }

    async fn add_all(&self, msgs: &[BrokerMessage]) -> StoreResult<()> {
        if msgs.is_empty() {
            return Ok(());
        }
        let mut col_names = String::new();
        let mut placeholders = String::new();
        for i in 1..=MAX_FIXED_TOPIC_LEVELS {
            col_names.push_str(&format!("topic_{}, ", i));
            placeholders.push_str("?, ");
        }
        let q = format!(
            "INSERT INTO {t} (topic, {cols}topic_r, topic_l, time, payload_blob, payload_json, qos, retained, client_id, message_uuid, creation_time, message_expiry_interval)
             VALUES (?, {ph}?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (topic) DO UPDATE SET
                time = excluded.time,
                payload_blob = excluded.payload_blob,
                payload_json = excluded.payload_json,
                qos = excluded.qos,
                retained = excluded.retained,
                client_id = excluded.client_id,
                message_uuid = excluded.message_uuid,
                creation_time = excluded.creation_time,
                message_expiry_interval = excluded.message_expiry_interval",
            t = self.table, cols = col_names, ph = placeholders
        );

        let mut tx = self.db.pool.begin().await?;
        for m in msgs {
            let (fixed, rest_json, last) = split_topic(&m.topic_name);
            let mut sql_q = sqlx::query(&q).bind(&m.topic_name);
            for v in fixed.iter() {
                sql_q = sql_q.bind(v);
            }
            sql_q = sql_q
                .bind(rest_json)
                .bind(last)
                .bind(m.time.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
                .bind(&m.payload)
                .bind::<Option<String>>(None)
                .bind(m.qos as i64)
                .bind(m.is_retain)
                .bind(&m.client_id)
                .bind(&m.message_uuid)
                .bind(m.time.timestamp_millis())
                .bind(m.message_expiry_interval.map(|e| e as i64));
            sql_q.execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn del_all(&self, topics: &[String]) -> StoreResult<()> {
        if topics.is_empty() {
            return Ok(());
        }
        let q = format!("DELETE FROM {} WHERE topic = ?", self.table);
        let mut tx = self.db.pool.begin().await?;
        for t in topics {
            sqlx::query(&q).bind(t).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn find_matching_messages(&self, pattern: &str) -> StoreResult<Vec<BrokerMessage>> {
        let q = format!(
            "SELECT topic, payload_blob, qos, client_id, message_uuid, creation_time, message_expiry_interval FROM {}",
            self.table
        );
        let rows = sqlx::query(&q).fetch_all(&self.db.pool).await?;
        let now_ms = Utc::now().timestamp_millis();
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let topic: String = row.try_get("topic")?;
            if !match_topic(pattern, &topic) {
                continue;
            }
            let payload: Vec<u8> = row.try_get("payload_blob").unwrap_or_default();
            let qos: i64 = row.try_get("qos").unwrap_or(0);
            let client_id: Option<String> = row.try_get("client_id").unwrap_or(None);
            let muuid: Option<String> = row.try_get("message_uuid").unwrap_or(None);
            let creation: Option<i64> = row.try_get("creation_time").unwrap_or(None);
            let expiry: Option<i64> = row.try_get("message_expiry_interval").unwrap_or(None);
            // expiry guard — only when both creation_time and a positive
            // message_expiry_interval are present.
            if let (Some(c), Some(e)) = (creation, expiry) {
                if e > 0 && (now_ms - c) / 1000 >= e {
                    continue;
                }
            }
            let mut msg = BrokerMessage {
                message_uuid: muuid.unwrap_or_default(),
                topic_name: topic,
                payload,
                qos: qos as u8,
                is_retain: true,
                client_id: client_id.unwrap_or_default(),
                time: creation
                    .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
                    .unwrap_or_else(Utc::now),
                ..Default::default()
            };
            if let Some(e) = expiry {
                msg.message_expiry_interval = Some(e as u32);
            }
            out.push(msg);
        }
        Ok(out)
    }

    async fn find_matching_topics(&self, pattern: &str) -> StoreResult<Vec<String>> {
        let q = format!("SELECT topic FROM {}", self.table);
        let rows = sqlx::query(&q).fetch_all(&self.db.pool).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let t: String = r.try_get("topic")?;
            if match_topic(pattern, &t) {
                out.push(t);
            }
        }
        Ok(out)
    }

    async fn purge_older_than(&self, t: DateTime<Utc>) -> StoreResult<PurgeResult> {
        let q = format!("DELETE FROM {} WHERE time < ?", self.table);
        let r = sqlx::query(&q)
            .bind(t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
            .execute(&self.db.pool)
            .await?;
        Ok(PurgeResult { deleted_rows: r.rows_affected() as i64 })
    }
}
