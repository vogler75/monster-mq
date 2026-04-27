use super::db::Db;
use crate::stores::traits::{QueueStore, StoreResult};
use crate::stores::types::BrokerMessage;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use sqlx::Row;
use std::time::Duration;

pub struct SqliteQueueStore {
    db: Db,
    visibility_timeout: Duration,
}

impl SqliteQueueStore {
    pub fn new(db: Db, visibility_timeout: Duration) -> Self {
        let visibility_timeout = if visibility_timeout.is_zero() { Duration::from_secs(30) } else { visibility_timeout };
        Self { db, visibility_timeout }
    }
}

#[async_trait]
impl QueueStore for SqliteQueueStore {
    async fn ensure_table(&self) -> StoreResult<()> {
        sqlx::query(r#"CREATE TABLE IF NOT EXISTS messagequeue (
            msg_id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_uuid TEXT NOT NULL,
            client_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            payload BLOB,
            qos INTEGER NOT NULL,
            retained INTEGER NOT NULL DEFAULT 0,
            publisher_id TEXT,
            creation_time INTEGER NOT NULL,
            message_expiry_interval INTEGER,
            vt INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            read_ct INTEGER NOT NULL DEFAULT 0
        )"#).execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS messagequeue_fetch_idx ON messagequeue (client_id, vt)").execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS messagequeue_client_uuid_idx ON messagequeue (client_id, message_uuid)").execute(&self.db.pool).await?;
        Ok(())
    }

    async fn enqueue(&self, client_id: &str, msg: &BrokerMessage) -> StoreResult<()> {
        self.enqueue_multi(msg, &[client_id.to_string()]).await
    }

    async fn enqueue_multi(&self, msg: &BrokerMessage, client_ids: &[String]) -> StoreResult<()> {
        if client_ids.is_empty() { return Ok(()); }
        let mut tx = self.db.pool.begin().await?;
        for cid in client_ids {
            sqlx::query(r#"INSERT INTO messagequeue
                (message_uuid, client_id, topic, payload, qos, retained, publisher_id, creation_time, message_expiry_interval)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#)
                .bind(&msg.message_uuid).bind(cid)
                .bind(&msg.topic_name).bind(&msg.payload)
                .bind(msg.qos as i64)
                .bind(if msg.is_retain { 1i64 } else { 0 })
                .bind(&msg.client_id)
                .bind(msg.time.timestamp_millis())
                .bind(msg.message_expiry_interval.map(|e| e as i64))
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn dequeue(&self, client_id: &str, batch_size: i64) -> StoreResult<Vec<BrokerMessage>> {
        let limit = if batch_size <= 0 { 10 } else { batch_size };
        let now = Utc::now().timestamp();
        let new_vt = now + self.visibility_timeout.as_secs() as i64;

        let mut tx = self.db.pool.begin().await?;
        let rows = sqlx::query(r#"SELECT msg_id, message_uuid, topic, payload, qos, retained, publisher_id, creation_time, message_expiry_interval
                                FROM messagequeue WHERE client_id = ? AND vt <= ? ORDER BY msg_id LIMIT ?"#)
            .bind(client_id).bind(now).bind(limit).fetch_all(&mut *tx).await?;
        let mut out = Vec::with_capacity(rows.len());
        let mut ids = Vec::with_capacity(rows.len());
        for r in rows {
            let mid: i64 = r.try_get("msg_id")?;
            let muuid: String = r.try_get("message_uuid").unwrap_or_default();
            let topic: String = r.try_get("topic").unwrap_or_default();
            let payload: Vec<u8> = r.try_get("payload").unwrap_or_default();
            let qos: i64 = r.try_get("qos").unwrap_or(0);
            let retained: i64 = r.try_get("retained").unwrap_or(0);
            let pub_id: Option<String> = r.try_get("publisher_id").ok();
            let created: i64 = r.try_get("creation_time").unwrap_or(0);
            let expiry: Option<i64> = r.try_get("message_expiry_interval").ok();
            let mut msg = BrokerMessage {
                message_uuid: muuid,
                topic_name: topic,
                payload,
                qos: qos as u8,
                is_retain: retained == 1,
                is_queued: true,
                client_id: pub_id.unwrap_or_default(),
                time: Utc.timestamp_millis_opt(created).single().unwrap_or_else(Utc::now),
                ..Default::default()
            };
            if let Some(e) = expiry { msg.message_expiry_interval = Some(e as u32); }
            out.push(msg);
            ids.push(mid);
        }
        for id in ids {
            sqlx::query("UPDATE messagequeue SET vt = ?, read_ct = read_ct + 1 WHERE msg_id = ?")
                .bind(new_vt).bind(id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(out)
    }

    async fn ack(&self, client_id: &str, message_uuid: &str) -> StoreResult<()> {
        sqlx::query("DELETE FROM messagequeue WHERE client_id = ? AND message_uuid = ?")
            .bind(client_id).bind(message_uuid).execute(&self.db.pool).await?;
        Ok(())
    }

    async fn purge_for_client(&self, client_id: &str) -> StoreResult<i64> {
        let r = sqlx::query("DELETE FROM messagequeue WHERE client_id = ?").bind(client_id).execute(&self.db.pool).await?;
        Ok(r.rows_affected() as i64)
    }

    async fn count(&self, client_id: &str) -> StoreResult<i64> {
        let row = sqlx::query("SELECT COUNT(*) AS c FROM messagequeue WHERE client_id = ?").bind(client_id).fetch_one(&self.db.pool).await?;
        Ok(row.try_get::<i64, _>("c").unwrap_or(0))
    }

    async fn count_all(&self) -> StoreResult<i64> {
        let row = sqlx::query("SELECT COUNT(*) AS c FROM messagequeue").fetch_one(&self.db.pool).await?;
        Ok(row.try_get::<i64, _>("c").unwrap_or(0))
    }
}
