use super::db::Db;
use crate::stores::traits::{SessionStore, StoreResult};
use crate::stores::types::{MqttSubscription, SessionInfo};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Row;

pub struct SqliteSessionStore { db: Db }

impl SqliteSessionStore {
    pub fn new(db: Db) -> Self { Self { db } }
}

fn parse_dt(s: Option<&str>) -> DateTime<Utc> {
    let Some(s) = s else { return Utc::now(); };
    if let Ok(t) = DateTime::parse_from_rfc3339(s) { return t.with_timezone(&Utc); }
    if let Ok(t) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") { return DateTime::<Utc>::from_naive_utc_and_offset(t, Utc); }
    if let Ok(t) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") { return DateTime::<Utc>::from_naive_utc_and_offset(t, Utc); }
    Utc::now()
}

#[async_trait]
impl SessionStore for SqliteSessionStore {
    async fn ensure_table(&self) -> StoreResult<()> {
        sqlx::query(r#"CREATE TABLE IF NOT EXISTS sessions (
            client_id TEXT PRIMARY KEY,
            node_id TEXT,
            clean_session BOOLEAN,
            connected BOOLEAN,
            update_time TEXT DEFAULT CURRENT_TIMESTAMP,
            information TEXT,
            last_will_topic TEXT,
            last_will_message BLOB,
            last_will_qos INTEGER,
            last_will_retain BOOLEAN
        )"#).execute(&self.db.pool).await?;
        sqlx::query(r#"CREATE TABLE IF NOT EXISTS subscriptions (
            client_id TEXT,
            topic TEXT,
            qos INTEGER,
            wildcard BOOLEAN,
            no_local INTEGER DEFAULT 0,
            retain_handling INTEGER DEFAULT 0,
            retain_as_published INTEGER DEFAULT 0,
            PRIMARY KEY (client_id, topic)
        )"#).execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS subscriptions_topic_idx ON subscriptions (topic)").execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS subscriptions_wildcard_idx ON subscriptions (wildcard) WHERE wildcard = 1").execute(&self.db.pool).await?;
        Ok(())
    }

    async fn set_client(&self, info: SessionInfo) -> StoreResult<()> {
        sqlx::query(r#"INSERT INTO sessions (client_id, node_id, clean_session, connected, update_time, information)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
            ON CONFLICT (client_id) DO UPDATE SET
                node_id = excluded.node_id,
                clean_session = excluded.clean_session,
                connected = excluded.connected,
                update_time = excluded.update_time,
                information = excluded.information"#)
            .bind(&info.client_id).bind(&info.node_id)
            .bind(info.clean_session).bind(info.connected)
            .bind(&info.information)
            .execute(&self.db.pool).await?;
        Ok(())
    }

    async fn set_connected(&self, client_id: &str, connected: bool) -> StoreResult<()> {
        sqlx::query("UPDATE sessions SET connected = ?, update_time = CURRENT_TIMESTAMP WHERE client_id = ?")
            .bind(connected).bind(client_id).execute(&self.db.pool).await?;
        Ok(())
    }

    async fn set_last_will(&self, client_id: &str, topic: &str, payload: &[u8], qos: u8, retain: bool) -> StoreResult<()> {
        sqlx::query("UPDATE sessions SET last_will_topic = ?, last_will_message = ?, last_will_qos = ?, last_will_retain = ? WHERE client_id = ?")
            .bind(topic).bind(payload).bind(qos as i64).bind(retain).bind(client_id)
            .execute(&self.db.pool).await?;
        Ok(())
    }

    async fn is_connected(&self, client_id: &str) -> StoreResult<bool> {
        let row = sqlx::query("SELECT connected FROM sessions WHERE client_id = ?").bind(client_id).fetch_optional(&self.db.pool).await?;
        Ok(row.map(|r| r.try_get::<bool, _>("connected").unwrap_or(false)).unwrap_or(false))
    }

    async fn is_present(&self, client_id: &str) -> StoreResult<bool> {
        let row = sqlx::query("SELECT 1 FROM sessions WHERE client_id = ?").bind(client_id).fetch_optional(&self.db.pool).await?;
        Ok(row.is_some())
    }

    async fn get_session(&self, client_id: &str) -> StoreResult<Option<SessionInfo>> {
        let row = sqlx::query(r#"SELECT client_id, node_id, clean_session, connected, update_time, information,
                                last_will_topic, last_will_message, last_will_qos, last_will_retain
                         FROM sessions WHERE client_id = ?"#).bind(client_id).fetch_optional(&self.db.pool).await?;
        Ok(row.map(|r| {
            let lw_payload: Option<Vec<u8>> = r.try_get("last_will_message").ok();
            let lw_qos: Option<i64> = r.try_get("last_will_qos").ok();
            let update_str: Option<String> = r.try_get("update_time").ok();
            SessionInfo {
                client_id: r.try_get::<String, _>("client_id").unwrap_or_default(),
                node_id: r.try_get::<Option<String>, _>("node_id").ok().flatten().unwrap_or_default(),
                clean_session: r.try_get("clean_session").unwrap_or(false),
                connected: r.try_get("connected").unwrap_or(false),
                update_time: parse_dt(update_str.as_deref()),
                information: r.try_get::<Option<String>, _>("information").ok().flatten().unwrap_or_default(),
                last_will_topic: r.try_get::<Option<String>, _>("last_will_topic").ok().flatten().unwrap_or_default(),
                last_will_payload: lw_payload.unwrap_or_default(),
                last_will_qos: lw_qos.unwrap_or(0) as u8,
                last_will_retain: r.try_get("last_will_retain").unwrap_or(false),
                ..Default::default()
            }
        }))
    }

    async fn iterate_sessions(&self) -> StoreResult<Vec<SessionInfo>> {
        let rows = sqlx::query(r#"SELECT client_id, node_id, clean_session, connected, update_time, information,
                                  last_will_topic, last_will_message, last_will_qos, last_will_retain FROM sessions"#)
            .fetch_all(&self.db.pool).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let lw_payload: Option<Vec<u8>> = r.try_get("last_will_message").ok();
            let lw_qos: Option<i64> = r.try_get("last_will_qos").ok();
            let update_str: Option<String> = r.try_get("update_time").ok();
            out.push(SessionInfo {
                client_id: r.try_get::<String, _>("client_id").unwrap_or_default(),
                node_id: r.try_get::<Option<String>, _>("node_id").ok().flatten().unwrap_or_default(),
                clean_session: r.try_get("clean_session").unwrap_or(false),
                connected: r.try_get("connected").unwrap_or(false),
                update_time: parse_dt(update_str.as_deref()),
                information: r.try_get::<Option<String>, _>("information").ok().flatten().unwrap_or_default(),
                last_will_topic: r.try_get::<Option<String>, _>("last_will_topic").ok().flatten().unwrap_or_default(),
                last_will_payload: lw_payload.unwrap_or_default(),
                last_will_qos: lw_qos.unwrap_or(0) as u8,
                last_will_retain: r.try_get("last_will_retain").unwrap_or(false),
                ..Default::default()
            });
        }
        Ok(out)
    }

    async fn iterate_subscriptions(&self) -> StoreResult<Vec<MqttSubscription>> {
        let rows = sqlx::query("SELECT client_id, topic, qos, no_local, retain_handling, retain_as_published FROM subscriptions")
            .fetch_all(&self.db.pool).await?;
        Ok(rows.into_iter().map(|r| MqttSubscription {
            client_id: r.try_get("client_id").unwrap_or_default(),
            topic_filter: r.try_get("topic").unwrap_or_default(),
            qos: r.try_get::<i64, _>("qos").unwrap_or(0) as u8,
            no_local: r.try_get::<i64, _>("no_local").unwrap_or(0) == 1,
            retain_handling: r.try_get::<i64, _>("retain_handling").unwrap_or(0) as u8,
            retain_as_published: r.try_get::<i64, _>("retain_as_published").unwrap_or(0) == 1,
            ..Default::default()
        }).collect())
    }

    async fn get_subscriptions_for_client(&self, client_id: &str) -> StoreResult<Vec<MqttSubscription>> {
        let rows = sqlx::query("SELECT client_id, topic, qos, no_local, retain_handling, retain_as_published FROM subscriptions WHERE client_id = ?")
            .bind(client_id).fetch_all(&self.db.pool).await?;
        Ok(rows.into_iter().map(|r| MqttSubscription {
            client_id: r.try_get("client_id").unwrap_or_default(),
            topic_filter: r.try_get("topic").unwrap_or_default(),
            qos: r.try_get::<i64, _>("qos").unwrap_or(0) as u8,
            no_local: r.try_get::<i64, _>("no_local").unwrap_or(0) == 1,
            retain_handling: r.try_get::<i64, _>("retain_handling").unwrap_or(0) as u8,
            retain_as_published: r.try_get::<i64, _>("retain_as_published").unwrap_or(0) == 1,
            ..Default::default()
        }).collect())
    }

    async fn add_subscriptions(&self, subs: &[MqttSubscription]) -> StoreResult<()> {
        if subs.is_empty() { return Ok(()); }
        let mut tx = self.db.pool.begin().await?;
        for s in subs {
            let wildcard = s.topic_filter.contains('+') || s.topic_filter.contains('#');
            sqlx::query(r#"INSERT INTO subscriptions (client_id, topic, qos, wildcard, no_local, retain_handling, retain_as_published)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (client_id, topic) DO UPDATE SET
                    qos = excluded.qos,
                    wildcard = excluded.wildcard,
                    no_local = excluded.no_local,
                    retain_handling = excluded.retain_handling,
                    retain_as_published = excluded.retain_as_published"#)
                .bind(&s.client_id).bind(&s.topic_filter).bind(s.qos as i64)
                .bind(wildcard)
                .bind(if s.no_local { 1i64 } else { 0 })
                .bind(s.retain_handling as i64)
                .bind(if s.retain_as_published { 1i64 } else { 0 })
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn del_subscriptions(&self, subs: &[MqttSubscription]) -> StoreResult<()> {
        if subs.is_empty() { return Ok(()); }
        let mut tx = self.db.pool.begin().await?;
        for s in subs {
            sqlx::query("DELETE FROM subscriptions WHERE client_id = ? AND topic = ?")
                .bind(&s.client_id).bind(&s.topic_filter).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn del_client(&self, client_id: &str) -> StoreResult<()> {
        let mut tx = self.db.pool.begin().await?;
        sqlx::query("DELETE FROM subscriptions WHERE client_id = ?").bind(client_id).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM sessions WHERE client_id = ?").bind(client_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn purge_sessions(&self) -> StoreResult<()> {
        let mut tx = self.db.pool.begin().await?;
        sqlx::query("DELETE FROM subscriptions WHERE client_id IN (SELECT client_id FROM sessions WHERE clean_session = 1 AND connected = 0)")
            .execute(&mut *tx).await?;
        sqlx::query("DELETE FROM sessions WHERE clean_session = 1 AND connected = 0")
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}
