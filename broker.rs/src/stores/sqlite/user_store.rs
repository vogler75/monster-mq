use super::db::Db;
use crate::stores::traits::{StoreResult, UserStore};
use crate::stores::types::{AclRule, User};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Row;

pub struct SqliteUserStore { db: Db }

impl SqliteUserStore { pub fn new(db: Db) -> Self { Self { db } } }

fn parse_dt(s: Option<String>) -> Option<DateTime<Utc>> {
    let s = s?;
    let s = s.replace(' ', "T");
    if let Ok(t) = DateTime::parse_from_rfc3339(&s) { return Some(t.with_timezone(&Utc)); }
    if let Ok(t) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") { return Some(DateTime::<Utc>::from_naive_utc_and_offset(t, Utc)); }
    None
}

#[async_trait]
impl UserStore for SqliteUserStore {
    async fn ensure_table(&self) -> StoreResult<()> {
        sqlx::query(r#"CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            enabled BOOLEAN DEFAULT 1,
            can_subscribe BOOLEAN DEFAULT 1,
            can_publish BOOLEAN DEFAULT 1,
            is_admin BOOLEAN DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )"#).execute(&self.db.pool).await?;
        sqlx::query(r#"CREATE TABLE IF NOT EXISTS usersacl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT REFERENCES users(username) ON DELETE CASCADE,
            topic_pattern TEXT NOT NULL,
            can_subscribe BOOLEAN DEFAULT 0,
            can_publish BOOLEAN DEFAULT 0,
            priority INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )"#).execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS usersacl_username_idx ON usersacl (username)").execute(&self.db.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS usersacl_priority_idx ON usersacl (priority)").execute(&self.db.pool).await?;
        Ok(())
    }

    async fn create_user(&self, u: &User) -> StoreResult<()> {
        sqlx::query("INSERT INTO users (username, password_hash, enabled, can_subscribe, can_publish, is_admin) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(&u.username).bind(&u.password_hash)
            .bind(u.enabled).bind(u.can_subscribe).bind(u.can_publish).bind(u.is_admin)
            .execute(&self.db.pool).await?;
        Ok(())
    }

    async fn update_user(&self, u: &User) -> StoreResult<()> {
        sqlx::query("UPDATE users SET password_hash = ?, enabled = ?, can_subscribe = ?, can_publish = ?, is_admin = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?")
            .bind(&u.password_hash)
            .bind(u.enabled).bind(u.can_subscribe).bind(u.can_publish).bind(u.is_admin)
            .bind(&u.username)
            .execute(&self.db.pool).await?;
        Ok(())
    }

    async fn delete_user(&self, username: &str) -> StoreResult<()> {
        sqlx::query("DELETE FROM users WHERE username = ?").bind(username).execute(&self.db.pool).await?;
        Ok(())
    }

    async fn get_user(&self, username: &str) -> StoreResult<Option<User>> {
        let row = sqlx::query("SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM users WHERE username = ?")
            .bind(username).fetch_optional(&self.db.pool).await?;
        Ok(row.map(|r| User {
            username: r.try_get("username").unwrap_or_default(),
            password_hash: r.try_get("password_hash").unwrap_or_default(),
            enabled: r.try_get("enabled").unwrap_or(false),
            can_subscribe: r.try_get("can_subscribe").unwrap_or(false),
            can_publish: r.try_get("can_publish").unwrap_or(false),
            is_admin: r.try_get("is_admin").unwrap_or(false),
            created_at: parse_dt(r.try_get("created_at").ok()),
            updated_at: parse_dt(r.try_get("updated_at").ok()),
        }))
    }

    async fn get_all_users(&self) -> StoreResult<Vec<User>> {
        let rows = sqlx::query("SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM users ORDER BY username")
            .fetch_all(&self.db.pool).await?;
        Ok(rows.into_iter().map(|r| User {
            username: r.try_get("username").unwrap_or_default(),
            password_hash: r.try_get("password_hash").unwrap_or_default(),
            enabled: r.try_get("enabled").unwrap_or(false),
            can_subscribe: r.try_get("can_subscribe").unwrap_or(false),
            can_publish: r.try_get("can_publish").unwrap_or(false),
            is_admin: r.try_get("is_admin").unwrap_or(false),
            created_at: parse_dt(r.try_get("created_at").ok()),
            updated_at: parse_dt(r.try_get("updated_at").ok()),
        }).collect())
    }

    async fn validate_credentials(&self, username: &str, password: &str) -> StoreResult<Option<User>> {
        let Some(u) = self.get_user(username).await? else { return Ok(None); };
        if !u.enabled { return Ok(None); }
        let ok = bcrypt::verify(password, &u.password_hash).unwrap_or(false);
        Ok(if ok { Some(u) } else { None })
    }

    async fn create_acl_rule(&self, r: &AclRule) -> StoreResult<()> {
        sqlx::query("INSERT INTO usersacl (username, topic_pattern, can_subscribe, can_publish, priority) VALUES (?, ?, ?, ?, ?)")
            .bind(&r.username).bind(&r.topic_pattern)
            .bind(r.can_subscribe).bind(r.can_publish).bind(r.priority as i64)
            .execute(&self.db.pool).await?;
        Ok(())
    }

    async fn update_acl_rule(&self, r: &AclRule) -> StoreResult<()> {
        let id_num: i64 = r.id.parse().unwrap_or(-1);
        sqlx::query("UPDATE usersacl SET username = ?, topic_pattern = ?, can_subscribe = ?, can_publish = ?, priority = ? WHERE id = ?")
            .bind(&r.username).bind(&r.topic_pattern)
            .bind(r.can_subscribe).bind(r.can_publish).bind(r.priority as i64)
            .bind(id_num)
            .execute(&self.db.pool).await?;
        Ok(())
    }

    async fn delete_acl_rule(&self, id: &str) -> StoreResult<()> {
        let id_num: i64 = id.parse().unwrap_or(-1);
        sqlx::query("DELETE FROM usersacl WHERE id = ?").bind(id_num).execute(&self.db.pool).await?;
        Ok(())
    }

    async fn get_user_acl_rules(&self, username: &str) -> StoreResult<Vec<AclRule>> {
        let rows = sqlx::query("SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM usersacl WHERE username = ? ORDER BY priority DESC")
            .bind(username).fetch_all(&self.db.pool).await?;
        Ok(rows.into_iter().map(|r| AclRule {
            id: r.try_get::<i64, _>("id").map(|x| x.to_string()).unwrap_or_default(),
            username: r.try_get("username").unwrap_or_default(),
            topic_pattern: r.try_get("topic_pattern").unwrap_or_default(),
            can_subscribe: r.try_get("can_subscribe").unwrap_or(false),
            can_publish: r.try_get("can_publish").unwrap_or(false),
            priority: r.try_get::<i64, _>("priority").unwrap_or(0) as i32,
            created_at: parse_dt(r.try_get("created_at").ok()),
        }).collect())
    }

    async fn get_all_acl_rules(&self) -> StoreResult<Vec<AclRule>> {
        let rows = sqlx::query("SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM usersacl ORDER BY username, priority DESC")
            .fetch_all(&self.db.pool).await?;
        Ok(rows.into_iter().map(|r| AclRule {
            id: r.try_get::<i64, _>("id").map(|x| x.to_string()).unwrap_or_default(),
            username: r.try_get("username").unwrap_or_default(),
            topic_pattern: r.try_get("topic_pattern").unwrap_or_default(),
            can_subscribe: r.try_get("can_subscribe").unwrap_or(false),
            can_publish: r.try_get("can_publish").unwrap_or(false),
            priority: r.try_get::<i64, _>("priority").unwrap_or(0) as i32,
            created_at: parse_dt(r.try_get("created_at").ok()),
        }).collect())
    }

    async fn load_all(&self) -> StoreResult<(Vec<User>, Vec<AclRule>)> {
        let users = self.get_all_users().await?;
        let rules = self.get_all_acl_rules().await?;
        Ok((users, rules))
    }
}
