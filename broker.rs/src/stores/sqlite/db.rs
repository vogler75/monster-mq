use anyhow::Context;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Wraps the sqlx SqlitePool with the same WAL+busy_timeout settings as the Go
/// broker (so the same DB file is byte-compatible).
#[derive(Clone)]
pub struct Db {
    pub pool: SqlitePool,
    pub path: PathBuf,
}

impl Db {
    pub async fn open(path: &str) -> anyhow::Result<Db> {
        let p = Path::new(path);
        if let Some(dir) = p.parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir).with_context(|| format!("mkdir {}", dir.display()))?;
            }
        }
        let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_millis(5000))
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await
            .with_context(|| format!("open sqlite {}", path))?;
        Ok(Db { pool, path: p.to_path_buf() })
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}
