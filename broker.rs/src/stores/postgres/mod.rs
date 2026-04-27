//! PostgreSQL store backend — TODO.
//!
//! The Go broker.go has full Postgres parity (`internal/stores/postgres/postgres.go`).
//! Porting it requires `sqlx` with the postgres feature and the same DDL
//! (BIGSERIAL/BYTEA/JSONB/TIMESTAMPTZ). Keeping this module as a placeholder
//! so the trait-routing surface and configuration shape stay accurate.
//!
//! Until implemented: setting `DefaultStoreType: POSTGRES` will fail at startup
//! with a descriptive error.

use crate::config::Config;
use crate::stores::storage::Storage;

pub async fn build(_cfg: &Config) -> anyhow::Result<Storage> {
    anyhow::bail!("Postgres backend not implemented in broker.rs yet — use SQLITE")
}
