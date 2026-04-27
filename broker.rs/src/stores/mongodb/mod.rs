//! MongoDB store backend — TODO.
//!
//! The Go broker.go has full MongoDB parity (`internal/stores/mongodb/mongodb.go`).
//! Porting requires the `mongodb` crate; documents must use the same field
//! names as the SQL schemas. Keeping this module as a placeholder so the
//! trait-routing surface and configuration shape stay accurate.

use crate::config::Config;
use crate::stores::storage::Storage;

pub async fn build(_cfg: &Config) -> anyhow::Result<Storage> {
    anyhow::bail!("MongoDB backend not implemented in broker.rs yet — use SQLITE")
}
