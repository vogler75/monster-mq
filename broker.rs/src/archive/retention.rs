use super::manager::Manager;
use chrono::{Duration, Utc};
use std::sync::Arc;

/// Parses retention strings like "30d", "12h", "5m", "10s" — Go broker uses
/// the same shorthand. Empty / unrecognised strings yield None (= forever).
pub fn parse_retention(s: &str) -> Option<Duration> {
    if s.is_empty() { return None; }
    let s = s.trim();
    if s.len() < 2 { return None; }
    let (num, unit) = s.split_at(s.len() - 1);
    let n: i64 = num.parse().ok()?;
    match unit {
        "d" => Some(Duration::days(n)),
        "h" => Some(Duration::hours(n)),
        "m" => Some(Duration::minutes(n)),
        "s" => Some(Duration::seconds(n)),
        _ => None,
    }
}

pub fn parse_purge_interval(s: &str) -> Option<std::time::Duration> {
    parse_retention(s).and_then(|d| d.to_std().ok())
}

/// Spawn a background task that periodically purges last-value/archive rows
/// older than the configured retention. Per-group; mirrors the Go broker's
/// `archive/retention.go` (one ticker per group).
pub fn start_retention_loop(manager: Arc<Manager>) {
    tokio::spawn(async move {
        let mut t = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            t.tick().await;
            for g in manager.snapshot() {
                let cfg = g.config();
                if let Some(ret) = parse_retention(&cfg.last_val_retention) {
                    if let Some(lv) = g.last_value() {
                        let cutoff = Utc::now() - ret;
                        let _ = lv.purge_older_than(cutoff).await;
                    }
                }
                if let Some(ret) = parse_retention(&cfg.archive_retention) {
                    if let Some(ar) = g.archive() {
                        let cutoff = Utc::now() - ret;
                        let _ = ar.purge_older_than(cutoff).await;
                    }
                }
            }
        }
    });
}
