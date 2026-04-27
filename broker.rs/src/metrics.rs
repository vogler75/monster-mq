use crate::stores::traits::MetricsStore;
use crate::stores::types::MetricKind;
use chrono::Utc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerSnapshot {
    pub messages_in: f64,
    pub messages_out: f64,
    pub mqtt_client_in: f64,
    pub mqtt_client_out: f64,
    pub node_session_count: i64,
    pub subscription_count: i64,
    pub queued_messages_count: i64,
}

/// Collector — atomic counters incremented from hot paths, persisted on a
/// 1 s tick to the MetricsStore. Mirrors `internal/metrics/collector.go`.
pub struct Collector {
    store: Arc<dyn MetricsStore>,
    pub node_id: String,
    interval: Duration,

    in_n: AtomicI64,
    out_n: AtomicI64,
    bridge_in: AtomicI64,
    bridge_out: AtomicI64,

    latest: RwLock<BrokerSnapshot>,
}

impl Collector {
    pub fn new(store: Arc<dyn MetricsStore>, node_id: String, interval: Duration) -> Arc<Self> {
        let interval = if interval.is_zero() { Duration::from_secs(1) } else { interval };
        Arc::new(Self {
            store, node_id, interval,
            in_n: AtomicI64::new(0),
            out_n: AtomicI64::new(0),
            bridge_in: AtomicI64::new(0),
            bridge_out: AtomicI64::new(0),
            latest: RwLock::new(BrokerSnapshot::default()),
        })
    }

    pub fn inc_in(&self) { self.in_n.fetch_add(1, Ordering::Relaxed); }
    pub fn inc_out(&self) { self.out_n.fetch_add(1, Ordering::Relaxed); }
    pub fn inc_bridge_in(&self, n: i64) { self.bridge_in.fetch_add(n, Ordering::Relaxed); }
    pub fn inc_bridge_out(&self, n: i64) { self.bridge_out.fetch_add(n, Ordering::Relaxed); }

    pub fn latest(&self) -> BrokerSnapshot { self.latest.read().clone() }

    /// Counts is called every tick to observe session/subscription/queue size.
    pub fn start<F, Fut>(self: &Arc<Self>, mut counts: F)
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = (i64, i64, i64)> + Send,
    {
        let me = self.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(me.interval);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                t.tick().await;
                let in_n = me.in_n.swap(0, Ordering::Relaxed);
                let out_n = me.out_n.swap(0, Ordering::Relaxed);
                let bi = me.bridge_in.swap(0, Ordering::Relaxed);
                let bo = me.bridge_out.swap(0, Ordering::Relaxed);
                let (sessions, subs, queued) = counts().await;
                let secs = me.interval.as_secs_f64().max(0.001);
                let snap = BrokerSnapshot {
                    messages_in: in_n as f64 / secs,
                    messages_out: out_n as f64 / secs,
                    mqtt_client_in: bi as f64 / secs,
                    mqtt_client_out: bo as f64 / secs,
                    node_session_count: sessions,
                    subscription_count: subs,
                    queued_messages_count: queued,
                };
                *me.latest.write() = snap.clone();
                let payload = serde_json::to_string(&snap).unwrap_or_else(|_| "{}".to_string());
                if let Err(e) = me.store.store_metrics(MetricKind::Broker, Utc::now(), &me.node_id, &payload).await {
                    tracing::warn!(err = ?e, "metrics persist failed");
                }
            }
        });
    }
}
