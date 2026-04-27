use crate::stores::topic::match_topic;
use crate::stores::traits::{MessageArchive, MessageStore};
use crate::stores::types::{ArchiveGroupConfig, BrokerMessage};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// One archive group: owns a last-value store and a message archive. Incoming
/// messages that match any of the group's topic filters are buffered and
/// flushed on a 250 ms tick (or when 100 messages accumulate).
pub struct Group {
    cfg: ArchiveGroupConfig,
    last_val: Option<Arc<dyn MessageStore>>,
    archive: Option<Arc<dyn MessageArchive>>,
    pending: Arc<Mutex<Vec<BrokerMessage>>>,
    flush_notify: Arc<Notify>,
    stop_notify: Arc<Notify>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Group {
    pub fn new(
        cfg: ArchiveGroupConfig,
        last_val: Option<Arc<dyn MessageStore>>,
        archive: Option<Arc<dyn MessageArchive>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            cfg, last_val, archive,
            pending: Arc::new(Mutex::new(Vec::with_capacity(128))),
            flush_notify: Arc::new(Notify::new()),
            stop_notify: Arc::new(Notify::new()),
            handle: Mutex::new(None),
        })
    }

    pub fn name(&self) -> &str { &self.cfg.name }
    pub fn config(&self) -> &ArchiveGroupConfig { &self.cfg }
    pub fn last_value(&self) -> Option<Arc<dyn MessageStore>> { self.last_val.clone() }
    pub fn archive(&self) -> Option<Arc<dyn MessageArchive>> { self.archive.clone() }

    pub fn matches(&self, topic: &str, retain: bool) -> bool {
        if !self.cfg.enabled { return false; }
        if self.cfg.retained_only && !retain { return false; }
        self.cfg.topic_filters.iter().any(|f| match_topic(f.trim(), topic))
    }

    pub fn submit(&self, msg: BrokerMessage) {
        let len = {
            let mut p = self.pending.lock();
            p.push(msg);
            p.len()
        };
        if len >= 100 { self.flush_notify.notify_one(); }
    }

    pub fn start(self: &Arc<Self>) {
        let me = self.clone();
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = me.stop_notify.notified() => {
                        me.flush().await;
                        return;
                    }
                    _ = tick.tick() => me.flush().await,
                    _ = me.flush_notify.notified() => me.flush().await,
                }
            }
        });
        *self.handle.lock() = Some(handle);
    }

    pub async fn stop(&self) {
        self.stop_notify.notify_one();
        let h = self.handle.lock().take();
        if let Some(h) = h {
            let _ = h.await;
        }
    }

    async fn flush(&self) {
        let batch: Vec<BrokerMessage> = {
            let mut p = self.pending.lock();
            if p.is_empty() { return; }
            std::mem::take(&mut *p)
        };
        if let Some(lv) = &self.last_val {
            if let Err(e) = lv.add_all(&batch).await {
                tracing::warn!(group = self.cfg.name, n = batch.len(), err = ?e, "archive lastval write failed");
            }
        }
        if let Some(ar) = &self.archive {
            if let Err(e) = ar.add_history(&batch).await {
                tracing::warn!(group = self.cfg.name, n = batch.len(), err = ?e, "archive history write failed");
            }
        }
    }
}
