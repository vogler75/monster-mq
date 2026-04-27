use crate::stores::topic::match_topic;
use crate::stores::types::BrokerMessage;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// In-process pub/sub used by GraphQL subscriptions to deliver MQTT-style
/// topic events. Subscribers express MQTT-style topic filters (with `+` and
/// `#` wildcards). Mirrors `internal/pubsub/bus.go`.
pub struct Bus {
    inner: Mutex<Inner>,
}

struct Inner {
    next: u64,
    subs: HashMap<u64, Sub>,
}

struct Sub {
    filters: Vec<String>,
    tx: mpsc::Sender<BrokerMessage>,
}

pub struct Subscription {
    pub id: u64,
    pub rx: mpsc::Receiver<BrokerMessage>,
}

impl Bus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { inner: Mutex::new(Inner { next: 0, subs: HashMap::new() }) })
    }

    pub fn subscribe(self: &Arc<Self>, filters: Vec<String>, buffer: usize) -> Subscription {
        let buffer = if buffer == 0 { 16 } else { buffer };
        let (tx, rx) = mpsc::channel(buffer);
        let mut g = self.inner.lock();
        g.next += 1;
        let id = g.next;
        g.subs.insert(id, Sub { filters, tx });
        Subscription { id, rx }
    }

    pub fn unsubscribe(&self, id: u64) {
        self.inner.lock().subs.remove(&id);
    }

    pub fn publish(&self, msg: BrokerMessage) {
        let g = self.inner.lock();
        for sub in g.subs.values() {
            if any_match(&sub.filters, &msg.topic_name) {
                // try_send drops on slow subscribers (matches Go's nonblocking semantics)
                let _ = sub.tx.try_send(msg.clone());
            }
        }
    }
}

fn any_match(filters: &[String], topic: &str) -> bool {
    filters.iter().any(|f| match_topic(f, topic))
}
