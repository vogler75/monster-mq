use crate::stores::topic::match_topic;
use crate::stores::traits::{MessageStore, StoreResult};
use crate::stores::types::{BrokerMessage, MessageStoreType, PurgeResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;

pub struct MemoryMessageStore {
    name: String,
    inner: RwLock<HashMap<String, BrokerMessage>>,
}

impl MemoryMessageStore {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), inner: RwLock::new(HashMap::new()) }
    }
}

#[async_trait]
impl MessageStore for MemoryMessageStore {
    fn name(&self) -> &str { &self.name }
    fn store_type(&self) -> MessageStoreType { MessageStoreType::Memory }

    async fn ensure_table(&self) -> StoreResult<()> { Ok(()) }

    async fn get(&self, topic: &str) -> StoreResult<Option<BrokerMessage>> {
        Ok(self.inner.read().get(topic).cloned())
    }

    async fn add_all(&self, msgs: &[BrokerMessage]) -> StoreResult<()> {
        let mut w = self.inner.write();
        for m in msgs { w.insert(m.topic_name.clone(), m.clone()); }
        Ok(())
    }

    async fn del_all(&self, topics: &[String]) -> StoreResult<()> {
        let mut w = self.inner.write();
        for t in topics { w.remove(t); }
        Ok(())
    }

    async fn find_matching_messages(&self, pattern: &str) -> StoreResult<Vec<BrokerMessage>> {
        let r = self.inner.read();
        Ok(r.iter().filter_map(|(k, v)| if match_topic(pattern, k) { Some(v.clone()) } else { None }).collect())
    }

    async fn find_matching_topics(&self, pattern: &str) -> StoreResult<Vec<String>> {
        let r = self.inner.read();
        Ok(r.keys().filter(|k| match_topic(pattern, k)).cloned().collect())
    }

    async fn purge_older_than(&self, t: DateTime<Utc>) -> StoreResult<PurgeResult> {
        let mut w = self.inner.write();
        let before = w.len();
        w.retain(|_, v| v.time >= t);
        Ok(PurgeResult { deleted_rows: (before - w.len()) as i64 })
    }
}
