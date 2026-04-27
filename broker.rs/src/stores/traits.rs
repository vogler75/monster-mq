use super::types::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub type StoreResult<T> = anyhow::Result<T>;

/// Last-value MessageStore: retained messages and per-archive-group "current value".
/// Iteration is exposed as Vec<T> here (Go's yield-based callback iterators map
/// awkwardly to async Rust); the stores all read whole tables anyway when
/// matching topic filters.
#[async_trait]
pub trait MessageStore: Send + Sync {
    fn name(&self) -> &str;
    fn store_type(&self) -> MessageStoreType;

    async fn get(&self, topic: &str) -> StoreResult<Option<BrokerMessage>>;
    async fn add_all(&self, msgs: &[BrokerMessage]) -> StoreResult<()>;
    async fn del_all(&self, topics: &[String]) -> StoreResult<()>;

    /// Return all stored messages whose topic matches the MQTT-style pattern.
    async fn find_matching_messages(&self, pattern: &str) -> StoreResult<Vec<BrokerMessage>>;

    /// Return all stored topics matching the pattern.
    async fn find_matching_topics(&self, pattern: &str) -> StoreResult<Vec<String>>;

    async fn purge_older_than(&self, t: DateTime<Utc>) -> StoreResult<PurgeResult>;
    async fn ensure_table(&self) -> StoreResult<()>;
    async fn close(&self) -> StoreResult<()> { Ok(()) }
}

#[async_trait]
pub trait MessageArchive: Send + Sync {
    fn name(&self) -> &str;
    fn archive_type(&self) -> MessageArchiveType;

    async fn add_history(&self, msgs: &[BrokerMessage]) -> StoreResult<()>;
    async fn get_history(
        &self,
        topic_filter: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: i64,
    ) -> StoreResult<Vec<ArchivedMessage>>;
    async fn purge_older_than(&self, t: DateTime<Utc>) -> StoreResult<PurgeResult>;
    async fn ensure_table(&self) -> StoreResult<()>;
    async fn close(&self) -> StoreResult<()> { Ok(()) }
}

#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn set_client(&self, info: SessionInfo) -> StoreResult<()>;
    async fn set_connected(&self, client_id: &str, connected: bool) -> StoreResult<()>;
    async fn set_last_will(
        &self,
        client_id: &str,
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
    ) -> StoreResult<()>;

    async fn is_connected(&self, client_id: &str) -> StoreResult<bool>;
    async fn is_present(&self, client_id: &str) -> StoreResult<bool>;
    async fn get_session(&self, client_id: &str) -> StoreResult<Option<SessionInfo>>;

    async fn iterate_sessions(&self) -> StoreResult<Vec<SessionInfo>>;
    async fn iterate_subscriptions(&self) -> StoreResult<Vec<MqttSubscription>>;
    async fn get_subscriptions_for_client(&self, client_id: &str) -> StoreResult<Vec<MqttSubscription>>;
    async fn add_subscriptions(&self, subs: &[MqttSubscription]) -> StoreResult<()>;
    async fn del_subscriptions(&self, subs: &[MqttSubscription]) -> StoreResult<()>;

    async fn del_client(&self, client_id: &str) -> StoreResult<()>;
    async fn purge_sessions(&self) -> StoreResult<()>;
    async fn ensure_table(&self) -> StoreResult<()>;
}

#[async_trait]
pub trait QueueStore: Send + Sync {
    async fn enqueue(&self, client_id: &str, msg: &BrokerMessage) -> StoreResult<()>;
    async fn enqueue_multi(&self, msg: &BrokerMessage, client_ids: &[String]) -> StoreResult<()>;
    async fn dequeue(&self, client_id: &str, batch_size: i64) -> StoreResult<Vec<BrokerMessage>>;
    async fn ack(&self, client_id: &str, message_uuid: &str) -> StoreResult<()>;
    async fn purge_for_client(&self, client_id: &str) -> StoreResult<i64>;
    async fn count(&self, client_id: &str) -> StoreResult<i64>;
    async fn count_all(&self) -> StoreResult<i64>;
    async fn ensure_table(&self) -> StoreResult<()>;
}

#[async_trait]
pub trait UserStore: Send + Sync {
    async fn create_user(&self, u: &User) -> StoreResult<()>;
    async fn update_user(&self, u: &User) -> StoreResult<()>;
    async fn delete_user(&self, username: &str) -> StoreResult<()>;
    async fn get_user(&self, username: &str) -> StoreResult<Option<User>>;
    async fn get_all_users(&self) -> StoreResult<Vec<User>>;
    async fn validate_credentials(&self, username: &str, password: &str) -> StoreResult<Option<User>>;

    async fn create_acl_rule(&self, r: &AclRule) -> StoreResult<()>;
    async fn update_acl_rule(&self, r: &AclRule) -> StoreResult<()>;
    async fn delete_acl_rule(&self, id: &str) -> StoreResult<()>;
    async fn get_user_acl_rules(&self, username: &str) -> StoreResult<Vec<AclRule>>;
    async fn get_all_acl_rules(&self) -> StoreResult<Vec<AclRule>>;
    async fn load_all(&self) -> StoreResult<(Vec<User>, Vec<AclRule>)>;
    async fn ensure_table(&self) -> StoreResult<()>;
}

#[async_trait]
pub trait ArchiveConfigStore: Send + Sync {
    async fn get_all(&self) -> StoreResult<Vec<ArchiveGroupConfig>>;
    async fn get(&self, name: &str) -> StoreResult<Option<ArchiveGroupConfig>>;
    async fn save(&self, cfg: &ArchiveGroupConfig) -> StoreResult<()>;
    async fn update(&self, cfg: &ArchiveGroupConfig) -> StoreResult<()>;
    async fn delete(&self, name: &str) -> StoreResult<()>;
    async fn ensure_table(&self) -> StoreResult<()>;
}

#[async_trait]
pub trait DeviceConfigStore: Send + Sync {
    async fn get_all(&self) -> StoreResult<Vec<DeviceConfig>>;
    async fn get_by_node(&self, node_id: &str) -> StoreResult<Vec<DeviceConfig>>;
    async fn get_enabled_by_node(&self, node_id: &str) -> StoreResult<Vec<DeviceConfig>>;
    async fn get(&self, name: &str) -> StoreResult<Option<DeviceConfig>>;
    async fn save(&self, d: &DeviceConfig) -> StoreResult<()>;
    async fn delete(&self, name: &str) -> StoreResult<()>;
    async fn toggle(&self, name: &str, enabled: bool) -> StoreResult<Option<DeviceConfig>>;
    async fn reassign(&self, name: &str, node_id: &str) -> StoreResult<Option<DeviceConfig>>;
    async fn ensure_table(&self) -> StoreResult<()>;
}

#[async_trait]
pub trait MetricsStore: Send + Sync {
    async fn store_metrics(
        &self,
        kind: MetricKind,
        ts: DateTime<Utc>,
        identifier: &str,
        json_payload: &str,
    ) -> StoreResult<()>;
    async fn get_latest(
        &self,
        kind: MetricKind,
        identifier: &str,
    ) -> StoreResult<Option<(DateTime<Utc>, String)>>;
    async fn get_history(
        &self,
        kind: MetricKind,
        identifier: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: i64,
    ) -> StoreResult<Vec<MetricsRow>>;
    async fn purge_older_than(&self, t: DateTime<Utc>) -> StoreResult<i64>;
    async fn ensure_table(&self) -> StoreResult<()>;
}
