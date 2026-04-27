use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Canonical in-broker representation of a published MQTT message.
/// Field shapes mirror MonsterMQ's `BrokerMessage` (Kotlin) so storage rows are
/// byte-compatible across the JVM, Go, and Rust implementations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BrokerMessage {
    #[serde(rename = "messageUuid", default)]
    pub message_uuid: String,
    #[serde(rename = "messageId", default)]
    pub message_id: u16,
    #[serde(rename = "topicName")]
    pub topic_name: String,
    #[serde(default, with = "serde_bytes")]
    pub payload: Vec<u8>,
    #[serde(default)]
    pub qos: u8,
    #[serde(rename = "isRetain", default)]
    pub is_retain: bool,
    #[serde(rename = "isDup", default)]
    pub is_dup: bool,
    #[serde(rename = "isQueued", default)]
    pub is_queued: bool,
    #[serde(rename = "clientId", default)]
    pub client_id: String,
    #[serde(rename = "time")]
    pub time: DateTime<Utc>,

    #[serde(rename = "payloadFormatIndicator", skip_serializing_if = "Option::is_none", default)]
    pub payload_format_indicator: Option<u8>,
    #[serde(rename = "messageExpiryInterval", skip_serializing_if = "Option::is_none", default)]
    pub message_expiry_interval: Option<u32>,
    #[serde(rename = "contentType", skip_serializing_if = "String::is_empty", default)]
    pub content_type: String,
    #[serde(rename = "responseTopic", skip_serializing_if = "String::is_empty", default)]
    pub response_topic: String,
    #[serde(rename = "correlationData", skip_serializing_if = "Vec::is_empty", default, with = "serde_bytes")]
    pub correlation_data: Vec<u8>,
    #[serde(rename = "userProperties", skip_serializing_if = "BTreeMap::is_empty", default)]
    pub user_properties: BTreeMap<String, String>,
}

mod serde_bytes {
    use serde::{Deserializer, Serializer};
    pub fn serialize<S: Serializer>(b: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(b)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        // Accept either a base64 string or a byte sequence
        let v = serde::de::Deserialize::deserialize(d)?;
        Ok(v)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ArchivedMessage {
    pub topic: String,
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub payload: Vec<u8>,
    pub qos: u8,
    #[serde(rename = "clientId", default)]
    pub client_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct SessionInfo {
    pub client_id: String,
    pub node_id: String,
    pub clean_session: bool,
    pub connected: bool,
    pub update_time: DateTime<Utc>,
    pub information: String,
    pub client_address: String,
    pub protocol_version: i32,
    pub session_expiry_interval: i64,
    pub receive_maximum: i32,
    pub maximum_packet_size: i64,
    pub topic_alias_maximum: i32,
    pub last_will_topic: String,
    pub last_will_payload: Vec<u8>,
    pub last_will_qos: u8,
    pub last_will_retain: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqttSubscription {
    #[serde(rename = "clientId", default)]
    pub client_id: String,
    #[serde(rename = "topicFilter")]
    pub topic_filter: String,
    pub qos: u8,
    #[serde(rename = "noLocal", default)]
    pub no_local: bool,
    #[serde(rename = "retainAsPublished", default)]
    pub retain_as_published: bool,
    #[serde(rename = "retainHandling", default)]
    pub retain_handling: u8,
    #[serde(rename = "subscriptionId", default)]
    pub subscription_id: i32,
}

#[derive(Debug, Clone, Default)]
pub struct PurgeResult {
    pub deleted_rows: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MessageStoreType {
    None,
    Memory,
    Hazelcast,
    Postgres,
    Cratedb,
    Mongodb,
    Sqlite,
}

impl Default for MessageStoreType {
    fn default() -> Self { MessageStoreType::None }
}

impl MessageStoreType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageStoreType::None => "NONE",
            MessageStoreType::Memory => "MEMORY",
            MessageStoreType::Hazelcast => "HAZELCAST",
            MessageStoreType::Postgres => "POSTGRES",
            MessageStoreType::Cratedb => "CRATEDB",
            MessageStoreType::Mongodb => "MONGODB",
            MessageStoreType::Sqlite => "SQLITE",
        }
    }
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "MEMORY" => Self::Memory,
            "HAZELCAST" => Self::Hazelcast,
            "POSTGRES" => Self::Postgres,
            "CRATEDB" => Self::Cratedb,
            "MONGODB" => Self::Mongodb,
            "SQLITE" => Self::Sqlite,
            _ => Self::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MessageArchiveType {
    None,
    Postgres,
    Cratedb,
    Mongodb,
    Kafka,
    Sqlite,
}

impl Default for MessageArchiveType {
    fn default() -> Self { MessageArchiveType::None }
}

impl MessageArchiveType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageArchiveType::None => "NONE",
            MessageArchiveType::Postgres => "POSTGRES",
            MessageArchiveType::Cratedb => "CRATEDB",
            MessageArchiveType::Mongodb => "MONGODB",
            MessageArchiveType::Kafka => "KAFKA",
            MessageArchiveType::Sqlite => "SQLITE",
        }
    }
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "POSTGRES" => Self::Postgres,
            "CRATEDB" => Self::Cratedb,
            "MONGODB" => Self::Mongodb,
            "KAFKA" => Self::Kafka,
            "SQLITE" => Self::Sqlite,
            _ => Self::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PayloadFormat {
    Default,
    Json,
}

impl Default for PayloadFormat {
    fn default() -> Self { PayloadFormat::Default }
}

impl PayloadFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            PayloadFormat::Default => "DEFAULT",
            PayloadFormat::Json => "JSON",
        }
    }
    pub fn parse(s: &str) -> Self {
        if s.eq_ignore_ascii_case("JSON") { Self::Json } else { Self::Default }
    }
}

#[derive(Debug, Clone, Default)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub enabled: bool,
    pub can_subscribe: bool,
    pub can_publish: bool,
    pub is_admin: bool,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct AclRule {
    pub id: String,
    pub username: String,
    pub topic_pattern: String,
    pub can_subscribe: bool,
    pub can_publish: bool,
    pub priority: i32,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct ArchiveGroupConfig {
    pub name: String,
    pub enabled: bool,
    pub topic_filters: Vec<String>,
    pub retained_only: bool,
    pub last_val_type: MessageStoreType,
    pub archive_type: MessageArchiveType,
    pub last_val_retention: String,
    pub archive_retention: String,
    pub purge_interval: String,
    pub payload_format: PayloadFormat,
}

#[derive(Debug, Clone, Default)]
pub struct DeviceConfig {
    pub name: String,
    pub namespace: String,
    pub node_id: String,
    pub r#type: String,
    pub enabled: bool,
    /// raw JSON config blob
    pub config: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Broker,
    Session,
    MqttClient,
}

impl MetricKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricKind::Broker => "broker",
            MetricKind::Session => "session",
            MetricKind::MqttClient => "mqttclient",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsRow {
    pub timestamp: DateTime<Utc>,
    /// raw JSON blob (matching the Kotlin/Go schemas)
    pub payload: String,
}
