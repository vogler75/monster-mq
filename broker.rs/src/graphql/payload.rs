use async_graphql::Enum;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde::{Deserialize, Serialize};

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[graphql(rename_items = "SCREAMING_SNAKE_CASE")]
pub enum DataFormat {
    Json,
    Binary,
}

impl Default for DataFormat {
    fn default() -> Self { DataFormat::Json }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[graphql(rename_items = "SCREAMING_SNAKE_CASE")]
pub enum OrderDirection { Asc, Desc }

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[graphql(rename_items = "SCREAMING_SNAKE_CASE")]
pub enum MessageStoreType {
    None, Memory, Hazelcast, Postgres, Cratedb, Mongodb, Sqlite,
}

impl From<crate::stores::types::MessageStoreType> for MessageStoreType {
    fn from(v: crate::stores::types::MessageStoreType) -> Self {
        use crate::stores::types::MessageStoreType as S;
        match v {
            S::None => Self::None, S::Memory => Self::Memory,
            S::Hazelcast => Self::Hazelcast, S::Postgres => Self::Postgres,
            S::Cratedb => Self::Cratedb, S::Mongodb => Self::Mongodb,
            S::Sqlite => Self::Sqlite,
        }
    }
}

impl From<MessageStoreType> for crate::stores::types::MessageStoreType {
    fn from(v: MessageStoreType) -> Self {
        use crate::stores::types::MessageStoreType as S;
        match v {
            MessageStoreType::None => S::None, MessageStoreType::Memory => S::Memory,
            MessageStoreType::Hazelcast => S::Hazelcast, MessageStoreType::Postgres => S::Postgres,
            MessageStoreType::Cratedb => S::Cratedb, MessageStoreType::Mongodb => S::Mongodb,
            MessageStoreType::Sqlite => S::Sqlite,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[graphql(rename_items = "SCREAMING_SNAKE_CASE")]
pub enum MessageArchiveType {
    None, Postgres, Cratedb, Mongodb, Kafka, Sqlite,
}

impl From<crate::stores::types::MessageArchiveType> for MessageArchiveType {
    fn from(v: crate::stores::types::MessageArchiveType) -> Self {
        use crate::stores::types::MessageArchiveType as S;
        match v {
            S::None => Self::None, S::Postgres => Self::Postgres,
            S::Cratedb => Self::Cratedb, S::Mongodb => Self::Mongodb,
            S::Kafka => Self::Kafka, S::Sqlite => Self::Sqlite,
        }
    }
}

impl From<MessageArchiveType> for crate::stores::types::MessageArchiveType {
    fn from(v: MessageArchiveType) -> Self {
        use crate::stores::types::MessageArchiveType as S;
        match v {
            MessageArchiveType::None => S::None, MessageArchiveType::Postgres => S::Postgres,
            MessageArchiveType::Cratedb => S::Cratedb, MessageArchiveType::Mongodb => S::Mongodb,
            MessageArchiveType::Kafka => S::Kafka, MessageArchiveType::Sqlite => S::Sqlite,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[graphql(rename_items = "SCREAMING_SNAKE_CASE")]
pub enum PayloadFormat { Default, Json }

impl From<crate::stores::types::PayloadFormat> for PayloadFormat {
    fn from(v: crate::stores::types::PayloadFormat) -> Self {
        use crate::stores::types::PayloadFormat as P;
        match v { P::Default => Self::Default, P::Json => Self::Json }
    }
}

impl From<PayloadFormat> for crate::stores::types::PayloadFormat {
    fn from(v: PayloadFormat) -> Self {
        use crate::stores::types::PayloadFormat as P;
        match v { PayloadFormat::Default => P::Default, PayloadFormat::Json => P::Json }
    }
}

/// encode_payload mirrors the Go broker's `encodePayload`:
///   - if requested == BINARY: (base64(raw), BINARY)
///   - if raw parses as JSON: (raw_text, JSON)
///   - otherwise: (base64(raw), BINARY)
pub fn encode_payload(raw: &[u8], requested: Option<DataFormat>) -> (String, DataFormat) {
    if matches!(requested, Some(DataFormat::Binary)) {
        return (B64.encode(raw), DataFormat::Binary);
    }
    if !raw.is_empty() && serde_json::from_slice::<serde_json::Value>(raw).is_ok() {
        return (String::from_utf8_lossy(raw).into_owned(), DataFormat::Json);
    }
    (B64.encode(raw), DataFormat::Binary)
}
