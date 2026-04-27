use super::payload::{encode_payload, DataFormat, MessageArchiveType, MessageStoreType, OrderDirection, PayloadFormat};
use super::schema::Ctx;
use crate::stores::topic::match_topic;
use crate::stores::types::{ArchiveGroupConfig, MetricKind};
use crate::version;
use async_graphql::{Context, Object, Result, SimpleObject, ID};
use chrono::{DateTime, Utc};

// -------- shared scalar wrappers --------

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug)]
#[serde(transparent)]
pub struct Long(pub i64);
async_graphql::scalar!(Long, "Long");
impl From<i64> for Long { fn from(v: i64) -> Self { Long(v) } }

// Use async_graphql::Json for the JSON scalar.

// -------- output types --------

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UserProperty {
    pub key: String,
    pub value: String,
}

#[async_graphql::ComplexObject]
impl UserProperty {}

#[derive(SimpleObject, Default)]
pub struct CurrentUser {
    pub username: String,
    pub is_admin: bool,
}

#[derive(SimpleObject)]
pub struct BrokerConfig {
    pub node_id: String,
    pub version: String,
    pub clustered: bool,
    pub tcp_port: i32,
    pub ws_port: i32,
    pub tcps_port: i32,
    pub wss_port: i32,
    pub nats_port: i32,
    pub session_store_type: String,
    pub retained_store_type: String,
    pub config_store_type: String,
    pub user_management_enabled: bool,
    pub anonymous_enabled: bool,
    pub mcp_enabled: bool,
    pub mcp_port: i32,
    pub prometheus_enabled: bool,
    pub prometheus_port: i32,
    pub i3x_enabled: bool,
    pub i3x_port: i32,
    pub graphql_enabled: bool,
    pub graphql_port: i32,
    pub metrics_enabled: bool,
    pub gen_ai_enabled: bool,
    pub gen_ai_provider: String,
    pub gen_ai_model: String,
    pub postgres_url: String,
    pub postgres_user: String,
    pub crate_db_url: String,
    pub crate_db_user: String,
    pub mongo_db_url: String,
    pub mongo_db_database: String,
    pub sqlite_path: String,
    pub kafka_servers: String,
}

#[derive(SimpleObject, Clone, Debug, Default)]
pub struct BrokerMetrics {
    pub messages_in: f64,
    pub messages_out: f64,
    pub node_session_count: i32,
    pub cluster_session_count: i32,
    pub queued_messages_count: i64,
    pub subscription_count: i32,
    pub client_node_mapping_size: i32,
    pub topic_node_mapping_size: i32,
    pub message_bus_in: f64,
    pub message_bus_out: f64,
    pub mqtt_client_in: f64,
    pub mqtt_client_out: f64,
    pub kafka_client_in: f64,
    pub kafka_client_out: f64,
    pub opc_ua_client_in: f64,
    pub opc_ua_client_out: f64,
    pub win_cc_oa_client_in: f64,
    pub win_cc_ua_client_in: f64,
    pub nats_client_in: f64,
    pub nats_client_out: f64,
    pub redis_client_in: f64,
    pub redis_client_out: f64,
    pub neo4j_client_in: f64,
    pub timestamp: String,
}

#[derive(SimpleObject, Default)]
pub struct SessionMetrics {
    pub messages_in: f64,
    pub messages_out: f64,
    pub connected: Option<bool>,
    pub last_ping: Option<String>,
    pub in_flight_messages_rcv: Option<i32>,
    pub in_flight_messages_snd: Option<i32>,
    pub timestamp: String,
}

#[derive(SimpleObject, Default)]
pub struct MqttClientMetrics {
    pub messages_in: f64,
    pub messages_out: f64,
    pub timestamp: String,
}

#[derive(SimpleObject, Default)]
pub struct ArchiveGroupMetrics {
    pub messages_out: f64,
    pub buffer_size: i32,
    pub timestamp: String,
}

#[derive(SimpleObject)]
pub struct Broker {
    pub node_id: String,
    pub version: String,
    pub user_management_enabled: bool,
    pub anonymous_enabled: bool,
    pub is_leader: bool,
    pub is_current: bool,
    pub enabled_features: Vec<String>,
    pub metrics: Vec<BrokerMetrics>,
    pub sessions: Vec<Session>,
}

// metricsHistory is computed via a custom resolver below
#[derive(SimpleObject)]
pub struct MqttSubscription {
    pub topic_filter: String,
    pub qos: i32,
    pub no_local: Option<bool>,
    pub retain_handling: Option<i32>,
    pub retain_as_published: Option<bool>,
}

#[derive(SimpleObject)]
pub struct Session {
    pub client_id: String,
    pub node_id: String,
    pub metrics: Vec<SessionMetrics>,
    pub subscriptions: Vec<MqttSubscription>,
    pub clean_session: bool,
    pub session_expiry_interval: i64,
    pub client_address: Option<String>,
    pub connected: bool,
    pub queued_message_count: i64,
    pub information: Option<String>,
    pub protocol_version: Option<i32>,
    pub receive_maximum: Option<i32>,
    pub maximum_packet_size: Option<i64>,
    pub topic_alias_maximum: Option<i32>,
}

#[derive(SimpleObject)]
pub struct TopicValue {
    pub topic: String,
    pub payload: String,
    pub format: DataFormat,
    pub timestamp: i64,
    pub qos: i32,
    pub message_expiry_interval: Option<i64>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub payload_format_indicator: Option<bool>,
    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(SimpleObject)]
pub struct RetainedMessage {
    pub topic: String,
    pub payload: String,
    pub format: DataFormat,
    pub timestamp: i64,
    pub qos: i32,
    pub message_expiry_interval: Option<i64>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub payload_format_indicator: Option<bool>,
    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(SimpleObject)]
pub struct ArchivedMessage {
    pub topic: String,
    pub payload: String,
    pub format: DataFormat,
    pub timestamp: i64,
    pub qos: i32,
    pub client_id: Option<String>,
    pub message_expiry_interval: Option<i64>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub payload_format_indicator: Option<bool>,
    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(SimpleObject)]
pub struct Topic {
    pub name: String,
    pub is_leaf: bool,
    pub value: Option<TopicValue>,
}

#[derive(SimpleObject)]
pub struct AggregatedResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<async_graphql::Json<serde_json::Value>>>,
}

#[derive(SimpleObject)]
pub struct NodeConnectionStatus {
    pub node_id: String,
    pub message_archive: Option<bool>,
    pub last_value_store: Option<bool>,
    pub error: Option<String>,
    pub timestamp: i64,
}

#[derive(SimpleObject)]
pub struct ArchiveGroupInfo {
    pub name: String,
    pub enabled: bool,
    pub deployed: bool,
    pub deployment_id: Option<String>,
    pub topic_filter: Vec<String>,
    pub retained_only: bool,
    pub last_val_type: MessageStoreType,
    pub archive_type: MessageArchiveType,
    pub payload_format: PayloadFormat,
    pub last_val_retention: Option<String>,
    pub archive_retention: Option<String>,
    pub purge_interval: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub connection_status: Vec<NodeConnectionStatus>,
    pub metrics: Vec<ArchiveGroupMetrics>,
}

#[derive(SimpleObject)]
pub struct AclRuleInfo {
    pub id: ID,
    pub username: String,
    pub topic_pattern: String,
    pub can_subscribe: bool,
    pub can_publish: bool,
    pub priority: i32,
    pub created_at: Option<String>,
}

#[derive(SimpleObject)]
pub struct UserInfo {
    pub username: String,
    pub enabled: bool,
    pub can_subscribe: bool,
    pub can_publish: bool,
    pub is_admin: bool,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub acl_rules: Vec<AclRuleInfo>,
}

#[derive(SimpleObject)]
pub struct MqttClientAddress {
    pub mode: String,
    pub remote_topic: String,
    pub local_topic: String,
    pub remove_path: bool,
    pub qos: Option<i32>,
    pub no_local: Option<bool>,
    pub retain_handling: Option<i32>,
    pub retain_as_published: Option<bool>,
    pub message_expiry_interval: Option<i64>,
    pub content_type: Option<String>,
    pub response_topic_pattern: Option<String>,
    pub payload_format_indicator: Option<bool>,
    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(SimpleObject)]
pub struct MqttClientConnectionConfig {
    pub broker_url: String,
    pub username: Option<String>,
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive: i32,
    pub reconnect_delay: i64,
    pub connection_timeout: i64,
    pub addresses: Vec<MqttClientAddress>,
    pub buffer_enabled: bool,
    pub buffer_size: i32,
    pub persist_buffer: bool,
    pub delete_oldest_messages: bool,
    pub ssl_verify_certificate: bool,
    pub protocol_version: Option<i32>,
    pub session_expiry_interval: Option<i64>,
    pub receive_maximum: Option<i32>,
    pub maximum_packet_size: Option<i64>,
    pub topic_alias_maximum: Option<i32>,
}

#[derive(SimpleObject)]
pub struct MqttClient {
    pub name: String,
    pub namespace: String,
    pub node_id: String,
    pub config: MqttClientConnectionConfig,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
    pub is_on_current_node: bool,
    pub metrics: Vec<MqttClientMetrics>,
}

#[derive(SimpleObject)]
pub struct SystemLogEntry {
    pub timestamp: String,
    pub level: String,
    pub logger: String,
    pub message: String,
    pub thread: i64,
    pub node: String,
    pub source_class: Option<String>,
    pub source_method: Option<String>,
    pub parameters: Option<Vec<String>>,
    pub exception: Option<ExceptionInfo>,
}

#[derive(SimpleObject)]
pub struct ExceptionInfo {
    pub class: String,
    pub message: Option<String>,
    pub stack_trace: String,
}

// ------------------------------------------------------------
// Query
// ------------------------------------------------------------

pub struct Query;

#[Object]
impl Query {
    async fn current_user(&self, _ctx: &Context<'_>) -> Result<Option<CurrentUser>> {
        Ok(Some(CurrentUser {
            username: "Anonymous".to_string(),
            is_admin: true,
        }))
    }

    async fn broker_config(&self, ctx: &Context<'_>) -> Result<BrokerConfig> {
        let c = ctx.data::<Ctx>()?;
        Ok(BrokerConfig {
            node_id: c.cfg.node_id.clone(),
            version: version::VERSION.to_string(),
            clustered: false,
            tcp_port: c.cfg.tcp.port as i32,
            ws_port: c.cfg.ws.port as i32,
            tcps_port: c.cfg.tcps.port as i32,
            wss_port: c.cfg.wss.port as i32,
            nats_port: 0,
            session_store_type: c.cfg.session_store().as_str().to_string(),
            retained_store_type: c.cfg.retained_store().as_str().to_string(),
            config_store_type: c.cfg.config_store().as_str().to_string(),
            user_management_enabled: c.cfg.user_management.enabled,
            anonymous_enabled: c.cfg.user_management.anonymous_enabled,
            mcp_enabled: false, mcp_port: 0,
            prometheus_enabled: false, prometheus_port: 0,
            i3x_enabled: false, i3x_port: 0,
            graphql_enabled: c.cfg.graphql.enabled,
            graphql_port: c.cfg.graphql.port as i32,
            metrics_enabled: c.cfg.metrics.enabled,
            gen_ai_enabled: false,
            gen_ai_provider: String::new(),
            gen_ai_model: String::new(),
            postgres_url: c.cfg.postgres.url.clone(),
            postgres_user: c.cfg.postgres.user.clone(),
            crate_db_url: String::new(),
            crate_db_user: String::new(),
            mongo_db_url: c.cfg.mongodb.url.clone(),
            mongo_db_database: c.cfg.mongodb.database.clone(),
            sqlite_path: c.cfg.sqlite.path.clone(),
            kafka_servers: String::new(),
        })
    }

    async fn broker(&self, ctx: &Context<'_>, node_id: Option<String>) -> Result<Option<Broker>> {
        let c = ctx.data::<Ctx>()?;
        if let Some(n) = node_id { if n != c.cfg.node_id { return Ok(None); } }
        Ok(Some(make_broker(c).await))
    }

    async fn brokers(&self, ctx: &Context<'_>) -> Result<Vec<Broker>> {
        let c = ctx.data::<Ctx>()?;
        Ok(vec![make_broker(c).await])
    }

    async fn sessions(&self, ctx: &Context<'_>, _node_id: Option<String>, clean_session: Option<bool>, connected: Option<bool>) -> Result<Vec<Session>> {
        let c = ctx.data::<Ctx>()?;
        list_sessions(c, clean_session, connected).await
    }

    async fn session(&self, ctx: &Context<'_>, client_id: String, _node_id: Option<String>) -> Result<Option<Session>> {
        let c = ctx.data::<Ctx>()?;
        let info = c.storage.sessions.get_session(&client_id).await?;
        let Some(info) = info else { return Ok(None); };
        let subs = c.storage.sessions.get_subscriptions_for_client(&client_id).await.unwrap_or_default();
        let queued = c.storage.queue.count(&client_id).await.unwrap_or(0);
        Ok(Some(Session {
            client_id: info.client_id, node_id: info.node_id,
            metrics: vec![],
            subscriptions: subs.into_iter().map(|s| MqttSubscription {
                topic_filter: s.topic_filter, qos: s.qos as i32,
                no_local: Some(s.no_local), retain_handling: Some(s.retain_handling as i32),
                retain_as_published: Some(s.retain_as_published),
            }).collect(),
            clean_session: info.clean_session, session_expiry_interval: info.session_expiry_interval,
            client_address: Some(info.client_address), connected: info.connected,
            queued_message_count: queued,
            information: Some(info.information),
            protocol_version: Some(info.protocol_version), receive_maximum: Some(info.receive_maximum),
            maximum_packet_size: Some(info.maximum_packet_size), topic_alias_maximum: Some(info.topic_alias_maximum),
        }))
    }

    async fn users(&self, ctx: &Context<'_>, username: Option<String>) -> Result<Vec<UserInfo>> {
        let c = ctx.data::<Ctx>()?;
        let users = c.storage.users.get_all_users().await?;
        let mut out = vec![];
        for u in users {
            if let Some(ref filter) = username {
                if &u.username != filter { continue; }
            }
            let rules = c.storage.users.get_user_acl_rules(&u.username).await.unwrap_or_default();
            out.push(UserInfo {
                username: u.username,
                enabled: u.enabled, can_subscribe: u.can_subscribe, can_publish: u.can_publish, is_admin: u.is_admin,
                created_at: u.created_at.map(iso),
                updated_at: u.updated_at.map(iso),
                acl_rules: rules.into_iter().map(|r| AclRuleInfo {
                    id: ID(r.id), username: r.username, topic_pattern: r.topic_pattern,
                    can_subscribe: r.can_subscribe, can_publish: r.can_publish,
                    priority: r.priority,
                    created_at: r.created_at.map(iso),
                }).collect(),
            });
        }
        Ok(out)
    }

    async fn current_value(&self, ctx: &Context<'_>, topic: String, format: Option<DataFormat>, archive_group: Option<String>) -> Result<Option<TopicValue>> {
        let c = ctx.data::<Ctx>()?;
        let group = archive_group.unwrap_or_else(|| "Default".to_string());
        let Some(g) = c.archives.snapshot().into_iter().find(|g| g.name() == group) else { return Ok(None); };
        let Some(lv) = g.last_value() else { return Ok(None); };
        let m = lv.get(&topic).await?;
        Ok(m.map(|m| {
            let (payload, fmt) = encode_payload(&m.payload, format);
            TopicValue {
                topic: m.topic_name, payload, format: fmt, timestamp: m.time.timestamp_millis(),
                qos: m.qos as i32,
                message_expiry_interval: m.message_expiry_interval.map(|v| v as i64),
                content_type: if m.content_type.is_empty() { None } else { Some(m.content_type) },
                response_topic: if m.response_topic.is_empty() { None } else { Some(m.response_topic) },
                payload_format_indicator: m.payload_format_indicator.map(|b| b == 1),
                user_properties: if m.user_properties.is_empty() { None } else {
                    Some(m.user_properties.into_iter().map(|(k, v)| UserProperty { key: k, value: v }).collect())
                },
            }
        }))
    }

    async fn current_values(&self, ctx: &Context<'_>, topic_filter: String, format: Option<DataFormat>, limit: Option<i32>, archive_group: Option<String>) -> Result<Vec<TopicValue>> {
        let c = ctx.data::<Ctx>()?;
        let group = archive_group.unwrap_or_else(|| "Default".to_string());
        let Some(g) = c.archives.snapshot().into_iter().find(|g| g.name() == group) else { return Ok(vec![]); };
        let Some(lv) = g.last_value() else { return Ok(vec![]); };
        let msgs = lv.find_matching_messages(&topic_filter).await?;
        let lim = limit.unwrap_or(0).max(0) as usize;
        let take = if lim == 0 { msgs.len() } else { lim.min(msgs.len()) };
        Ok(msgs.into_iter().take(take).map(|m| {
            let (payload, fmt) = encode_payload(&m.payload, format);
            TopicValue {
                topic: m.topic_name, payload, format: fmt, timestamp: m.time.timestamp_millis(),
                qos: m.qos as i32,
                message_expiry_interval: m.message_expiry_interval.map(|v| v as i64),
                content_type: None, response_topic: None,
                payload_format_indicator: None, user_properties: None,
            }
        }).collect())
    }

    async fn retained_message(&self, ctx: &Context<'_>, topic: String, format: Option<DataFormat>) -> Result<Option<RetainedMessage>> {
        let c = ctx.data::<Ctx>()?;
        let m = c.storage.retained.get(&topic).await?;
        Ok(m.map(|m| {
            let (payload, fmt) = encode_payload(&m.payload, format);
            RetainedMessage {
                topic: m.topic_name, payload, format: fmt, timestamp: m.time.timestamp_millis(),
                qos: m.qos as i32,
                message_expiry_interval: m.message_expiry_interval.map(|v| v as i64),
                content_type: None, response_topic: None,
                payload_format_indicator: None, user_properties: None,
            }
        }))
    }

    async fn retained_messages(&self, ctx: &Context<'_>, topic_filter: String, format: Option<DataFormat>, limit: Option<i32>) -> Result<Vec<RetainedMessage>> {
        let c = ctx.data::<Ctx>()?;
        let msgs = c.storage.retained.find_matching_messages(&topic_filter).await?;
        let lim = limit.unwrap_or(0).max(0) as usize;
        let take = if lim == 0 { msgs.len() } else { lim.min(msgs.len()) };
        Ok(msgs.into_iter().take(take).map(|m| {
            let (payload, fmt) = encode_payload(&m.payload, format);
            RetainedMessage {
                topic: m.topic_name, payload, format: fmt, timestamp: m.time.timestamp_millis(),
                qos: m.qos as i32,
                message_expiry_interval: m.message_expiry_interval.map(|v| v as i64),
                content_type: None, response_topic: None,
                payload_format_indicator: None, user_properties: None,
            }
        }).collect())
    }

    async fn search_topics(&self, ctx: &Context<'_>, pattern: String, limit: Option<i32>, archive_group: Option<String>) -> Result<Vec<String>> {
        let c = ctx.data::<Ctx>()?;
        let group = archive_group.unwrap_or_else(|| "Default".to_string());
        let Some(g) = c.archives.snapshot().into_iter().find(|g| g.name() == group) else { return Ok(vec![]); };
        let Some(lv) = g.last_value() else { return Ok(vec![]); };
        let topics = lv.find_matching_topics("#").await?;
        let lim = limit.unwrap_or(0).max(0) as usize;
        let mut out: Vec<String> = topics.into_iter().filter(|t| t.contains(&pattern)).collect();
        if lim > 0 && out.len() > lim { out.truncate(lim); }
        Ok(out)
    }

    /// browseTopics — see GRAPHQL.md "Critical contract" section.
    async fn browse_topics(&self, ctx: &Context<'_>, topic: String, archive_group: Option<String>) -> Result<Vec<Topic>> {
        let c = ctx.data::<Ctx>()?;
        let group = archive_group.unwrap_or_else(|| "Default".to_string());
        let Some(g) = c.archives.snapshot().into_iter().find(|g| g.name() == group) else { return Ok(vec![]); };
        let Some(lv) = g.last_value() else { return Ok(vec![]); };
        let all = lv.find_matching_topics("#").await?;

        // Exact (no-wildcard) topic case.
        if !topic.contains('+') && !topic.contains('#') {
            let exists = all.iter().any(|t| t == &topic);
            return Ok(if exists { vec![Topic { name: topic, is_leaf: true, value: None }] } else { vec![] });
        }

        // Pattern: figure out the depth. Truncate matching topics at that level
        // and dedupe; mark `is_leaf = true` only when the value sits at exactly
        // the queried depth.
        let depth = topic.split('/').count();
        let mut seen: std::collections::BTreeMap<String, bool> = std::collections::BTreeMap::new();
        for t in all.iter() {
            if !match_topic(&topic, t) {
                // Allow prefix-style matches too (used by depth truncation).
                let prefix_match = t.split('/').take(depth).enumerate().all(|(i, seg)| {
                    let pat = topic.split('/').nth(i).unwrap_or("");
                    pat == "+" || pat == "#" || pat == seg
                });
                if !prefix_match { continue; }
            }
            let segs: Vec<&str> = t.split('/').collect();
            if segs.len() < depth { continue; }
            let prefix = segs[..depth].join("/");
            let leaf = segs.len() == depth;
            let entry = seen.entry(prefix).or_insert(false);
            *entry = *entry || leaf;
        }
        Ok(seen.into_iter().map(|(name, is_leaf)| Topic { name, is_leaf, value: None }).collect())
    }

    async fn archived_messages(
        &self, ctx: &Context<'_>,
        topic_filter: String, start_time: Option<String>, end_time: Option<String>,
        format: Option<DataFormat>, limit: Option<i32>, archive_group: Option<String>, _include_topic: Option<bool>,
    ) -> Result<Vec<ArchivedMessage>> {
        let c = ctx.data::<Ctx>()?;
        let group = archive_group.unwrap_or_else(|| "Default".to_string());
        let Some(g) = c.archives.snapshot().into_iter().find(|g| g.name() == group) else { return Ok(vec![]); };
        let Some(ar) = g.archive() else { return Ok(vec![]); };
        let from = start_time.and_then(|s| DateTime::parse_from_rfc3339(&s).ok()).map(|d| d.with_timezone(&Utc));
        let to   = end_time.and_then(|s| DateTime::parse_from_rfc3339(&s).ok()).map(|d| d.with_timezone(&Utc));
        let limit = limit.unwrap_or(100) as i64;
        let msgs = ar.get_history(&topic_filter, from, to, limit).await?;
        Ok(msgs.into_iter().map(|m| {
            let (payload, fmt) = encode_payload(&m.payload, format);
            ArchivedMessage {
                topic: m.topic, payload, format: fmt, timestamp: m.timestamp.timestamp_millis(),
                qos: m.qos as i32, client_id: Some(m.client_id),
                message_expiry_interval: None, content_type: None, response_topic: None,
                payload_format_indicator: None, user_properties: None,
            }
        }).collect())
    }

    async fn aggregated_messages(
        &self, _ctx: &Context<'_>,
        _topics: Vec<String>, _interval: i32, _start_time: String, _end_time: String,
        _functions: Vec<String>, _fields: Option<Vec<String>>, _archive_group: Option<String>,
    ) -> Result<AggregatedResult> {
        // Stub — see Go broker GRAPHQL.md, requires backend-specific time-bucketing SQL.
        Ok(AggregatedResult { columns: vec!["timestamp".to_string()], rows: vec![] })
    }

    async fn system_logs(
        &self, ctx: &Context<'_>,
        start_time: Option<String>, end_time: Option<String>, last_minutes: Option<i32>,
        node: Option<String>, level: Option<Vec<String>>, logger: Option<String>,
        source_class: Option<String>, source_method: Option<String>, message: Option<String>,
        limit: Option<i32>, order_by_time: Option<OrderDirection>,
    ) -> Result<Vec<SystemLogEntry>> {
        let c = ctx.data::<Ctx>()?;
        let mut entries = c.log_bus.snapshot();
        let from = start_time.and_then(|s| DateTime::parse_from_rfc3339(&s).ok()).map(|d| d.with_timezone(&Utc));
        let to   = end_time.and_then(|s| DateTime::parse_from_rfc3339(&s).ok()).map(|d| d.with_timezone(&Utc));
        let lm   = last_minutes.map(|m| Utc::now() - chrono::Duration::minutes(m as i64));
        entries.retain(|e| {
            if let Some(f) = from { if e.timestamp < f { return false; } }
            if let Some(t) = to   { if e.timestamp > t { return false; } }
            if let Some(t) = lm   { if e.timestamp < t { return false; } }
            if let Some(ref n) = node { if n != "+" && &e.node != n { return false; } }
            if let Some(ref ls) = level {
                if !ls.iter().any(|x| x.eq_ignore_ascii_case(&e.level)) { return false; }
            }
            if let Some(ref s) = logger { if !e.logger.contains(s) { return false; } }
            if let Some(ref s) = source_class { if !e.source_class.as_deref().unwrap_or("").contains(s) { return false; } }
            if let Some(ref s) = source_method { if !e.source_method.as_deref().unwrap_or("").contains(s) { return false; } }
            if let Some(ref s) = message { if !e.message.contains(s) { return false; } }
            true
        });
        if matches!(order_by_time, Some(OrderDirection::Desc)) { entries.reverse(); }
        if let Some(l) = limit { if l > 0 && entries.len() > l as usize { entries.truncate(l as usize); } }
        Ok(entries.into_iter().map(|e| SystemLogEntry {
            timestamp: e.timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            level: e.level, logger: e.logger, message: e.message, thread: e.thread, node: e.node,
            source_class: e.source_class, source_method: e.source_method,
            parameters: if e.parameters.is_empty() { None } else { Some(e.parameters) },
            exception: e.exception.map(|x| ExceptionInfo {
                class: x.class, message: x.message, stack_trace: x.stack_trace,
            }),
        }).collect())
    }

    async fn archive_groups(
        &self, ctx: &Context<'_>,
        enabled: Option<bool>, last_val_type_equals: Option<MessageStoreType>, last_val_type_not_equals: Option<MessageStoreType>,
    ) -> Result<Vec<ArchiveGroupInfo>> {
        let c = ctx.data::<Ctx>()?;
        let configs = c.storage.archive_config.get_all().await?;
        let mut out = vec![];
        for cfg in configs {
            if let Some(b) = enabled { if cfg.enabled != b { continue; } }
            if let Some(eq) = last_val_type_equals {
                let want: crate::stores::types::MessageStoreType = eq.into();
                if cfg.last_val_type != want { continue; }
            }
            if let Some(neq) = last_val_type_not_equals {
                let avoid: crate::stores::types::MessageStoreType = neq.into();
                if cfg.last_val_type == avoid { continue; }
            }
            out.push(make_archive_group_info(c, &cfg));
        }
        Ok(out)
    }

    async fn archive_group(&self, ctx: &Context<'_>, name: String) -> Result<Option<ArchiveGroupInfo>> {
        let c = ctx.data::<Ctx>()?;
        let cfg = c.storage.archive_config.get(&name).await?;
        Ok(cfg.map(|cfg| make_archive_group_info(c, &cfg)))
    }

    async fn mqtt_clients(&self, ctx: &Context<'_>, name: Option<String>, node: Option<String>) -> Result<Vec<MqttClient>> {
        let c = ctx.data::<Ctx>()?;
        let devs = c.storage.device_config.get_all().await?;
        let mut out = vec![];
        for d in devs {
            if !d.r#type.is_empty() && d.r#type != "MQTT_CLIENT" { continue; }
            if let Some(ref n) = name { if &d.name != n { continue; } }
            if let Some(ref n) = node { if &d.node_id != n { continue; } }
            out.push(device_to_mqtt_client(c, d));
        }
        Ok(out)
    }
}

// -------- helpers --------

pub(crate) fn iso(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub(crate) async fn list_sessions(c: &Ctx, clean_session: Option<bool>, connected: Option<bool>) -> Result<Vec<Session>> {
    let infos = c.storage.sessions.iterate_sessions().await?;
    let mut out = Vec::with_capacity(infos.len());
    for info in infos {
        if let Some(b) = clean_session { if info.clean_session != b { continue; } }
        if let Some(b) = connected     { if info.connected != b { continue; } }
        let subs = c.storage.sessions.get_subscriptions_for_client(&info.client_id).await.unwrap_or_default();
        let queued = c.storage.queue.count(&info.client_id).await.unwrap_or(0);
        out.push(Session {
            client_id: info.client_id.clone(),
            node_id: if info.node_id.is_empty() { c.cfg.node_id.clone() } else { info.node_id },
            metrics: vec![],
            subscriptions: subs.into_iter().map(|s| MqttSubscription {
                topic_filter: s.topic_filter, qos: s.qos as i32,
                no_local: Some(s.no_local), retain_handling: Some(s.retain_handling as i32),
                retain_as_published: Some(s.retain_as_published),
            }).collect(),
            clean_session: info.clean_session, session_expiry_interval: info.session_expiry_interval,
            client_address: Some(info.client_address), connected: info.connected,
            queued_message_count: queued,
            information: Some(info.information),
            protocol_version: Some(info.protocol_version), receive_maximum: Some(info.receive_maximum),
            maximum_packet_size: Some(info.maximum_packet_size), topic_alias_maximum: Some(info.topic_alias_maximum),
        });
    }
    Ok(out)
}

pub(crate) async fn make_broker(c: &Ctx) -> Broker {
    let snap = c.collector.as_ref().map(|c| c.latest()).unwrap_or_default();
    let now_iso = iso(Utc::now());
    let metrics = vec![BrokerMetrics {
        messages_in: snap.messages_in,
        messages_out: snap.messages_out,
        node_session_count: snap.node_session_count as i32,
        cluster_session_count: snap.node_session_count as i32,
        queued_messages_count: snap.queued_messages_count,
        subscription_count: snap.subscription_count as i32,
        client_node_mapping_size: 0,
        topic_node_mapping_size: 0,
        message_bus_in: 0.0,
        message_bus_out: 0.0,
        mqtt_client_in: snap.mqtt_client_in,
        mqtt_client_out: snap.mqtt_client_out,
        kafka_client_in: 0.0, kafka_client_out: 0.0,
        opc_ua_client_in: 0.0, opc_ua_client_out: 0.0,
        win_cc_oa_client_in: 0.0, win_cc_ua_client_in: 0.0,
        nats_client_in: 0.0, nats_client_out: 0.0,
        redis_client_in: 0.0, redis_client_out: 0.0,
        neo4j_client_in: 0.0,
        timestamp: now_iso,
    }];
    let sessions = list_sessions(c, None, None).await.unwrap_or_default();
    Broker {
        node_id: c.cfg.node_id.clone(),
        version: version::VERSION.to_string(),
        user_management_enabled: c.cfg.user_management.enabled,
        anonymous_enabled: c.cfg.user_management.anonymous_enabled,
        is_leader: true, is_current: true,
        enabled_features: c.cfg.features.enabled_subset(),
        metrics, sessions,
    }
}

fn make_archive_group_info(c: &Ctx, cfg: &ArchiveGroupConfig) -> ArchiveGroupInfo {
    let deployed = c.archives.snapshot().iter().any(|g| g.name() == cfg.name);
    let lv_running = c.archives.snapshot().iter().find(|g| g.name() == cfg.name).and_then(|g| g.last_value()).is_some();
    let ar_running = c.archives.snapshot().iter().find(|g| g.name() == cfg.name).and_then(|g| g.archive()).is_some();
    let err = c.archives.deploy_error(&cfg.name);
    let timestamp = Utc::now().timestamp_millis();
    ArchiveGroupInfo {
        name: cfg.name.clone(),
        enabled: cfg.enabled,
        deployed,
        deployment_id: if deployed { Some(format!("{}:{}", c.cfg.node_id, cfg.name)) } else { None },
        topic_filter: cfg.topic_filters.clone(),
        retained_only: cfg.retained_only,
        last_val_type: cfg.last_val_type.into(),
        archive_type: cfg.archive_type.into(),
        payload_format: cfg.payload_format.into(),
        last_val_retention: opt_str(&cfg.last_val_retention),
        archive_retention: opt_str(&cfg.archive_retention),
        purge_interval: opt_str(&cfg.purge_interval),
        created_at: None, updated_at: None,
        connection_status: vec![NodeConnectionStatus {
            node_id: c.cfg.node_id.clone(),
            message_archive: match cfg.archive_type {
                crate::stores::types::MessageArchiveType::None => None,
                _ => Some(ar_running),
            },
            last_value_store: match cfg.last_val_type {
                crate::stores::types::MessageStoreType::None => None,
                _ => Some(lv_running),
            },
            error: err,
            timestamp,
        }],
        metrics: vec![ArchiveGroupMetrics { messages_out: 0.0, buffer_size: 0, timestamp: iso(Utc::now()) }],
    }
}

fn opt_str(s: &str) -> Option<String> { if s.is_empty() { None } else { Some(s.to_string()) } }

fn device_to_mqtt_client(c: &Ctx, d: crate::stores::types::DeviceConfig) -> MqttClient {
    let cfg: crate::bridge::mqttclient::BridgeConfig = serde_json::from_str(&d.config).unwrap_or_default();
    MqttClient {
        name: d.name,
        namespace: d.namespace,
        is_on_current_node: d.node_id == c.cfg.node_id || d.node_id == "*",
        node_id: d.node_id,
        config: MqttClientConnectionConfig {
            broker_url: cfg.broker_url,
            username: if cfg.username.is_empty() { None } else { Some(cfg.username) },
            client_id: cfg.client_id,
            clean_session: cfg.clean_session,
            keep_alive: cfg.keep_alive,
            reconnect_delay: cfg.reconnect_delay as i64,
            connection_timeout: cfg.connection_timeout as i64,
            addresses: cfg.addresses.into_iter().map(|a| MqttClientAddress {
                mode: a.mode, remote_topic: a.remote_topic, local_topic: a.local_topic,
                remove_path: a.remove_path,
                qos: Some(a.qos), no_local: None, retain_handling: None, retain_as_published: None,
                message_expiry_interval: None, content_type: None, response_topic_pattern: None,
                payload_format_indicator: None, user_properties: None,
            }).collect(),
            buffer_enabled: false, buffer_size: 5000, persist_buffer: false,
            delete_oldest_messages: true, ssl_verify_certificate: true,
            protocol_version: Some(4),
            session_expiry_interval: None, receive_maximum: None,
            maximum_packet_size: None, topic_alias_maximum: None,
        },
        enabled: d.enabled,
        created_at: d.created_at.map(iso).unwrap_or_default(),
        updated_at: d.updated_at.map(iso).unwrap_or_default(),
        metrics: vec![MqttClientMetrics::default()],
    }
}

#[allow(dead_code)]
async fn _unused_metric_kind(_k: MetricKind) {}
