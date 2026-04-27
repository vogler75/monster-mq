use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum StoreType {
    None,
    Memory,
    Sqlite,
    Postgres,
    Mongodb,
}

impl Default for StoreType {
    fn default() -> Self {
        StoreType::Sqlite
    }
}

impl StoreType {
    pub fn as_str(&self) -> &'static str {
        match self {
            StoreType::None => "NONE",
            StoreType::Memory => "MEMORY",
            StoreType::Sqlite => "SQLITE",
            StoreType::Postgres => "POSTGRES",
            StoreType::Mongodb => "MONGODB",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Listener {
    #[serde(rename = "Enabled", default)]
    pub enabled: bool,
    #[serde(rename = "Port", default)]
    pub port: u16,
    #[serde(rename = "KeyStorePath", default)]
    pub key_store_path: String,
    #[serde(rename = "KeyStorePassword", default)]
    pub key_store_password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SqliteConfig {
    #[serde(rename = "Path")]
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PostgresConfig {
    #[serde(rename = "Url", default)]
    pub url: String,
    #[serde(rename = "User", default)]
    pub user: String,
    #[serde(rename = "Pass", default)]
    pub pass: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MongoDBConfig {
    #[serde(rename = "Url", default)]
    pub url: String,
    #[serde(rename = "Database", default)]
    pub database: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserManagementConfig {
    #[serde(rename = "Enabled", default)]
    pub enabled: bool,
    #[serde(rename = "PasswordAlgorithm", default = "default_password_algo")]
    pub password_algorithm: String,
    #[serde(rename = "AnonymousEnabled", default = "default_true")]
    pub anonymous_enabled: bool,
    #[serde(rename = "AclCacheEnabled", default = "default_true")]
    pub acl_cache_enabled: bool,
}

fn default_password_algo() -> String {
    "BCRYPT".to_string()
}
fn default_true() -> bool {
    true
}

impl Default for UserManagementConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            password_algorithm: "BCRYPT".to_string(),
            anonymous_enabled: true,
            acl_cache_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(rename = "Enabled", default = "default_true")]
    pub enabled: bool,
    #[serde(rename = "CollectionIntervalSeconds", default = "default_metrics_interval")]
    pub collection_interval_seconds: u32,
    #[serde(rename = "RetentionHours", default = "default_metrics_retention")]
    pub retention_hours: u32,
}

fn default_metrics_interval() -> u32 {
    1
}
fn default_metrics_retention() -> u32 {
    168
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collection_interval_seconds: 1,
            retention_hours: 168,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(rename = "Level", default = "default_log_level")]
    pub level: String,
    #[serde(rename = "MqttSyslogEnabled", default)]
    pub mqtt_syslog_enabled: bool,
    #[serde(rename = "RingBufferSize", default = "default_ring_buf_size")]
    pub ring_buffer_size: usize,
}

fn default_log_level() -> String {
    "INFO".to_string()
}
fn default_ring_buf_size() -> usize {
    1000
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "INFO".to_string(),
            mqtt_syslog_enabled: false,
            ring_buffer_size: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLConfig {
    #[serde(rename = "Enabled", default = "default_true")]
    pub enabled: bool,
    #[serde(rename = "Port", default = "default_gql_port")]
    pub port: u16,
}

fn default_gql_port() -> u16 {
    8080
}

impl Default for GraphQLConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8080,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DashboardConfig {
    #[serde(rename = "Enabled", default)]
    pub enabled: bool,
    #[serde(rename = "Path", default)]
    pub path: String,
}

/// Top-level `Features:` block — same shape as the main JVM broker
/// (`broker/src/main/kotlin/Features.kt`). Each known feature is a boolean
/// that defaults to `true` when missing.
///
/// broker.rs (edge profile) only acts on `MqttClient`. The other keys are
/// accepted for config-parity so a config.yaml authored for the main broker
/// loads without errors here. Unknown keys are also accepted via `extra`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeaturesConfig {
    #[serde(rename = "MqttClient", default = "default_true")]
    pub mqtt_client: bool,

    // Accepted for config compatibility with the main broker. broker.rs
    // doesn't implement these — they're effectively no-ops here.
    #[serde(rename = "OpcUa", default = "default_true")]
    pub opc_ua: bool,
    #[serde(rename = "OpcUaServer", default = "default_true")]
    pub opc_ua_server: bool,
    #[serde(rename = "Kafka", default = "default_true")]
    pub kafka: bool,
    #[serde(rename = "Nats", default = "default_true")]
    pub nats: bool,
    #[serde(rename = "Redis", default = "default_true")]
    pub redis: bool,
    #[serde(rename = "Telegram", default = "default_true")]
    pub telegram: bool,
    #[serde(rename = "WinCCOa", default = "default_true")]
    pub wincc_oa: bool,
    #[serde(rename = "WinCCUa", default = "default_true")]
    pub wincc_ua: bool,
    #[serde(rename = "Plc4x", default = "default_true")]
    pub plc4x: bool,
    #[serde(rename = "Neo4j", default = "default_true")]
    pub neo4j: bool,
    #[serde(rename = "JdbcLogger", default = "default_true")]
    pub jdbc_logger: bool,
    #[serde(rename = "InfluxDBLogger", default = "default_true")]
    pub influx_db_logger: bool,
    #[serde(rename = "TimeBaseLogger", default = "default_true")]
    pub time_base_logger: bool,
    #[serde(rename = "SparkplugB", default = "default_true")]
    pub sparkplug_b: bool,
    #[serde(rename = "FlowEngine", default = "default_true")]
    pub flow_engine: bool,
    #[serde(rename = "Agents", default = "default_true")]
    pub agents: bool,
    #[serde(rename = "GenAi", default = "default_true")]
    pub gen_ai: bool,
    #[serde(rename = "Mcp", default = "default_true")]
    pub mcp: bool,
    #[serde(rename = "SchemaPolicy", default = "default_true")]
    pub schema_policy: bool,
    #[serde(rename = "TopicNamespace", default = "default_true")]
    pub topic_namespace: bool,
    #[serde(rename = "DeviceImportExport", default = "default_true")]
    pub device_import_export: bool,
}

impl Default for FeaturesConfig {
    fn default() -> Self {
        // Match the main-broker default: every feature on (the runtime decides
        // what's actually wired).
        Self {
            mqtt_client: true,
            opc_ua: true, opc_ua_server: true, kafka: true, nats: true, redis: true,
            telegram: true, wincc_oa: true, wincc_ua: true, plc4x: true, neo4j: true,
            jdbc_logger: true, influx_db_logger: true, time_base_logger: true,
            sparkplug_b: true, flow_engine: true, agents: true,
            gen_ai: true, mcp: true, schema_policy: true,
            topic_namespace: true, device_import_export: true,
        }
    }
}

impl FeaturesConfig {
    /// Names of features that broker.rs actually implements + reports via
    /// `Broker.enabledFeatures`. The main broker reports the full list; the
    /// edge profile is a strict subset.
    pub fn enabled_subset(&self) -> Vec<String> {
        let mut out = vec!["MqttBroker".to_string()];
        if self.mqtt_client { out.push("MqttClient".to_string()); }
        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(rename = "NodeId", default = "default_node_id")]
    pub node_id: String,

    #[serde(rename = "TCP", default)]
    pub tcp: Listener,
    #[serde(rename = "TCPS", default)]
    pub tcps: Listener,
    #[serde(rename = "WS", default)]
    pub ws: Listener,
    #[serde(rename = "WSS", default)]
    pub wss: Listener,
    #[serde(rename = "MaxMessageSize", default = "default_max_msg_size")]
    pub max_message_size: usize,

    #[serde(rename = "DefaultStoreType", default)]
    pub default_store_type: StoreType,
    #[serde(rename = "SessionStoreType", default)]
    pub session_store_type: Option<StoreType>,
    #[serde(rename = "RetainedStoreType", default)]
    pub retained_store_type: Option<StoreType>,
    #[serde(rename = "ConfigStoreType", default)]
    pub config_store_type: Option<StoreType>,

    #[serde(rename = "SQLite", default = "default_sqlite_cfg")]
    pub sqlite: SqliteConfig,
    #[serde(rename = "Postgres", default)]
    pub postgres: PostgresConfig,
    #[serde(rename = "MongoDB", default)]
    pub mongodb: MongoDBConfig,

    #[serde(rename = "UserManagement", default)]
    pub user_management: UserManagementConfig,
    #[serde(rename = "Metrics", default)]
    pub metrics: MetricsConfig,
    #[serde(rename = "Logging", default)]
    pub logging: LoggingConfig,
    #[serde(rename = "GraphQL", default)]
    pub graphql: GraphQLConfig,
    #[serde(rename = "Dashboard", default)]
    pub dashboard: DashboardConfig,
    #[serde(rename = "Features", default)]
    pub features: FeaturesConfig,

    /// QueuedMessagesEnabled — when true, persist offline-queued PUBLISHes for
    /// clean=false sessions to the QueueStore so they survive restarts.
    #[serde(rename = "QueuedMessagesEnabled", default = "default_true")]
    pub queued_messages_enabled: bool,
}

fn default_node_id() -> String {
    "edge".to_string()
}
fn default_max_msg_size() -> usize {
    1_048_576
}
fn default_sqlite_cfg() -> SqliteConfig {
    SqliteConfig {
        path: "./data/monstermq.db".to_string(),
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: "edge".to_string(),
            tcp: Listener {
                enabled: true,
                port: 1883,
                ..Default::default()
            },
            tcps: Listener {
                enabled: false,
                port: 8883,
                ..Default::default()
            },
            ws: Listener {
                enabled: false,
                port: 1884,
                ..Default::default()
            },
            wss: Listener {
                enabled: false,
                port: 8884,
                ..Default::default()
            },
            max_message_size: 1_048_576,
            default_store_type: StoreType::Sqlite,
            session_store_type: Some(StoreType::Sqlite),
            retained_store_type: Some(StoreType::Sqlite),
            config_store_type: Some(StoreType::Sqlite),
            sqlite: default_sqlite_cfg(),
            postgres: PostgresConfig::default(),
            mongodb: MongoDBConfig::default(),
            user_management: UserManagementConfig::default(),
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
            graphql: GraphQLConfig::default(),
            dashboard: DashboardConfig {
                enabled: true,
                path: String::new(),
            },
            features: FeaturesConfig::default(),
            queued_messages_enabled: true,
        }
    }
}

impl Config {
    pub fn session_store(&self) -> StoreType {
        self.session_store_type.unwrap_or(self.default_store_type)
    }
    pub fn retained_store(&self) -> StoreType {
        self.retained_store_type.unwrap_or(self.default_store_type)
    }
    pub fn config_store(&self) -> StoreType {
        self.config_store_type.unwrap_or(self.default_store_type)
    }

    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let Some(p) = path else {
            return Ok(Config::default());
        };
        let s = std::fs::read_to_string(p)
            .map_err(|e| anyhow::anyhow!("read {}: {}", p.display(), e))?;
        let cfg: Config = serde_yaml::from_str(&s)
            .map_err(|e| anyhow::anyhow!("parse {}: {}", p.display(), e))?;
        Ok(cfg)
    }
}
