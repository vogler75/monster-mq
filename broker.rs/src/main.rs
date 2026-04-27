use clap::Parser;
use monstermq_edge::archive::Manager as ArchiveManager;
use monstermq_edge::auth::Cache as AuthCache;
use monstermq_edge::bridge::mqttclient::Manager as BridgeManager;
use monstermq_edge::broker::Server as BrokerServer;
use monstermq_edge::config::{Config, StoreType};
use monstermq_edge::graphql::{build_schema, serve_graphql, Ctx};
use monstermq_edge::log_bus;
use monstermq_edge::metrics::Collector as MetricsCollector;
use monstermq_edge::pubsub::Bus as PubsubBus;
use monstermq_edge::stores::{sqlite, mongodb as store_mongodb, postgres as store_postgres};
use monstermq_edge::version;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "monstermq-edge",
    version,
    disable_version_flag = true,
    about = "MonsterMQ Edge — single-binary MQTT broker (broker.rs)"
)]
struct Cli {
    /// Path to config.yaml (defaults to built-in defaults if omitted).
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override log level (DEBUG | INFO | WARN | ERROR).
    #[arg(long = "log-level")]
    log_level: Option<String>,
    /// Print version and exit.
    #[arg(long, short = 'V')]
    version: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.version {
        println!("{}", version::full());
        return Ok(());
    }

    let mut cfg = Config::load(cli.config.as_deref())?;
    if let Some(l) = cli.log_level { cfg.logging.level = l; }

    // Logging — bus + tracing layer.
    let log_bus = log_bus::Bus::new(cfg.logging.ring_buffer_size, cfg.node_id.clone());
    log_bus::setup(&cfg.logging.level, log_bus.clone());

    tracing::info!(name = version::NAME, version = version::VERSION, node = cfg.node_id, "starting");

    // Storage.
    let (storage, sqlite_db_opt) = match cfg.default_store_type {
        StoreType::Sqlite | StoreType::None => {
            let (s, db) = sqlite::build(&cfg).await?;
            (s, Some(db))
        }
        StoreType::Postgres => (store_postgres::build(&cfg).await?, None),
        StoreType::Mongodb => (store_mongodb::build(&cfg).await?, None),
        StoreType::Memory => {
            anyhow::bail!("MEMORY default store unsupported (no persistence). Use SQLITE.");
        }
    };

    // Auth cache.
    let anon_allow = cfg.user_management.anonymous_enabled || !cfg.user_management.enabled;
    let auth = AuthCache::new(storage.users.clone(), anon_allow);
    if let Err(e) = auth.refresh().await {
        tracing::warn!(err = ?e, "user cache refresh failed");
    }
    auth.clone().start_refresher(Duration::from_secs(30));

    // Pub/sub bus + archive manager.
    let bus = PubsubBus::new();
    let archives = ArchiveManager::new(cfg.clone(), storage.clone(), sqlite_db_opt);
    if let Err(e) = archives.load().await {
        tracing::warn!(err = ?e, "archive groups load failed");
    }

    // Metrics collector.
    let collector = if cfg.metrics.enabled {
        Some(MetricsCollector::new(
            storage.metrics.clone(),
            cfg.node_id.clone(),
            Duration::from_secs(cfg.metrics.collection_interval_seconds.max(1) as u64),
        ))
    } else { None };

    // Broker server (NB: PublishFn is wired below via Arc::clone).
    let server = BrokerServer::new(
        cfg.clone(), storage.clone(), bus.clone(), archives.clone(),
        auth.clone(), collector.clone(), None,
    );
    let server_clone = server.clone();
    let publish_fn: monstermq_edge::broker::server::PublishFn =
        Arc::new(move |topic: &str, payload: Vec<u8>, retain: bool, qos: u8| {
            if let Err(e) = server_clone.publish(topic, payload, retain, qos) {
                tracing::warn!(err = ?e, "publish failed");
            }
        });

    // MQTT bridge manager — needs the publish_fn.
    let bridges = if cfg.features.mqtt_client {
        Some(BridgeManager::new(
            storage.device_config.clone(), publish_fn.clone(), bus.clone(), cfg.node_id.clone(),
        ))
    } else { None };

    // Build a final Server view that has the real publish_fn + bridges.
    // Since `server` is already an Arc, we replace the field via a parallel
    // wrapper for GraphQL Ctx (simpler than mutating the Arc).
    let gql_ctx = Ctx {
        cfg: cfg.clone(),
        storage: storage.clone(),
        bus: bus.clone(),
        log_bus: log_bus.clone(),
        archives: archives.clone(),
        auth: auth.clone(),
        bridges: bridges.clone(),
        collector: collector.clone(),
        publish_fn: publish_fn.clone(),
    };

    // Bring up the embedded broker (storage hooks, retained restore, listeners).
    server.clone().serve().await?;

    // Bridges start after the broker is listening so the publish_fn is live.
    if let Some(b) = bridges.clone() {
        if let Err(e) = b.start().await {
            tracing::warn!(err = ?e, "bridge manager start failed");
        }
    }

    // GraphQL HTTP+WS.
    if cfg.graphql.enabled {
        let schema = build_schema(gql_ctx);
        let dashboard_path = if cfg.dashboard.enabled { Some(cfg.dashboard.path.clone()) } else { None };
        tokio::spawn(async move {
            if let Err(e) = serve_graphql(schema, cfg.graphql.port, dashboard_path).await {
                tracing::error!(err = ?e, "graphql server stopped");
            }
        });
    }

    // Ctrl-C handler.
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    server.close().await;
    Ok(())
}
