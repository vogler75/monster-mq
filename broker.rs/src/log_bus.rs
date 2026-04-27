use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

#[derive(Debug, Clone)]
pub struct Entry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub logger: String,
    pub message: String,
    pub thread: i64,
    pub node: String,
    pub source_class: Option<String>,
    pub source_method: Option<String>,
    pub parameters: Vec<String>,
    pub exception: Option<Exception>,
}

#[derive(Debug, Clone)]
pub struct Exception {
    pub class: String,
    pub message: Option<String>,
    pub stack_trace: String,
}

/// Bus is a ring buffer + broadcast channel for log entries.
/// Mirrors `internal/log/bus.go`: `Snapshot()` backs the GraphQL systemLogs
/// query, `subscribe()` backs the systemLogs WebSocket subscription.
pub struct Bus {
    ring: Mutex<VecDeque<Entry>>,
    cap: usize,
    tx: broadcast::Sender<Entry>,
    pub node_id: String,
}

impl Bus {
    pub fn new(cap: usize, node_id: String) -> Arc<Self> {
        let cap = if cap == 0 { 1000 } else { cap };
        let (tx, _) = broadcast::channel(1024);
        Arc::new(Self { ring: Mutex::new(VecDeque::with_capacity(cap)), cap, tx, node_id })
    }

    pub fn publish(&self, e: Entry) {
        {
            let mut r = self.ring.lock();
            if r.len() == self.cap { r.pop_front(); }
            r.push_back(e.clone());
        }
        let _ = self.tx.send(e);
    }

    pub fn snapshot(&self) -> Vec<Entry> {
        self.ring.lock().iter().cloned().collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Entry> {
        self.tx.subscribe()
    }
}

/// tracing Layer that forwards every event to the Bus while delegating to the
/// inner subscriber (so logs still hit stderr).
pub struct BusLayer {
    bus: Arc<Bus>,
}

impl BusLayer {
    pub fn new(bus: Arc<Bus>) -> Self { Self { bus } }
}

struct FieldVisitor<'a> {
    msg: &'a mut String,
    logger: &'a mut Option<String>,
    src_class: &'a mut Option<String>,
    src_method: &'a mut Option<String>,
}

impl<'a> tracing::field::Visit for FieldVisitor<'a> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "message" => *self.msg = value.to_string(),
            "logger"  => *self.logger = Some(value.to_string()),
            "source_class" | "sourceClass" => *self.src_class = Some(value.to_string()),
            "source_method" | "sourceMethod" => *self.src_method = Some(value.to_string()),
            _ => {}
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            *self.msg = format!("{:?}", value).trim_matches('"').to_string();
        }
    }
}

impl<S> Layer<S> for BusLayer where S: Subscriber {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let level = match *event.metadata().level() {
            tracing::Level::ERROR => "SEVERE",
            tracing::Level::WARN  => "WARNING",
            tracing::Level::INFO  => "INFO",
            tracing::Level::DEBUG | tracing::Level::TRACE => "FINE",
        };
        let target = event.metadata().target();
        let mut msg = String::new();
        let mut logger: Option<String> = None;
        let mut src_class: Option<String> = None;
        let mut src_method: Option<String> = None;
        event.record(&mut FieldVisitor {
            msg: &mut msg,
            logger: &mut logger,
            src_class: &mut src_class,
            src_method: &mut src_method,
        });
        let entry = Entry {
            timestamp: Utc::now(),
            level: level.to_string(),
            logger: logger.unwrap_or_else(|| target.to_string()),
            message: msg,
            thread: 0,
            node: self.bus.node_id.clone(),
            source_class: src_class,
            source_method: src_method,
            parameters: vec![],
            exception: None,
        };
        self.bus.publish(entry);
    }
}

/// Configures tracing-subscriber with the Bus layer + a stderr fmt layer.
pub fn setup(level: &str, bus: Arc<Bus>) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let lvl = match level.to_ascii_uppercase().as_str() {
        "DEBUG" => "debug",
        "WARN" | "WARNING" => "warn",
        "ERROR" => "error",
        _ => "info",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(lvl));
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true).compact();
    tracing_subscriber::registry()
        .with(filter)
        .with(BusLayer::new(bus))
        .with(fmt_layer)
        .init();
}
