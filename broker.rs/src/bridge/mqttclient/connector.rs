use crate::pubsub::Bus as PubsubBus;
use crate::stores::topic::match_topic;
use parking_lot::Mutex;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS, Transport};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// One inbound or outbound mapping rule. Mirrors the dashboard's
/// MqttClientAddressInput.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Address {
    #[serde(default)]
    pub mode: String, // SUBSCRIBE | PUBLISH
    #[serde(default, rename = "remoteTopic")]
    pub remote_topic: String,
    #[serde(default, rename = "localTopic")]
    pub local_topic: String,
    #[serde(default)]
    pub qos: i32,
    #[serde(default)]
    pub retain: bool,
    /// removePath: when true, strip the literal prefix of the source-side
    /// pattern (everything before the first `+` or `#`) from the matched
    /// topic before mapping to the destination side.
    #[serde(default, rename = "removePath")]
    pub remove_path: bool,
}

/// Persisted JSON config (DeviceConfig.config) for one bridge.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default, rename = "brokerUrl")]
    pub broker_url: String,
    #[serde(default, rename = "clientId")]
    pub client_id: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default, rename = "cleanSession")]
    pub clean_session: bool,
    #[serde(default, rename = "keepAlive")]
    pub keep_alive: i32,
    #[serde(default, rename = "connectionTimeout")]
    pub connection_timeout: i32,
    #[serde(default, rename = "reconnectDelay")]
    pub reconnect_delay: i32,
    #[serde(default)]
    pub addresses: Vec<Address>,
}

/// Local publish callback — invoked when a remote message comes in.
pub type LocalPublishFn = Arc<dyn Fn(&str, Vec<u8>, bool, u8) + Send + Sync>;

/// Local message delivered by the in-process pubsub bus to the bridge.
#[derive(Debug, Clone)]
pub struct LocalMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

/// Connector — one bridge to one remote broker.
pub struct Connector {
    pub name: String,
    cfg: Config,
    publisher: LocalPublishFn,
    pubsub: Arc<PubsubBus>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    client: Mutex<Option<AsyncClient>>,
}

impl Connector {
    pub fn new(name: String, cfg: Config, publisher: LocalPublishFn, pubsub: Arc<PubsubBus>) -> Arc<Self> {
        Arc::new(Self {
            name, cfg, publisher, pubsub,
            handles: Mutex::new(vec![]),
            client: Mutex::new(None),
        })
    }

    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        let url = self.cfg.broker_url.clone();
        let (host, port, transport) = parse_broker_url(&url)?;

        let client_id = if self.cfg.client_id.is_empty() {
            format!("monstermq-{}", &self.name)
        } else { self.cfg.client_id.clone() };

        let mut opts = MqttOptions::new(client_id, host, port);
        opts.set_keep_alive(Duration::from_secs(self.cfg.keep_alive.max(30) as u64));
        opts.set_clean_session(self.cfg.clean_session);
        opts.set_transport(transport);
        if !self.cfg.username.is_empty() {
            opts.set_credentials(&self.cfg.username, &self.cfg.password);
        }

        let (client, mut eventloop): (AsyncClient, EventLoop) = AsyncClient::new(opts, 256);
        *self.client.lock() = Some(client.clone());

        // Inbound: subscribe to remote topics for SUBSCRIBE addresses.
        for a in self.cfg.addresses.iter().filter(|a| a.mode.eq_ignore_ascii_case("SUBSCRIBE")) {
            let qos = match a.qos { 0 => QoS::AtMostOnce, 1 => QoS::AtLeastOnce, _ => QoS::ExactlyOnce };
            let _ = client.subscribe(&a.remote_topic, qos).await;
        }

        let me_in = self.clone();
        let h = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        if let Some(addr) = me_in.find_subscribe_addr(&p.topic) {
                            let local_topic = map_inbound_topic(&addr, &p.topic);
                            (me_in.publisher)(&local_topic, p.payload.to_vec(), addr.retain, p.qos as u8);
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(name = me_in.name, err = ?e, "bridge poll error");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });
        self.handles.lock().push(h);

        // Outbound: subscribe to the local pubsub bus for PUBLISH addresses.
        let pub_filters: Vec<String> = self.cfg.addresses.iter()
            .filter(|a| a.mode.eq_ignore_ascii_case("PUBLISH"))
            .map(|a| a.local_topic.clone())
            .collect();
        if !pub_filters.is_empty() {
            let mut sub = self.pubsub.subscribe(pub_filters, 256);
            let me_out = self.clone();
            let client_out = client.clone();
            let h = tokio::spawn(async move {
                while let Some(msg) = sub.rx.recv().await {
                    if let Some(addr) = me_out.find_publish_addr(&msg.topic_name) {
                        let remote = map_outbound_topic(&addr, &msg.topic_name);
                        let qos = match addr.qos { 0 => QoS::AtMostOnce, 1 => QoS::AtLeastOnce, _ => QoS::ExactlyOnce };
                        let _ = client_out.publish(&remote, qos, addr.retain, msg.payload.clone()).await;
                    }
                }
            });
            self.handles.lock().push(h);
        }
        Ok(())
    }

    pub async fn stop(&self) {
        // Drop the guard before awaiting (parking_lot guards are !Send).
        let client = self.client.lock().take();
        if let Some(c) = client {
            let _ = c.disconnect().await;
        }
        let handles: Vec<_> = self.handles.lock().drain(..).collect();
        for h in handles { h.abort(); }
    }

    fn find_subscribe_addr(&self, remote_topic: &str) -> Option<Address> {
        self.cfg.addresses.iter()
            .filter(|a| a.mode.eq_ignore_ascii_case("SUBSCRIBE"))
            .find(|a| match_topic(&a.remote_topic, remote_topic))
            .cloned()
    }

    fn find_publish_addr(&self, local_topic: &str) -> Option<Address> {
        self.cfg.addresses.iter()
            .filter(|a| a.mode.eq_ignore_ascii_case("PUBLISH"))
            .find(|a| match_topic(&a.local_topic, local_topic))
            .cloned()
    }
}

fn parse_broker_url(url: &str) -> anyhow::Result<(String, u16, Transport)> {
    let (scheme, rest) = url.split_once("://").ok_or_else(|| anyhow::anyhow!("invalid brokerUrl: {}", url))?;
    let (host, port_s) = match rest.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().unwrap_or(0)),
        None => (rest.to_string(), 0),
    };
    let (default_port, transport) = match scheme {
        "tcp" | "mqtt" => (1883u16, Transport::tcp()),
        "ssl" | "tls" | "mqtts" => (8883, Transport::tls_with_default_config()),
        // TODO(broker.rs): WS / WSS bridge transports — requires the `websocket`
        // feature on rumqttc and a different Transport constructor.
        "ws" | "wss" => anyhow::bail!("ws/wss bridge transport not yet implemented in broker.rs"),
        _ => anyhow::bail!("unsupported brokerUrl scheme: {}", scheme),
    };
    let port = if port_s == 0 { default_port } else { port_s };
    Ok((host, port, transport))
}

/// Maps an incoming topic from the remote broker to the local topic to publish
/// under, respecting the address's removePath flag and the LocalTopic prefix
/// (if it has no wildcards).
pub fn map_inbound_topic(a: &Address, remote_topic: &str) -> String {
    let mut t = remote_topic.to_string();
    if a.remove_path {
        let prefix = literal_prefix(&a.remote_topic);
        if !prefix.is_empty() {
            t = t.strip_prefix(&prefix).unwrap_or(&t).to_string();
            t = t.strip_prefix('/').unwrap_or(&t).to_string();
        }
    }
    if a.local_topic.is_empty() || a.local_topic.contains('+') || a.local_topic.contains('#') {
        return t;
    }
    if t.is_empty() {
        return a.local_topic.trim_end_matches('/').to_string();
    }
    format!("{}/{}", a.local_topic.trim_end_matches('/'), t)
}

pub fn map_outbound_topic(a: &Address, local_topic: &str) -> String {
    let mut t = local_topic.to_string();
    if a.remove_path {
        let prefix = literal_prefix(&a.local_topic);
        if !prefix.is_empty() {
            t = t.strip_prefix(&prefix).unwrap_or(&t).to_string();
            t = t.strip_prefix('/').unwrap_or(&t).to_string();
        }
    }
    if a.remote_topic.is_empty() || a.remote_topic.contains('+') || a.remote_topic.contains('#') {
        return t;
    }
    if t.is_empty() {
        return a.remote_topic.trim_end_matches('/').to_string();
    }
    format!("{}/{}", a.remote_topic.trim_end_matches('/'), t)
}

/// Longest literal prefix of an MQTT topic pattern — i.e. everything up to but
/// not including the first wildcard segment.
///
/// `"sensor/#"        → "sensor"`
/// `"a/b/+/c"         → "a/b"`
/// `"+/x"             → ""`
/// `"plain/topic"     → "plain/topic"`
pub fn literal_prefix(pattern: &str) -> String {
    let parts: Vec<&str> = pattern.split('/').collect();
    for (i, p) in parts.iter().enumerate() {
        if *p == "+" || *p == "#" {
            return parts[..i].join("/");
        }
    }
    pattern.to_string()
}
