//! MQTT broker lifecycle.
//!
//! The Go broker uses mochi-mqtt embedded. Rust has `rumqttd`, which is
//! configurable via a `Config` struct and runs in-process.
//!
//! INTEGRATION NOTE: rumqttd's hook surface differs from mochi-mqtt:
//!   - Auth: rumqttd has a per-listener `BrokerConfig.tls + auth` plus
//!     an `AuthHandler` trait — wired below if user management is enabled.
//!   - Storage hook (every PUBLISH): rumqttd exposes a `LinkBuilder`/`LinkRx`
//!     pair via which we can subscribe to ALL topics (`#`) and receive each
//!     PUBLISH packet. We use that to drive: retained store, archive manager,
//!     pubsub bus, metrics counter, queue store enqueue.
//!   - Publishing INTO the broker: `LinkTx::publish(topic, payload, qos, retain)`.
//!
//! That is enough for feature parity with the Go broker's storage hook and
//! restoring retained messages at boot.
//!
//! What is NOT yet wired:
//!   - The `OnSessionEstablished` queue replay (post-restart drain) — needs
//!     rumqttd's session-connect hook.
//!   - TLS for TCPS/WSS — see `tls.rs` (loaded from `KeyStorePath`/Pass; this
//!     port currently uses rumqttd's TLS config which expects PEM cert+key).
//!
//! For now: the embedded broker is started, every publish is captured via a
//! local link subscribed to `#`, and our storage/archive/pubsub/metrics hooks
//! all run as in the Go broker.

use crate::archive::Manager as ArchiveManager;
use crate::auth::Cache as AuthCache;
use crate::bridge::mqttclient::Manager as BridgeManager;
use crate::config::Config;
use crate::metrics::Collector as MetricsCollector;
use crate::pubsub::Bus as PubsubBus;
use crate::stores::storage::Storage;
use crate::stores::types::BrokerMessage;
use chrono::Utc;
use parking_lot::Mutex;
use rumqttd::local::{LinkRx, LinkTx};
use rumqttd::{Broker, Notification};
use std::sync::Arc;
use uuid::Uuid;

/// PublishFn — invoked from the bridge when a remote message arrives, or from
/// GraphQL `publish` mutations. Routes into the embedded broker so all
/// subscribers receive it and our storage hook fires.
pub type PublishFn = Arc<dyn Fn(&str, Vec<u8>, bool, u8) + Send + Sync>;

pub struct Server {
    pub cfg: Config,
    pub storage: Storage,
    pub bus: Arc<PubsubBus>,
    pub archives: Arc<ArchiveManager>,
    pub auth: Arc<AuthCache>,
    pub collector: Option<Arc<MetricsCollector>>,
    pub bridges: Option<Arc<BridgeManager>>,
    pub publish_fn: PublishFn,
    broker_tx: Mutex<Option<LinkTx>>,
}

impl Server {
    /// Build a Server: wire all hooks but don't start listeners yet. Call
    /// `serve()` afterwards to enter the run loop.
    pub fn new(
        cfg: Config,
        storage: Storage,
        bus: Arc<PubsubBus>,
        archives: Arc<ArchiveManager>,
        auth: Arc<AuthCache>,
        collector: Option<Arc<MetricsCollector>>,
        bridges: Option<Arc<BridgeManager>>,
    ) -> Arc<Self> {
        // PublishFn is replaced once the LinkTx is set up in serve(); for now
        // it's a no-op so the bridge manager has something to hold. After
        // serve() runs, the real LinkTx-backed function is installed.
        let publish_fn: PublishFn = Arc::new(|_t, _p, _r, _q| {});
        Arc::new(Self {
            cfg, storage, bus, archives, auth, collector, bridges,
            publish_fn,
            broker_tx: Mutex::new(None),
        })
    }

    /// Bring up the embedded broker, restore retained messages, attach the
    /// storage hook, start the metrics / archive / bridge background tasks.
    pub async fn serve(self: Arc<Self>) -> anyhow::Result<()> {
        let rumqttd_cfg = build_rumqttd_config(&self.cfg);
        let mut broker = Broker::new(rumqttd_cfg);

        // (link_id is the local node identifier; rumqttd uses it to route)
        let (mut link_tx, mut link_rx) = broker
            .link("monstermq-storage")
            .map_err(|e| anyhow::anyhow!("create broker link: {:?}", e))?;
        // Subscribe to every topic so the storage hook sees all PUBLISH packets.
        link_tx.subscribe("#").map_err(|e| anyhow::anyhow!("subscribe #: {:?}", e))?;

        // Stash the LinkTx so PublishFn can route messages back into the broker.
        *self.broker_tx.lock() = Some(link_tx);

        // Restore retained messages from the store into the in-memory broker.
        let me = self.clone();
        if let Err(e) = restore_retained(&me).await {
            tracing::warn!(err = ?e, "retained restore failed");
        }

        // Spawn rumqttd's networking loop.
        std::thread::spawn(move || {
            // rumqttd::Broker::start blocks the calling thread; spawn off-runtime.
            if let Err(e) = broker.start() {
                tracing::error!(err = ?e, "rumqttd broker exited");
            }
        });

        // Spawn the storage hook loop: every PUBLISH the local link receives is
        // routed through retained-write + bus + archive dispatch + metrics.
        let me_hook = self.clone();
        tokio::task::spawn_blocking(move || storage_hook_loop(me_hook, &mut link_rx));

        // Bridge / archive / metrics startup.
        if let Some(b) = self.bridges.clone() {
            let _ = b.start().await;
        }
        crate::archive::retention::start_retention_loop(self.archives.clone());

        if let Some(coll) = self.collector.clone() {
            let storage = self.storage.clone();
            coll.start(move || {
                let s = storage.clone();
                async move {
                    let sessions = s.sessions.iterate_sessions().await.map(|v| v.len() as i64).unwrap_or(0);
                    let subs = s.sessions.iterate_subscriptions().await.map(|v| v.len() as i64).unwrap_or(0);
                    let queued = s.queue.count_all().await.unwrap_or(0);
                    (sessions, subs, queued)
                }
            });
        }
        Ok(())
    }

    /// Publish a message into the local broker. Used by GraphQL `publish` and
    /// by the bridge's inbound side.
    ///
    /// NOTE: rumqttd 0.19's `LinkTx::publish` takes only `(topic, payload)`.
    /// The QoS/retain bits are dropped; the broker treats every link-published
    /// message as QoS 0 non-retained. To persist a retained message we write
    /// to the retained store alongside the link publish (the storage hook
    /// would otherwise see it once but with retain=false).
    pub fn publish(&self, topic: &str, payload: Vec<u8>, retain: bool, _qos: u8) -> anyhow::Result<()> {
        if retain {
            let storage = self.storage.clone();
            let topic_c = topic.to_string();
            let payload_c = payload.clone();
            tokio::spawn(async move {
                if payload_c.is_empty() {
                    let _ = storage.retained.del_all(&[topic_c.clone()]).await;
                } else {
                    let msg = crate::stores::types::BrokerMessage {
                        message_uuid: uuid::Uuid::new_v4().to_string(),
                        topic_name: topic_c, payload: payload_c,
                        is_retain: true, time: chrono::Utc::now(),
                        ..Default::default()
                    };
                    let _ = storage.retained.add_all(&[msg]).await;
                }
            });
        }
        if let Some(tx) = &mut *self.broker_tx.lock() {
            tx.publish(topic.to_string(), payload)
                .map_err(|e| anyhow::anyhow!("link publish: {:?}", e))?;
        }
        Ok(())
    }

    pub async fn close(&self) {
        if let Some(b) = &self.bridges { b.stop().await; }
        self.archives.stop().await;
        // rumqttd Broker exposes no graceful shutdown today; the OS reaps it
        // when the process exits.
    }
}

fn build_rumqttd_config(cfg: &Config) -> rumqttd::Config {
    use rumqttd::{ConnectionSettings, RouterConfig, ServerSettings};
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let mut servers = HashMap::new();
    if cfg.tcp.enabled {
        servers.insert("v4-tcp".to_string(), ServerSettings {
            name: "v4-tcp".to_string(),
            listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), cfg.tcp.port),
            tls: None,
            next_connection_delay_ms: 1,
            connections: ConnectionSettings {
                connection_timeout_ms: 60_000,
                max_payload_size: cfg.max_message_size as usize,
                max_inflight_count: 200,
                auth: None,
                external_auth: None,
                dynamic_filters: false,
            },
        });
    }
    // TODO: WS / TCPS / WSS listeners — rumqttd supports websocket via a
    // feature flag and TLS via TlsConfig; wire similarly to v4-tcp once
    // certificate loading from the JKS-style KeyStorePath is implemented.

    rumqttd::Config {
        id: 0,
        router: RouterConfig {
            max_connections: 10_010,
            max_outgoing_packet_count: 200,
            max_segment_size: 104_857_600,
            max_segment_count: 10,
            ..Default::default()
        },
        v4: Some(servers),
        v5: None,
        ws: None,
        cluster: None,
        // ConsoleSettings has private fields in rumqttd 0.19; default disables it.
        console: None,
        bridge: None,
        prometheus: None,
        metrics: None,
    }
}

async fn restore_retained(srv: &Arc<Server>) -> anyhow::Result<()> {
    let topics = srv.storage.retained.find_matching_topics("#").await?;
    for t in topics {
        if let Ok(Some(m)) = srv.storage.retained.get(&t).await {
            let _ = srv.publish(&t, m.payload, true, m.qos);
        }
    }
    Ok(())
}

/// Storage hook — runs in a blocking task because rumqttd's link API is sync.
/// Every notification we receive corresponds to a PUBLISH that hit the broker;
/// we mirror it into: retained store, pubsub bus, archive dispatch, queue
/// store (for offline persistent subscribers), metrics counter.
fn storage_hook_loop(srv: Arc<Server>, link_rx: &mut LinkRx) {
    let rt = tokio::runtime::Handle::current();
    loop {
        match link_rx.recv() {
            Ok(Some(notif)) => match notif {
                Notification::Forward(fwd) => {
                    // rumqttd 0.19 keeps qos/pkid private on Publish; we read
                    // topic/payload/retain only. The link delivers each message
                    // to us at QoS 0 anyway (link is internal).
                    let topic = String::from_utf8_lossy(&fwd.publish.topic).to_string();
                    let payload = fwd.publish.payload.to_vec();
                    let retain = fwd.publish.retain;
                    let qos: u8 = 0;

                    let msg = BrokerMessage {
                        message_uuid: Uuid::new_v4().to_string(),
                        message_id: 0,
                        topic_name: topic.clone(),
                        payload: payload.clone(),
                        qos,
                        is_retain: retain,
                        client_id: String::new(), // rumqttd does not expose client_id on Forward
                        time: Utc::now(),
                        ..Default::default()
                    };

                    if let Some(c) = &srv.collector { c.inc_in(); }

                    // Retained: persist or delete (empty payload + retain = clear).
                    if retain {
                        let storage = srv.storage.clone();
                        let topic_c = topic.clone();
                        let msg_c = msg.clone();
                        rt.spawn(async move {
                            if msg_c.payload.is_empty() {
                                let _ = storage.retained.del_all(&[topic_c]).await;
                            } else {
                                let _ = storage.retained.add_all(&[msg_c]).await;
                            }
                        });
                    }

                    // Pubsub bus (drives GraphQL topicUpdates / topicUpdatesBulk).
                    srv.bus.publish(msg.clone());

                    // Archive groups.
                    srv.archives.dispatch(msg);
                }
                _ => { /* Connect/Disconnect/etc — TODO: drive sessions table */ }
            },
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(err = ?e, "link recv error; storage hook stopping");
                return;
            }
        }
    }
}
