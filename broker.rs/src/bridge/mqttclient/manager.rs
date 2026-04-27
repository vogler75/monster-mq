use super::connector::{Config as BridgeConfig, Connector, LocalPublishFn};
use crate::pubsub::Bus as PubsubBus;
use crate::stores::traits::DeviceConfigStore;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// Loads MQTT-bridge device configs from the DeviceConfigStore and runs one
/// Connector per enabled device. Mirrors `internal/bridge/mqttclient/manager.go`.
pub struct Manager {
    store: Arc<dyn DeviceConfigStore>,
    publisher: LocalPublishFn,
    pubsub: Arc<PubsubBus>,
    pub node_id: String,
    inner: Mutex<Inner>,
}

struct Inner {
    connectors: HashMap<String, Arc<Connector>>,
    last_config: HashMap<String, String>,
}

impl Manager {
    pub fn new(store: Arc<dyn DeviceConfigStore>, publisher: LocalPublishFn, pubsub: Arc<PubsubBus>, node_id: String) -> Arc<Self> {
        Arc::new(Self {
            store, publisher, pubsub, node_id,
            inner: Mutex::new(Inner { connectors: HashMap::new(), last_config: HashMap::new() }),
        })
    }

    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        let devices = self.store.get_enabled_by_node(&self.node_id).await?;
        for d in devices {
            if !d.r#type.is_empty() && d.r#type != "MQTT_CLIENT" { continue; }
            let cfg: BridgeConfig = match serde_json::from_str(&d.config) {
                Ok(c) => c,
                Err(e) => { tracing::warn!(name = d.name, err = ?e, "bridge config parse failed"); continue; }
            };
            let mut cfg = cfg;
            if cfg.client_id.is_empty() {
                cfg.client_id = format!("edge-{}-{}", self.node_id, d.name);
            }
            let c = Connector::new(d.name.clone(), cfg, self.publisher.clone(), self.pubsub.clone());
            if let Err(e) = c.clone().start().await {
                tracing::warn!(name = d.name, err = ?e, "bridge start failed");
                continue;
            }
            let mut g = self.inner.lock();
            g.connectors.insert(d.name.clone(), c);
            g.last_config.insert(d.name.clone(), d.config.clone());
        }
        Ok(())
    }

    pub async fn stop(&self) {
        let conns: Vec<Arc<Connector>> = self.inner.lock().connectors.values().cloned().collect();
        for c in conns { c.stop().await; }
        self.inner.lock().connectors.clear();
    }

    /// Reconciles the live connector set against the persisted state.
    /// Call after every GraphQL mutation that touches an MQTT client.
    pub async fn reload(self: Arc<Self>) -> anyhow::Result<()> {
        let devices = self.store.get_enabled_by_node(&self.node_id).await?;
        let mut wanted: HashMap<String, BridgeConfig> = HashMap::new();
        let mut wanted_raw: HashMap<String, String> = HashMap::new();
        for d in devices {
            if !d.r#type.is_empty() && d.r#type != "MQTT_CLIENT" { continue; }
            let cfg: BridgeConfig = match serde_json::from_str(&d.config) {
                Ok(c) => c,
                Err(e) => { tracing::warn!(name = d.name, err = ?e, "bridge config parse failed"); continue; }
            };
            let mut cfg = cfg;
            if cfg.client_id.is_empty() {
                cfg.client_id = format!("edge-{}-{}", self.node_id, d.name);
            }
            wanted.insert(d.name.clone(), cfg);
            wanted_raw.insert(d.name, d.config);
        }
        // Stop bridges that are no longer wanted, or whose config changed.
        let to_stop: Vec<(String, Arc<Connector>)> = {
            let g = self.inner.lock();
            g.connectors.iter().filter_map(|(name, c)| {
                let stop = match wanted_raw.get(name) {
                    Some(raw) => g.last_config.get(name).map(|prev| raw != prev).unwrap_or(true),
                    None => true,
                };
                if stop { Some((name.clone(), c.clone())) } else { None }
            }).collect()
        };
        for (name, c) in to_stop {
            c.stop().await;
            let mut g = self.inner.lock();
            g.connectors.remove(&name);
            g.last_config.remove(&name);
        }
        // Start bridges that should be running but aren't.
        for (name, cfg) in wanted {
            if self.inner.lock().connectors.contains_key(&name) { continue; }
            let c = Connector::new(name.clone(), cfg, self.publisher.clone(), self.pubsub.clone());
            if let Err(e) = c.clone().start().await {
                tracing::warn!(name, err = ?e, "bridge start failed");
                continue;
            }
            let mut g = self.inner.lock();
            g.connectors.insert(name.clone(), c);
            if let Some(raw) = wanted_raw.get(&name) {
                g.last_config.insert(name, raw.clone());
            }
        }
        Ok(())
    }

    pub fn active(&self) -> Vec<String> {
        self.inner.lock().connectors.keys().cloned().collect()
    }
}
