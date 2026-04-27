package mqttclient

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"monstermq.io/edge/internal/stores"
)

// Manager loads MQTT-bridge device configs from the DeviceConfigStore and runs
// one Connector per enabled device.
type Manager struct {
	store     stores.DeviceConfigStore
	publisher LocalPublisher
	subBus    LocalSubscriber
	logger    *slog.Logger
	nodeID    string

	mu         sync.Mutex
	connectors map[string]*Connector
	lastConfig map[string]string // device name → raw JSON last deployed
}

func NewManager(store stores.DeviceConfigStore, publisher LocalPublisher, subBus LocalSubscriber, nodeID string, logger *slog.Logger) *Manager {
	return &Manager{
		store: store, publisher: publisher, subBus: subBus, logger: logger, nodeID: nodeID,
		connectors: map[string]*Connector{},
		lastConfig: map[string]string{},
	}
}

func (m *Manager) Start(ctx context.Context) error {
	devices, err := m.store.GetEnabledByNode(ctx, m.nodeID)
	if err != nil {
		return err
	}
	for _, d := range devices {
		if d.Type != "" && d.Type != "MQTT_CLIENT" {
			continue
		}
		var cfg Config
		if err := json.Unmarshal([]byte(d.Config), &cfg); err != nil {
			m.logger.Warn("bridge config parse failed", "name", d.Name, "err", err)
			continue
		}
		if cfg.ClientID == "" {
			cfg.ClientID = "edge-" + m.nodeID + "-" + d.Name
		}
		c := NewConnector(d.Name, cfg, m.publisher, m.subBus, m.logger)
		if err := c.Start(ctx); err != nil {
			m.logger.Warn("bridge start failed", "name", d.Name, "err", err)
			continue
		}
		m.mu.Lock()
		m.connectors[d.Name] = c
		m.lastConfig[d.Name] = d.Config
		m.mu.Unlock()
	}
	return nil
}

func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.connectors {
		c.Stop()
	}
	m.connectors = map[string]*Connector{}
}

// Reload reconciles the live connector set against the persisted device
// configs: starts new ones, restarts changed ones, stops removed/disabled
// ones. Call after every GraphQL mutation that touches an MQTT client.
func (m *Manager) Reload(ctx context.Context) error {
	devices, err := m.store.GetEnabledByNode(ctx, m.nodeID)
	if err != nil {
		return err
	}
	wanted := map[string]Config{}
	wantedRaw := map[string]string{}
	for _, d := range devices {
		if d.Type != "" && d.Type != "MQTT_CLIENT" {
			continue
		}
		var cfg Config
		if err := json.Unmarshal([]byte(d.Config), &cfg); err != nil {
			m.logger.Warn("bridge config parse failed", "name", d.Name, "err", err)
			continue
		}
		if cfg.ClientID == "" {
			cfg.ClientID = "edge-" + m.nodeID + "-" + d.Name
		}
		wanted[d.Name] = cfg
		wantedRaw[d.Name] = d.Config
	}

	m.mu.Lock()
	current := m.connectors
	currentRaw := m.lastConfig
	m.mu.Unlock()

	// Stop bridges that are no longer wanted, or whose stored config changed.
	for name, c := range current {
		if newRaw, keep := wantedRaw[name]; !keep || newRaw != currentRaw[name] {
			c.Stop()
			m.mu.Lock()
			delete(m.connectors, name)
			delete(m.lastConfig, name)
			m.mu.Unlock()
		}
	}

	// Start bridges that should be running but aren't.
	for name, cfg := range wanted {
		m.mu.Lock()
		_, running := m.connectors[name]
		m.mu.Unlock()
		if running {
			continue
		}
		c := NewConnector(name, cfg, m.publisher, m.subBus, m.logger)
		if err := c.Start(ctx); err != nil {
			m.logger.Warn("bridge start failed", "name", name, "err", err)
			continue
		}
		m.mu.Lock()
		m.connectors[name] = c
		m.lastConfig[name] = wantedRaw[name]
		m.mu.Unlock()
	}
	return nil
}

func (m *Manager) Active() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.connectors))
	for name := range m.connectors {
		out = append(out, name)
	}
	return out
}
