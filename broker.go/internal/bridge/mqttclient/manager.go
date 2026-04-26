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
}

func NewManager(store stores.DeviceConfigStore, publisher LocalPublisher, subBus LocalSubscriber, nodeID string, logger *slog.Logger) *Manager {
	return &Manager{
		store: store, publisher: publisher, subBus: subBus, logger: logger, nodeID: nodeID,
		connectors: map[string]*Connector{},
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

func (m *Manager) Active() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.connectors))
	for name := range m.connectors {
		out = append(out, name)
	}
	return out
}
