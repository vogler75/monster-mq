package config

type StoreType string

const (
	StoreNone     StoreType = "NONE"
	StoreMemory   StoreType = "MEMORY"
	StoreSQLite   StoreType = "SQLITE"
	StorePostgres StoreType = "POSTGRES"
	StoreMongoDB  StoreType = "MONGODB"
)

type Listener struct {
	Enabled          bool   `yaml:"Enabled"`
	Port             int    `yaml:"Port"`
	KeyStorePath     string `yaml:"KeyStorePath,omitempty"`
	KeyStorePassword string `yaml:"KeyStorePassword,omitempty"`
}

type SQLiteConfig struct {
	Path string `yaml:"Path"`
}

type PostgresConfig struct {
	URL  string `yaml:"Url"`
	User string `yaml:"User"`
	Pass string `yaml:"Pass"`
}

type MongoDBConfig struct {
	URL      string `yaml:"Url"`
	Database string `yaml:"Database"`
}

type UserManagementConfig struct {
	Enabled           bool   `yaml:"Enabled"`
	PasswordAlgorithm string `yaml:"PasswordAlgorithm"`
	AnonymousEnabled  bool   `yaml:"AnonymousEnabled"`
	AclCacheEnabled   bool   `yaml:"AclCacheEnabled"`
}

type MetricsConfig struct {
	Enabled                   bool `yaml:"Enabled"`
	CollectionIntervalSeconds int  `yaml:"CollectionIntervalSeconds"`
	RetentionHours            int  `yaml:"RetentionHours"`
}

// ArchiveConfig tunes the in-memory queue every archive group uses to
// decouple publish from disk write. Buffer is bounded — overflow drops
// new messages rather than growing memory.
type ArchiveConfig struct {
	BufferSize      int `yaml:"BufferSize"`      // 0 → 100_000
	MaxBatchSize    int `yaml:"MaxBatchSize"`    // 0 → 1000
	FlushIntervalMs int `yaml:"FlushIntervalMs"` // 0 → 250
}

type LoggingConfig struct {
	Level             string `yaml:"Level"`
	MqttSyslogEnabled bool   `yaml:"MqttSyslogEnabled"`
	RingBufferSize    int    `yaml:"RingBufferSize"`
}

type GraphQLConfig struct {
	Enabled bool `yaml:"Enabled"`
	Port    int  `yaml:"Port"`
}

type DashboardConfig struct {
	Enabled bool   `yaml:"Enabled"`
	Path    string `yaml:"Path,omitempty"` // optional: path to dashboard/dist (built UI)
}

// FeaturesConfig matches the JVM broker's Features block. Every feature
// defaults to enabled; explicitly set to false to disable. Only features
// this broker actually implements are honoured — others are accepted for
// schema parity but have no effect.
type FeaturesConfig struct {
	MqttClient *bool `yaml:"MqttClient,omitempty"` // MQTT-to-MQTT bridge
}

// Enabled returns the effective on/off for a feature, honoring the
// JVM-compatible "default true unless explicitly false" rule.
func (f FeaturesConfig) MqttClientEnabled() bool {
	if f.MqttClient == nil {
		return true
	}
	return *f.MqttClient
}

type Config struct {
	NodeID         string   `yaml:"NodeId"`
	TCP            Listener `yaml:"TCP"`
	TCPS           Listener `yaml:"TCPS"`
	WS             Listener `yaml:"WS"`
	WSS            Listener `yaml:"WSS"`
	MaxMessageSize int      `yaml:"MaxMessageSize"`

	DefaultStoreType  StoreType `yaml:"DefaultStoreType"`
	SessionStoreType  StoreType `yaml:"SessionStoreType"`
	RetainedStoreType StoreType `yaml:"RetainedStoreType"`
	ConfigStoreType   StoreType `yaml:"ConfigStoreType"`

	SQLite   SQLiteConfig   `yaml:"SQLite"`
	Postgres PostgresConfig `yaml:"Postgres"`
	MongoDB  MongoDBConfig  `yaml:"MongoDB"`

	UserManagement UserManagementConfig `yaml:"UserManagement"`
	Metrics        MetricsConfig        `yaml:"Metrics"`
	Archive        ArchiveConfig        `yaml:"Archive"`
	Logging        LoggingConfig        `yaml:"Logging"`
	GraphQL        GraphQLConfig        `yaml:"GraphQL"`
	Dashboard      DashboardConfig      `yaml:"Dashboard"`
	Features       FeaturesConfig       `yaml:"Features"`

	// QueuedMessagesEnabled selects how messages for offline persistent (clean=false)
	// sessions are held until the client reconnects.
	//
	//   true  → use the persistent QueueStore (SQLite/Postgres/MongoDB). Messages
	//           survive a broker restart.
	//   false → rely on mochi-mqtt's in-memory inflight buffer. Messages are lost
	//           on broker restart but lower latency / no DB writes per publish.
	QueuedMessagesEnabled bool `yaml:"QueuedMessagesEnabled"`
}

func Default() *Config {
	return &Config{
		NodeID:            "edge",
		TCP:               Listener{Enabled: true, Port: 1883},
		TCPS:              Listener{Enabled: false, Port: 8883},
		WS:                Listener{Enabled: false, Port: 1884},
		WSS:               Listener{Enabled: false, Port: 8884},
		MaxMessageSize:    1048576,
		DefaultStoreType:  StoreSQLite,
		SessionStoreType:  StoreSQLite,
		RetainedStoreType: StoreSQLite,
		ConfigStoreType:   StoreSQLite,
		SQLite:            SQLiteConfig{Path: "./data/monstermq.db"},
		UserManagement:    UserManagementConfig{Enabled: false, PasswordAlgorithm: "BCRYPT", AnonymousEnabled: true, AclCacheEnabled: true},
		Metrics:           MetricsConfig{Enabled: true, CollectionIntervalSeconds: 1, RetentionHours: 168},
		Archive:           ArchiveConfig{BufferSize: 100_000, MaxBatchSize: 1000, FlushIntervalMs: 250},
		Logging:           LoggingConfig{Level: "INFO", MqttSyslogEnabled: false, RingBufferSize: 1000},
		GraphQL:               GraphQLConfig{Enabled: true, Port: 8080},
		Dashboard:             DashboardConfig{Enabled: true},
		QueuedMessagesEnabled: true,
	}
}

// SessionStore returns the effective store type for sessions, falling back to DefaultStoreType.
func (c *Config) SessionStore() StoreType {
	if c.SessionStoreType != "" {
		return c.SessionStoreType
	}
	return c.DefaultStoreType
}

func (c *Config) RetainedStore() StoreType {
	if c.RetainedStoreType != "" {
		return c.RetainedStoreType
	}
	return c.DefaultStoreType
}

func (c *Config) ConfigStore() StoreType {
	if c.ConfigStoreType != "" {
		return c.ConfigStoreType
	}
	return c.DefaultStoreType
}
