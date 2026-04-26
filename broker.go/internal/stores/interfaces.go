package stores

import (
	"context"
	"time"
)

type MessageStore interface {
	Name() string
	Type() MessageStoreType
	Get(ctx context.Context, topic string) (*BrokerMessage, error)
	AddAll(ctx context.Context, msgs []BrokerMessage) error
	DelAll(ctx context.Context, topics []string) error
	FindMatchingMessages(ctx context.Context, pattern string, yield func(BrokerMessage) bool) error
	FindMatchingTopics(ctx context.Context, pattern string, yield func(string) bool) error
	PurgeOlderThan(ctx context.Context, t time.Time) (PurgeResult, error)
	EnsureTable(ctx context.Context) error
	Close() error
}

type MessageArchive interface {
	Name() string
	Type() MessageArchiveType
	AddHistory(ctx context.Context, msgs []BrokerMessage) error
	GetHistory(ctx context.Context, topic string, from, to *time.Time, limit int) ([]ArchivedMessage, error)
	PurgeOlderThan(ctx context.Context, t time.Time) (PurgeResult, error)
	EnsureTable(ctx context.Context) error
	Close() error
}

type ArchivedMessage struct {
	Topic     string    `json:"topic"`
	Timestamp time.Time `json:"timestamp"`
	Payload   []byte    `json:"payload"`
	QoS       byte      `json:"qos"`
	ClientID  string    `json:"clientId"`
}

type SessionInfo struct {
	ClientID              string
	NodeID                string
	CleanSession          bool
	Connected             bool
	UpdateTime            time.Time
	Information           string
	ClientAddress         string
	ProtocolVersion       int
	SessionExpiryInterval int64
	ReceiveMaximum        int
	MaximumPacketSize     int64
	TopicAliasMaximum     int
	LastWillTopic         string
	LastWillPayload       []byte
	LastWillQoS           byte
	LastWillRetain        bool
}

type SessionStore interface {
	SetClient(ctx context.Context, info SessionInfo) error
	SetConnected(ctx context.Context, clientID string, connected bool) error
	SetLastWill(ctx context.Context, clientID string, topic string, payload []byte, qos byte, retain bool) error
	IsConnected(ctx context.Context, clientID string) (bool, error)
	IsPresent(ctx context.Context, clientID string) (bool, error)
	GetSession(ctx context.Context, clientID string) (*SessionInfo, error)
	IterateSessions(ctx context.Context, yield func(SessionInfo) bool) error
	IterateSubscriptions(ctx context.Context, yield func(MqttSubscription) bool) error
	GetSubscriptionsForClient(ctx context.Context, clientID string) ([]MqttSubscription, error)
	AddSubscriptions(ctx context.Context, subs []MqttSubscription) error
	DelSubscriptions(ctx context.Context, subs []MqttSubscription) error
	DelClient(ctx context.Context, clientID string) error
	PurgeSessions(ctx context.Context) error
	Close() error
}

type QueueStore interface {
	Enqueue(ctx context.Context, clientID string, msg BrokerMessage) error
	EnqueueMulti(ctx context.Context, msg BrokerMessage, clientIDs []string) error
	Dequeue(ctx context.Context, clientID string, batchSize int) ([]BrokerMessage, error)
	Ack(ctx context.Context, clientID string, messageUUID string) error
	PurgeForClient(ctx context.Context, clientID string) (int64, error)
	Count(ctx context.Context, clientID string) (int64, error)
	CountAll(ctx context.Context) (int64, error)
	Close() error
}

type User struct {
	Username     string
	PasswordHash string
	Enabled      bool
	CanSubscribe bool
	CanPublish   bool
	IsAdmin      bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type AclRule struct {
	ID           string
	Username     string
	TopicPattern string
	CanSubscribe bool
	CanPublish   bool
	Priority     int
	CreatedAt    time.Time
}

type UserStore interface {
	CreateUser(ctx context.Context, u User) error
	UpdateUser(ctx context.Context, u User) error
	DeleteUser(ctx context.Context, username string) error
	GetUser(ctx context.Context, username string) (*User, error)
	GetAllUsers(ctx context.Context) ([]User, error)
	ValidateCredentials(ctx context.Context, username, password string) (*User, error)
	CreateAclRule(ctx context.Context, r AclRule) error
	UpdateAclRule(ctx context.Context, r AclRule) error
	DeleteAclRule(ctx context.Context, id string) error
	GetUserAclRules(ctx context.Context, username string) ([]AclRule, error)
	GetAllAclRules(ctx context.Context) ([]AclRule, error)
	LoadAll(ctx context.Context) ([]User, []AclRule, error)
	Close() error
}

type ArchiveGroupConfig struct {
	Name              string
	Enabled           bool
	TopicFilters      []string
	RetainedOnly      bool
	LastValType       MessageStoreType
	ArchiveType       MessageArchiveType
	LastValRetention  string
	ArchiveRetention  string
	PurgeInterval     string
	PayloadFormat     PayloadFormat
}

type ArchiveConfigStore interface {
	GetAll(ctx context.Context) ([]ArchiveGroupConfig, error)
	Get(ctx context.Context, name string) (*ArchiveGroupConfig, error)
	Save(ctx context.Context, cfg ArchiveGroupConfig) error
	Update(ctx context.Context, cfg ArchiveGroupConfig) error
	Delete(ctx context.Context, name string) error
	Close() error
}

type DeviceConfig struct {
	Name      string
	Namespace string
	NodeID    string
	Type      string
	Enabled   bool
	Config    string // raw JSON
	CreatedAt time.Time
	UpdatedAt time.Time
}

type DeviceConfigStore interface {
	GetAll(ctx context.Context) ([]DeviceConfig, error)
	GetByNode(ctx context.Context, nodeID string) ([]DeviceConfig, error)
	GetEnabledByNode(ctx context.Context, nodeID string) ([]DeviceConfig, error)
	Get(ctx context.Context, name string) (*DeviceConfig, error)
	Save(ctx context.Context, d DeviceConfig) error
	Delete(ctx context.Context, name string) error
	Toggle(ctx context.Context, name string, enabled bool) (*DeviceConfig, error)
	Reassign(ctx context.Context, name string, nodeID string) (*DeviceConfig, error)
	Close() error
}

type MetricKind string

const (
	MetricBroker     MetricKind = "broker"
	MetricSession    MetricKind = "session"
	MetricMqttClient MetricKind = "mqttclient"
)

type MetricsStore interface {
	StoreMetrics(ctx context.Context, kind MetricKind, ts time.Time, identifier string, jsonPayload string) error
	GetLatest(ctx context.Context, kind MetricKind, identifier string) (time.Time, string, error)
	GetHistory(ctx context.Context, kind MetricKind, identifier string, from, to time.Time, limit int) ([]MetricsRow, error)
	PurgeOlderThan(ctx context.Context, t time.Time) (int64, error)
	Close() error
}

type MetricsRow struct {
	Timestamp time.Time
	Payload   string // JSON
}
