package stores

import "time"

// BrokerMessage is the canonical in-broker representation of a published MQTT message.
// Field names mirror MonsterMQ's BrokerMessage (Kotlin) so storage rows are byte-compatible.
type BrokerMessage struct {
	MessageUUID string    `json:"messageUuid"`
	MessageID   uint16    `json:"messageId"` // MQTT packet identifier (0 for QoS 0)
	TopicName   string    `json:"topicName"`
	Payload     []byte    `json:"payload"`
	QoS         byte      `json:"qos"`
	IsRetain    bool      `json:"isRetain"`
	IsDup       bool      `json:"isDup"`
	IsQueued    bool      `json:"isQueued"`
	ClientID    string    `json:"clientId"`
	Time        time.Time `json:"time"`

	// MQTT v5 properties (subset)
	PayloadFormatIndicator *byte             `json:"payloadFormatIndicator,omitempty"`
	MessageExpiryInterval  *uint32           `json:"messageExpiryInterval,omitempty"`
	ContentType            string            `json:"contentType,omitempty"`
	ResponseTopic          string            `json:"responseTopic,omitempty"`
	CorrelationData        []byte            `json:"correlationData,omitempty"`
	UserProperties         map[string]string `json:"userProperties,omitempty"`
}

// MqttSubscription mirrors MonsterMQ's MqttSubscription type.
type MqttSubscription struct {
	ClientID          string `json:"clientId"`
	TopicFilter       string `json:"topicFilter"`
	QoS               byte   `json:"qos"`
	NoLocal           bool   `json:"noLocal,omitempty"`
	RetainAsPublished bool   `json:"retainAsPublished,omitempty"`
	RetainHandling    byte   `json:"retainHandling,omitempty"`
	SubscriptionID    int    `json:"subscriptionId,omitempty"`
}

type PurgeResult struct {
	DeletedRows int64
	Err         error
}

type MessageStoreType string

const (
	MessageStoreNone     MessageStoreType = "NONE"
	MessageStoreMemory   MessageStoreType = "MEMORY"
	MessageStorePostgres MessageStoreType = "POSTGRES"
	MessageStoreMongoDB  MessageStoreType = "MONGODB"
	MessageStoreSQLite   MessageStoreType = "SQLITE"
)

type MessageArchiveType string

const (
	ArchiveNone     MessageArchiveType = "NONE"
	ArchivePostgres MessageArchiveType = "POSTGRES"
	ArchiveMongoDB  MessageArchiveType = "MONGODB"
	ArchiveSQLite   MessageArchiveType = "SQLITE"
)

type PayloadFormat string

const (
	PayloadDefault PayloadFormat = "DEFAULT"
	PayloadJSON    PayloadFormat = "JSON"
)
