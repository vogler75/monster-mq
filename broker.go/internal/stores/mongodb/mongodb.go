// Package mongodb implements all seven storage interfaces against MongoDB
// using the official Go driver. Collection names mirror MonsterMQ's MongoDB
// implementation so the same database can be used by either broker.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/crypto/bcrypt"

	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
)

// DB wraps a Mongo client + database handle.
type DB struct {
	client *mongo.Client
	db     *mongo.Database
}

func Open(ctx context.Context, uri, dbName string) (*DB, error) {
	cli, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connect mongo: %w", err)
	}
	if err := cli.Ping(ctx, nil); err != nil {
		_ = cli.Disconnect(ctx)
		return nil, fmt.Errorf("ping mongo: %w", err)
	}
	return &DB{client: cli, db: cli.Database(dbName)}, nil
}

func (d *DB) Close() error {
	return d.client.Disconnect(context.Background())
}

func Build(ctx context.Context, cfg *config.Config) (*stores.Storage, *DB, error) {
	db, err := Open(ctx, cfg.MongoDB.URL, cfg.MongoDB.Database)
	if err != nil {
		return nil, nil, err
	}
	retained := &MessageStore{name: "retainedmessages", db: db}
	sessions := &SessionStore{db: db}
	queue := &QueueStore{db: db, visibility: 30 * time.Second}
	users := &UserStore{db: db}
	archives := &ArchiveConfigStore{db: db}
	devices := &DeviceConfigStore{db: db}
	metricsStore := &MetricsStore{db: db}
	for _, t := range []interface{ EnsureTable(context.Context) error }{
		retained, sessions, queue, users, archives, devices, metricsStore,
	} {
		if err := t.EnsureTable(ctx); err != nil {
			db.Close()
			return nil, nil, err
		}
	}
	return &stores.Storage{
		Backend:       config.StoreMongoDB,
		Sessions:      sessions, Subscriptions: sessions,
		Queue: queue, Retained: retained, Users: users,
		ArchiveConfig: archives, DeviceConfig: devices, Metrics: metricsStore,
		Closer: db.Close,
	}, db, nil
}

// MessageStore (retained) ------------------------------------------------

type MessageStore struct {
	name string
	db   *DB
}

func NewMessageStore(name string, db *DB) *MessageStore { return &MessageStore{name: name, db: db} }

func (s *MessageStore) Name() string                  { return s.name }
func (s *MessageStore) Type() stores.MessageStoreType { return stores.MessageStoreMongoDB }
func (s *MessageStore) Close() error                  { return nil }

func (s *MessageStore) coll() *mongo.Collection { return s.db.db.Collection(strings.ToLower(s.name)) }

func (s *MessageStore) EnsureTable(ctx context.Context) error {
	_, err := s.coll().Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "topic", Value: 1}}, Options: options.Index().SetUnique(true)})
	return err
}

func (s *MessageStore) Get(ctx context.Context, topic string) (*stores.BrokerMessage, error) {
	var doc bson.M
	err := s.coll().FindOne(ctx, bson.M{"topic": topic}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return docToBrokerMessage(doc), nil
}

func docToBrokerMessage(doc bson.M) *stores.BrokerMessage {
	m := &stores.BrokerMessage{
		TopicName:   getStr(doc, "topic"),
		MessageUUID: getStr(doc, "message_uuid"),
		ClientID:    getStr(doc, "client_id"),
		QoS:         byte(getInt(doc, "qos")),
		IsRetain:    getBool(doc, "retained"),
	}
	if payload, ok := doc["payload"].(primitiveBinary); ok {
		m.Payload = payload.Data
	} else if b, ok := doc["payload"].(bson.Binary); ok {
		m.Payload = b.Data
	}
	if t, ok := doc["time"].(bson.DateTime); ok {
		m.Time = t.Time()
	} else if t, ok := doc["time"].(time.Time); ok {
		m.Time = t
	}
	return m
}

// primitiveBinary is a placeholder kept compatible with older driver type usage.
type primitiveBinary = bson.Binary

func getStr(d bson.M, k string) string {
	if v, ok := d[k].(string); ok {
		return v
	}
	return ""
}
func getInt(d bson.M, k string) int {
	switch v := d[k].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	case float64:
		return int(v)
	}
	return 0
}
func getBool(d bson.M, k string) bool {
	if v, ok := d[k].(bool); ok {
		return v
	}
	return false
}

func getTime(d bson.M, k string) time.Time {
	switch v := d[k].(type) {
	case time.Time:
		return v.UTC()
	case bson.DateTime:
		return v.Time().UTC()
	}
	return time.Time{}
}

func (s *MessageStore) AddAll(ctx context.Context, msgs []stores.BrokerMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(msgs))
	for _, m := range msgs {
		doc := bson.M{
			"topic":        m.TopicName,
			"time":         m.Time.UTC(),
			"payload":      bson.Binary{Data: m.Payload},
			"qos":          int(m.QoS),
			"retained":     m.IsRetain,
			"client_id":    m.ClientID,
			"message_uuid": m.MessageUUID,
		}
		if m.MessageExpiryInterval != nil {
			doc["message_expiry_interval"] = int64(*m.MessageExpiryInterval)
		}
		models = append(models, mongo.NewReplaceOneModel().SetFilter(bson.M{"topic": m.TopicName}).SetReplacement(doc).SetUpsert(true))
	}
	_, err := s.coll().BulkWrite(ctx, models)
	return err
}

func (s *MessageStore) DelAll(ctx context.Context, topics []string) error {
	if len(topics) == 0 {
		return nil
	}
	_, err := s.coll().DeleteMany(ctx, bson.M{"topic": bson.M{"$in": topics}})
	return err
}

func (s *MessageStore) FindMatchingMessages(ctx context.Context, pattern string, yield func(stores.BrokerMessage) bool) error {
	cur, err := s.coll().Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return err
		}
		topic := getStr(doc, "topic")
		if !matchTopic(pattern, topic) {
			continue
		}
		msg := docToBrokerMessage(doc)
		msg.IsRetain = true
		if !yield(*msg) {
			return nil
		}
	}
	return cur.Err()
}

func (s *MessageStore) FindMatchingTopics(ctx context.Context, pattern string, yield func(string) bool) error {
	cur, err := s.coll().Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"topic": 1}))
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return err
		}
		t := getStr(doc, "topic")
		if matchTopic(pattern, t) {
			if !yield(t) {
				return nil
			}
		}
	}
	return cur.Err()
}

func (s *MessageStore) PurgeOlderThan(ctx context.Context, t time.Time) (stores.PurgeResult, error) {
	res, err := s.coll().DeleteMany(ctx, bson.M{"time": bson.M{"$lt": t.UTC()}})
	if err != nil {
		return stores.PurgeResult{Err: err}, err
	}
	return stores.PurgeResult{DeletedRows: res.DeletedCount}, nil
}

// MessageArchive ---------------------------------------------------------

type MessageArchive struct {
	name string
	db   *DB
}

func NewMessageArchive(name string, db *DB, _ stores.PayloadFormat) *MessageArchive {
	return &MessageArchive{name: name, db: db}
}
func (a *MessageArchive) Name() string                    { return a.name }
func (a *MessageArchive) Type() stores.MessageArchiveType { return stores.ArchiveMongoDB }
func (a *MessageArchive) Close() error                    { return nil }
func (a *MessageArchive) coll() *mongo.Collection         { return a.db.db.Collection(strings.ToLower(a.name)) }

func (a *MessageArchive) EnsureTable(ctx context.Context) error {
	_, err := a.coll().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "topic", Value: 1}, {Key: "time", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "time", Value: 1}}},
	})
	return err
}

func (a *MessageArchive) AddHistory(ctx context.Context, msgs []stores.BrokerMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	docs := make([]any, 0, len(msgs))
	for _, m := range msgs {
		docs = append(docs, bson.M{
			"topic": m.TopicName, "time": m.Time.UTC(),
			"payload": bson.Binary{Data: m.Payload}, "qos": int(m.QoS),
			"retained": m.IsRetain, "client_id": m.ClientID, "message_uuid": m.MessageUUID,
		})
	}
	_, err := a.coll().InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

func (a *MessageArchive) GetHistory(ctx context.Context, topic string, from, to *time.Time, limit int) ([]stores.ArchivedMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	filter := bson.M{}
	if strings.ContainsAny(topic, "+#") {
		// best-effort regex translation
		re := strings.ReplaceAll(strings.ReplaceAll(topic, "#", ".*"), "+", "[^/]+")
		filter["topic"] = bson.M{"$regex": "^" + re + "$"}
	} else {
		filter["topic"] = topic
	}
	if from != nil || to != nil {
		t := bson.M{}
		if from != nil {
			t["$gte"] = from.UTC()
		}
		if to != nil {
			t["$lte"] = to.UTC()
		}
		filter["time"] = t
	}
	cur, err := a.coll().Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "time", Value: -1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := []stores.ArchivedMessage{}
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		m := stores.ArchivedMessage{
			Topic:    getStr(doc, "topic"),
			ClientID: getStr(doc, "client_id"),
			QoS:      byte(getInt(doc, "qos")),
		}
		if t, ok := doc["time"].(bson.DateTime); ok {
			m.Timestamp = t.Time()
		} else if t, ok := doc["time"].(time.Time); ok {
			m.Timestamp = t
		}
		if b, ok := doc["payload"].(bson.Binary); ok {
			m.Payload = b.Data
		}
		out = append(out, m)
	}
	return out, cur.Err()
}

func (a *MessageArchive) PurgeOlderThan(ctx context.Context, t time.Time) (stores.PurgeResult, error) {
	res, err := a.coll().DeleteMany(ctx, bson.M{"time": bson.M{"$lt": t.UTC()}})
	if err != nil {
		return stores.PurgeResult{Err: err}, err
	}
	return stores.PurgeResult{DeletedRows: res.DeletedCount}, nil
}

// SessionStore -----------------------------------------------------------

type SessionStore struct{ db *DB }

func (s *SessionStore) Close() error { return nil }
func (s *SessionStore) sessionsColl() *mongo.Collection { return s.db.db.Collection("sessions") }
func (s *SessionStore) subsColl() *mongo.Collection     { return s.db.db.Collection("subscriptions") }

func (s *SessionStore) EnsureTable(ctx context.Context) error {
	if _, err := s.sessionsColl().Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "client_id", Value: 1}}, Options: options.Index().SetUnique(true)}); err != nil {
		return err
	}
	_, err := s.subsColl().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "client_id", Value: 1}, {Key: "topic", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "topic", Value: 1}}},
	})
	return err
}

func (s *SessionStore) SetClient(ctx context.Context, info stores.SessionInfo) error {
	_, err := s.sessionsColl().UpdateOne(ctx,
		bson.M{"client_id": info.ClientID},
		bson.M{"$set": bson.M{
			"node_id": info.NodeID, "clean_session": info.CleanSession,
			"connected": info.Connected, "update_time": time.Now().UTC(),
			"information": info.Information,
		}},
		options.UpdateOne().SetUpsert(true))
	return err
}

func (s *SessionStore) SetConnected(ctx context.Context, clientID string, connected bool) error {
	_, err := s.sessionsColl().UpdateOne(ctx, bson.M{"client_id": clientID},
		bson.M{"$set": bson.M{"connected": connected, "update_time": time.Now().UTC()}})
	return err
}

func (s *SessionStore) SetLastWill(ctx context.Context, clientID, topic string, payload []byte, qos byte, retain bool) error {
	_, err := s.sessionsColl().UpdateOne(ctx, bson.M{"client_id": clientID},
		bson.M{"$set": bson.M{
			"last_will_topic": topic, "last_will_message": bson.Binary{Data: payload},
			"last_will_qos": int(qos), "last_will_retain": retain,
		}})
	return err
}

func (s *SessionStore) IsConnected(ctx context.Context, clientID string) (bool, error) {
	var doc bson.M
	err := s.sessionsColl().FindOne(ctx, bson.M{"client_id": clientID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return getBool(doc, "connected"), nil
}

func (s *SessionStore) IsPresent(ctx context.Context, clientID string) (bool, error) {
	n, err := s.sessionsColl().CountDocuments(ctx, bson.M{"client_id": clientID}, options.Count().SetLimit(1))
	return n > 0, err
}

func (s *SessionStore) GetSession(ctx context.Context, clientID string) (*stores.SessionInfo, error) {
	var doc bson.M
	err := s.sessionsColl().FindOne(ctx, bson.M{"client_id": clientID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return docToSession(doc), nil
}

func docToSession(doc bson.M) *stores.SessionInfo {
	info := &stores.SessionInfo{
		ClientID:     getStr(doc, "client_id"),
		NodeID:       getStr(doc, "node_id"),
		CleanSession: getBool(doc, "clean_session"),
		Connected:    getBool(doc, "connected"),
		Information:  getStr(doc, "information"),
		LastWillTopic: getStr(doc, "last_will_topic"),
		LastWillRetain: getBool(doc, "last_will_retain"),
		LastWillQoS:    byte(getInt(doc, "last_will_qos")),
	}
	if t, ok := doc["update_time"].(bson.DateTime); ok {
		info.UpdateTime = t.Time()
	} else if t, ok := doc["update_time"].(time.Time); ok {
		info.UpdateTime = t
	}
	if b, ok := doc["last_will_message"].(bson.Binary); ok {
		info.LastWillPayload = b.Data
	}
	return info
}

func (s *SessionStore) IterateSessions(ctx context.Context, yield func(stores.SessionInfo) bool) error {
	cur, err := s.sessionsColl().Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return err
		}
		if !yield(*docToSession(doc)) {
			return nil
		}
	}
	return cur.Err()
}

func (s *SessionStore) IterateSubscriptions(ctx context.Context, yield func(stores.MqttSubscription) bool) error {
	cur, err := s.subsColl().Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return err
		}
		sub := stores.MqttSubscription{
			ClientID: getStr(doc, "client_id"), TopicFilter: getStr(doc, "topic"),
			QoS: byte(getInt(doc, "qos")),
		}
		if !yield(sub) {
			return nil
		}
	}
	return cur.Err()
}

func (s *SessionStore) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]stores.MqttSubscription, error) {
	cur, err := s.subsColl().Find(ctx, bson.M{"client_id": clientID})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := []stores.MqttSubscription{}
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		out = append(out, stores.MqttSubscription{
			ClientID: getStr(doc, "client_id"), TopicFilter: getStr(doc, "topic"),
			QoS: byte(getInt(doc, "qos")),
			NoLocal: getBool(doc, "no_local"),
			RetainAsPublished: getBool(doc, "retain_as_published"),
			RetainHandling: byte(getInt(doc, "retain_handling")),
		})
	}
	return out, cur.Err()
}

func (s *SessionStore) AddSubscriptions(ctx context.Context, subs []stores.MqttSubscription) error {
	if len(subs) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(subs))
	for _, sub := range subs {
		models = append(models, mongo.NewReplaceOneModel().
			SetFilter(bson.M{"client_id": sub.ClientID, "topic": sub.TopicFilter}).
			SetReplacement(bson.M{
				"client_id": sub.ClientID, "topic": sub.TopicFilter,
				"qos": int(sub.QoS), "wildcard": strings.ContainsAny(sub.TopicFilter, "+#"),
				"no_local": sub.NoLocal, "retain_handling": int(sub.RetainHandling),
				"retain_as_published": sub.RetainAsPublished,
			}).SetUpsert(true))
	}
	_, err := s.subsColl().BulkWrite(ctx, models)
	return err
}

func (s *SessionStore) DelSubscriptions(ctx context.Context, subs []stores.MqttSubscription) error {
	if len(subs) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(subs))
	for _, sub := range subs {
		models = append(models, mongo.NewDeleteOneModel().SetFilter(bson.M{"client_id": sub.ClientID, "topic": sub.TopicFilter}))
	}
	_, err := s.subsColl().BulkWrite(ctx, models)
	return err
}

func (s *SessionStore) DelClient(ctx context.Context, clientID string) error {
	if _, err := s.subsColl().DeleteMany(ctx, bson.M{"client_id": clientID}); err != nil {
		return err
	}
	_, err := s.sessionsColl().DeleteOne(ctx, bson.M{"client_id": clientID})
	return err
}

func (s *SessionStore) PurgeSessions(ctx context.Context) error {
	_, err := s.sessionsColl().DeleteMany(ctx, bson.M{"clean_session": true, "connected": false})
	return err
}

// QueueStore ------------------------------------------------------------

type QueueStore struct {
	db         *DB
	visibility time.Duration
}

func (q *QueueStore) Close() error                  { return nil }
func (q *QueueStore) coll() *mongo.Collection       { return q.db.db.Collection("messagequeue") }
func (q *QueueStore) EnsureTable(ctx context.Context) error {
	_, err := q.coll().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "client_id", Value: 1}, {Key: "vt", Value: 1}}},
		{Keys: bson.D{{Key: "client_id", Value: 1}, {Key: "message_uuid", Value: 1}}},
	})
	return err
}

func (q *QueueStore) Enqueue(ctx context.Context, clientID string, msg stores.BrokerMessage) error {
	return q.EnqueueMulti(ctx, msg, []string{clientID})
}

func (q *QueueStore) EnqueueMulti(ctx context.Context, msg stores.BrokerMessage, clientIDs []string) error {
	if len(clientIDs) == 0 {
		return nil
	}
	docs := make([]any, 0, len(clientIDs))
	now := time.Now().Unix()
	for _, cid := range clientIDs {
		docs = append(docs, bson.M{
			"message_uuid": msg.MessageUUID, "client_id": cid, "topic": msg.TopicName,
			"payload":      bson.Binary{Data: msg.Payload}, "qos": int(msg.QoS),
			"retained":     msg.IsRetain, "publisher_id": msg.ClientID,
			"creation_time": msg.Time.UnixMilli(),
			"vt":            now, "read_ct": 0,
		})
	}
	_, err := q.coll().InsertMany(ctx, docs)
	return err
}

func (q *QueueStore) Dequeue(ctx context.Context, clientID string, batchSize int) ([]stores.BrokerMessage, error) {
	if batchSize <= 0 {
		batchSize = 10
	}
	now := time.Now().Unix()
	newVT := now + int64(q.visibility.Seconds())
	cur, err := q.coll().Find(ctx,
		bson.M{"client_id": clientID, "vt": bson.M{"$lte": now}},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(batchSize)))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := []stores.BrokerMessage{}
	ids := []any{}
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		ids = append(ids, doc["_id"])
		m := stores.BrokerMessage{
			MessageUUID: getStr(doc, "message_uuid"),
			TopicName:   getStr(doc, "topic"),
			QoS:         byte(getInt(doc, "qos")),
			IsRetain:    getBool(doc, "retained"),
			ClientID:    getStr(doc, "publisher_id"),
			IsQueued:    true,
		}
		if b, ok := doc["payload"].(bson.Binary); ok {
			m.Payload = b.Data
		}
		if v, ok := doc["creation_time"].(int64); ok {
			m.Time = time.UnixMilli(v)
		}
		out = append(out, m)
	}
	if len(ids) > 0 {
		_, err := q.coll().UpdateMany(ctx, bson.M{"_id": bson.M{"$in": ids}}, bson.M{"$set": bson.M{"vt": newVT}, "$inc": bson.M{"read_ct": 1}})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (q *QueueStore) Ack(ctx context.Context, clientID, messageUUID string) error {
	_, err := q.coll().DeleteOne(ctx, bson.M{"client_id": clientID, "message_uuid": messageUUID})
	return err
}
func (q *QueueStore) PurgeForClient(ctx context.Context, clientID string) (int64, error) {
	res, err := q.coll().DeleteMany(ctx, bson.M{"client_id": clientID})
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}
func (q *QueueStore) Count(ctx context.Context, clientID string) (int64, error) {
	return q.coll().CountDocuments(ctx, bson.M{"client_id": clientID})
}
func (q *QueueStore) CountAll(ctx context.Context) (int64, error) {
	return q.coll().CountDocuments(ctx, bson.M{})
}

// UserStore -------------------------------------------------------------

type UserStore struct{ db *DB }

func (u *UserStore) Close() error                          { return nil }
func (u *UserStore) usersColl() *mongo.Collection          { return u.db.db.Collection("users") }
func (u *UserStore) aclColl() *mongo.Collection            { return u.db.db.Collection("usersacl") }
func (u *UserStore) EnsureTable(ctx context.Context) error {
	if _, err := u.usersColl().Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "username", Value: 1}}, Options: options.Index().SetUnique(true)}); err != nil {
		return err
	}
	_, err := u.aclColl().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "username", Value: 1}}},
		{Keys: bson.D{{Key: "priority", Value: -1}}},
	})
	return err
}
func (u *UserStore) CreateUser(ctx context.Context, user stores.User) error {
	now := time.Now().UTC()
	_, err := u.usersColl().InsertOne(ctx, bson.M{
		"username": user.Username, "password_hash": user.PasswordHash,
		"enabled": user.Enabled, "can_subscribe": user.CanSubscribe,
		"can_publish": user.CanPublish, "is_admin": user.IsAdmin,
		"created_at": now, "updated_at": now,
	})
	return err
}
func (u *UserStore) UpdateUser(ctx context.Context, user stores.User) error {
	_, err := u.usersColl().UpdateOne(ctx, bson.M{"username": user.Username},
		bson.M{"$set": bson.M{
			"password_hash": user.PasswordHash, "enabled": user.Enabled,
			"can_subscribe": user.CanSubscribe, "can_publish": user.CanPublish,
			"is_admin": user.IsAdmin, "updated_at": time.Now().UTC(),
		}})
	return err
}
func (u *UserStore) DeleteUser(ctx context.Context, username string) error {
	if _, err := u.aclColl().DeleteMany(ctx, bson.M{"username": username}); err != nil {
		return err
	}
	_, err := u.usersColl().DeleteOne(ctx, bson.M{"username": username})
	return err
}
func (u *UserStore) GetUser(ctx context.Context, username string) (*stores.User, error) {
	var doc bson.M
	err := u.usersColl().FindOne(ctx, bson.M{"username": username}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return docToUser(doc), nil
}
func docToUser(doc bson.M) *stores.User {
	user := &stores.User{
		Username:     getStr(doc, "username"),
		PasswordHash: getStr(doc, "password_hash"),
		Enabled:      getBool(doc, "enabled"),
		CanSubscribe: getBool(doc, "can_subscribe"),
		CanPublish:   getBool(doc, "can_publish"),
		IsAdmin:      getBool(doc, "is_admin"),
	}
	if t, ok := doc["created_at"].(bson.DateTime); ok {
		user.CreatedAt = t.Time()
	} else if t, ok := doc["created_at"].(time.Time); ok {
		user.CreatedAt = t
	}
	if t, ok := doc["updated_at"].(bson.DateTime); ok {
		user.UpdatedAt = t.Time()
	} else if t, ok := doc["updated_at"].(time.Time); ok {
		user.UpdatedAt = t
	}
	return user
}
func (u *UserStore) GetAllUsers(ctx context.Context) ([]stores.User, error) {
	cur, err := u.usersColl().Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "username", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := []stores.User{}
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		out = append(out, *docToUser(doc))
	}
	return out, cur.Err()
}
func (u *UserStore) ValidateCredentials(ctx context.Context, username, password string) (*stores.User, error) {
	user, err := u.GetUser(ctx, username)
	if err != nil || user == nil || !user.Enabled {
		return nil, err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, nil
	}
	return user, nil
}

func (u *UserStore) CreateAclRule(ctx context.Context, r stores.AclRule) error {
	_, err := u.aclColl().InsertOne(ctx, bson.M{
		"username": r.Username, "topic_pattern": r.TopicPattern,
		"can_subscribe": r.CanSubscribe, "can_publish": r.CanPublish,
		"priority": r.Priority, "created_at": time.Now().UTC(),
	})
	return err
}
func (u *UserStore) UpdateAclRule(ctx context.Context, r stores.AclRule) error {
	id, err := bson.ObjectIDFromHex(r.ID)
	if err != nil {
		return err
	}
	_, err = u.aclColl().UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{
		"username": r.Username, "topic_pattern": r.TopicPattern,
		"can_subscribe": r.CanSubscribe, "can_publish": r.CanPublish, "priority": r.Priority,
	}})
	return err
}
func (u *UserStore) DeleteAclRule(ctx context.Context, id string) error {
	oid, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	_, err = u.aclColl().DeleteOne(ctx, bson.M{"_id": oid})
	return err
}
func (u *UserStore) GetUserAclRules(ctx context.Context, username string) ([]stores.AclRule, error) {
	return u.queryAcl(ctx, bson.M{"username": username})
}
func (u *UserStore) GetAllAclRules(ctx context.Context) ([]stores.AclRule, error) {
	return u.queryAcl(ctx, bson.M{})
}
func (u *UserStore) queryAcl(ctx context.Context, filter bson.M) ([]stores.AclRule, error) {
	cur, err := u.aclColl().Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "username", Value: 1}, {Key: "priority", Value: -1}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := []stores.AclRule{}
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		r := stores.AclRule{
			Username: getStr(doc, "username"), TopicPattern: getStr(doc, "topic_pattern"),
			CanSubscribe: getBool(doc, "can_subscribe"), CanPublish: getBool(doc, "can_publish"),
			Priority: getInt(doc, "priority"),
		}
		if oid, ok := doc["_id"].(bson.ObjectID); ok {
			r.ID = oid.Hex()
		}
		if t, ok := doc["created_at"].(bson.DateTime); ok {
			r.CreatedAt = t.Time()
		}
		out = append(out, r)
	}
	return out, cur.Err()
}
func (u *UserStore) LoadAll(ctx context.Context) ([]stores.User, []stores.AclRule, error) {
	users, err := u.GetAllUsers(ctx)
	if err != nil {
		return nil, nil, err
	}
	rules, err := u.GetAllAclRules(ctx)
	if err != nil {
		return nil, nil, err
	}
	return users, rules, nil
}

// ArchiveConfigStore --------------------------------------------------

type ArchiveConfigStore struct{ db *DB }

func (a *ArchiveConfigStore) Close() error                          { return nil }
func (a *ArchiveConfigStore) coll() *mongo.Collection               { return a.db.db.Collection("archiveconfigs") }
func (a *ArchiveConfigStore) EnsureTable(ctx context.Context) error {
	_, err := a.coll().Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true)})
	return err
}
func (a *ArchiveConfigStore) GetAll(ctx context.Context) ([]stores.ArchiveGroupConfig, error) {
	cur, err := a.coll().Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "name", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := []stores.ArchiveGroupConfig{}
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		out = append(out, *docToArchive(doc))
	}
	return out, cur.Err()
}
func (a *ArchiveConfigStore) Get(ctx context.Context, name string) (*stores.ArchiveGroupConfig, error) {
	var doc bson.M
	err := a.coll().FindOne(ctx, bson.M{"name": name}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return docToArchive(doc), nil
}
func docToArchive(doc bson.M) *stores.ArchiveGroupConfig {
	c := &stores.ArchiveGroupConfig{
		Name:    getStr(doc, "name"),
		Enabled: getBool(doc, "enabled"),
		RetainedOnly: getBool(doc, "retained_only"),
		LastValType: stores.MessageStoreType(getStr(doc, "last_val_type")),
		ArchiveType: stores.MessageArchiveType(getStr(doc, "archive_type")),
		LastValRetention: getStr(doc, "last_val_retention"),
		ArchiveRetention: getStr(doc, "archive_retention"),
		PurgeInterval:    getStr(doc, "purge_interval"),
		PayloadFormat:    stores.PayloadFormat(getStr(doc, "payload_format")),
		CreatedAt:        getTime(doc, "created_at"),
		UpdatedAt:        getTime(doc, "updated_at"),
	}
	if c.PayloadFormat == "" {
		c.PayloadFormat = stores.PayloadDefault
	}
	if filter := getStr(doc, "topic_filter"); filter != "" {
		c.TopicFilters = strings.Split(filter, ",")
	}
	return c
}
func (a *ArchiveConfigStore) Save(ctx context.Context, cfg stores.ArchiveGroupConfig) error {
	_, err := a.coll().UpdateOne(ctx, bson.M{"name": cfg.Name},
		bson.M{"$set": bson.M{
			"enabled": cfg.Enabled, "topic_filter": strings.Join(cfg.TopicFilters, ","),
			"retained_only": cfg.RetainedOnly,
			"last_val_type": string(cfg.LastValType), "archive_type": string(cfg.ArchiveType),
			"last_val_retention": cfg.LastValRetention, "archive_retention": cfg.ArchiveRetention,
			"purge_interval": cfg.PurgeInterval, "payload_format": string(cfg.PayloadFormat),
			"updated_at": time.Now().UTC(),
		}, "$setOnInsert": bson.M{"created_at": time.Now().UTC()}},
		options.UpdateOne().SetUpsert(true))
	return err
}
func (a *ArchiveConfigStore) Update(ctx context.Context, cfg stores.ArchiveGroupConfig) error { return a.Save(ctx, cfg) }
func (a *ArchiveConfigStore) Delete(ctx context.Context, name string) error {
	_, err := a.coll().DeleteOne(ctx, bson.M{"name": name})
	return err
}

// DeviceConfigStore ----------------------------------------------------

type DeviceConfigStore struct{ db *DB }

func (d *DeviceConfigStore) Close() error                          { return nil }
func (d *DeviceConfigStore) coll() *mongo.Collection               { return d.db.db.Collection("deviceconfigs") }
func (d *DeviceConfigStore) EnsureTable(ctx context.Context) error {
	_, err := d.coll().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "node_id", Value: 1}}},
		{Keys: bson.D{{Key: "namespace", Value: 1}}},
	})
	return err
}
func (d *DeviceConfigStore) GetAll(ctx context.Context) ([]stores.DeviceConfig, error) {
	return d.query(ctx, bson.M{})
}
func (d *DeviceConfigStore) GetByNode(ctx context.Context, nodeID string) ([]stores.DeviceConfig, error) {
	return d.query(ctx, bson.M{"node_id": nodeID})
}
func (d *DeviceConfigStore) GetEnabledByNode(ctx context.Context, nodeID string) ([]stores.DeviceConfig, error) {
	return d.query(ctx, bson.M{"node_id": nodeID, "enabled": true})
}
func (d *DeviceConfigStore) query(ctx context.Context, filter bson.M) ([]stores.DeviceConfig, error) {
	cur, err := d.coll().Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "name", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := []stores.DeviceConfig{}
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		out = append(out, *docToDevice(doc))
	}
	return out, cur.Err()
}
func docToDevice(doc bson.M) *stores.DeviceConfig {
	dc := &stores.DeviceConfig{
		Name:      getStr(doc, "name"),
		Namespace: getStr(doc, "namespace"),
		NodeID:    getStr(doc, "node_id"),
		Type:      getStr(doc, "type"),
		Config:    getStr(doc, "config"),
		Enabled:   getBool(doc, "enabled"),
	}
	if t, ok := doc["created_at"].(bson.DateTime); ok {
		dc.CreatedAt = t.Time()
	}
	if t, ok := doc["updated_at"].(bson.DateTime); ok {
		dc.UpdatedAt = t.Time()
	}
	return dc
}
func (d *DeviceConfigStore) Get(ctx context.Context, name string) (*stores.DeviceConfig, error) {
	var doc bson.M
	err := d.coll().FindOne(ctx, bson.M{"name": name}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return docToDevice(doc), nil
}
func (d *DeviceConfigStore) Save(ctx context.Context, dc stores.DeviceConfig) error {
	_, err := d.coll().UpdateOne(ctx, bson.M{"name": dc.Name},
		bson.M{"$set": bson.M{
			"namespace": dc.Namespace, "node_id": dc.NodeID, "config": dc.Config,
			"enabled": dc.Enabled, "type": dc.Type, "updated_at": time.Now().UTC(),
		}, "$setOnInsert": bson.M{"created_at": time.Now().UTC()}},
		options.UpdateOne().SetUpsert(true))
	return err
}
func (d *DeviceConfigStore) Delete(ctx context.Context, name string) error {
	_, err := d.coll().DeleteOne(ctx, bson.M{"name": name})
	return err
}
func (d *DeviceConfigStore) Toggle(ctx context.Context, name string, enabled bool) (*stores.DeviceConfig, error) {
	if _, err := d.coll().UpdateOne(ctx, bson.M{"name": name}, bson.M{"$set": bson.M{"enabled": enabled, "updated_at": time.Now().UTC()}}); err != nil {
		return nil, err
	}
	return d.Get(ctx, name)
}
func (d *DeviceConfigStore) Reassign(ctx context.Context, name, nodeID string) (*stores.DeviceConfig, error) {
	if _, err := d.coll().UpdateOne(ctx, bson.M{"name": name}, bson.M{"$set": bson.M{"node_id": nodeID, "updated_at": time.Now().UTC()}}); err != nil {
		return nil, err
	}
	return d.Get(ctx, name)
}

// MetricsStore ---------------------------------------------------------

type MetricsStore struct{ db *DB }

func (m *MetricsStore) Close() error                          { return nil }
func (m *MetricsStore) coll() *mongo.Collection               { return m.db.db.Collection("metrics") }
func (m *MetricsStore) EnsureTable(ctx context.Context) error {
	_, err := m.coll().Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "timestamp", Value: 1}, {Key: "metric_type", Value: 1}, {Key: "identifier", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "metric_type", Value: 1}, {Key: "identifier", Value: 1}, {Key: "timestamp", Value: 1}}},
	})
	return err
}
func (m *MetricsStore) StoreMetrics(ctx context.Context, kind stores.MetricKind, ts time.Time, identifier, payload string) error {
	_, err := m.coll().UpdateOne(ctx,
		bson.M{"timestamp": ts.UTC(), "metric_type": string(kind), "identifier": identifier},
		bson.M{"$set": bson.M{"metrics": payload}},
		options.UpdateOne().SetUpsert(true))
	return err
}
func (m *MetricsStore) GetLatest(ctx context.Context, kind stores.MetricKind, identifier string) (time.Time, string, error) {
	var doc bson.M
	err := m.coll().FindOne(ctx,
		bson.M{"metric_type": string(kind), "identifier": identifier},
		options.FindOne().SetSort(bson.D{{Key: "timestamp", Value: -1}})).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, "", nil
	}
	if err != nil {
		return time.Time{}, "", err
	}
	ts := time.Time{}
	if t, ok := doc["timestamp"].(bson.DateTime); ok {
		ts = t.Time()
	}
	return ts, getStr(doc, "metrics"), nil
}
func (m *MetricsStore) GetHistory(ctx context.Context, kind stores.MetricKind, identifier string, from, to time.Time, limit int) ([]stores.MetricsRow, error) {
	if limit <= 0 {
		limit = 1000
	}
	cur, err := m.coll().Find(ctx,
		bson.M{
			"metric_type": string(kind), "identifier": identifier,
			"timestamp": bson.M{"$gte": from.UTC(), "$lte": to.UTC()},
		},
		options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := []stores.MetricsRow{}
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		row := stores.MetricsRow{Payload: getStr(doc, "metrics")}
		if t, ok := doc["timestamp"].(bson.DateTime); ok {
			row.Timestamp = t.Time()
		}
		out = append(out, row)
	}
	return out, cur.Err()
}
func (m *MetricsStore) PurgeOlderThan(ctx context.Context, t time.Time) (int64, error) {
	res, err := m.coll().DeleteMany(ctx, bson.M{"timestamp": bson.M{"$lt": t.UTC()}})
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}

// helpers --------------------------------------------------------------

func matchTopic(pattern, topic string) bool {
	pp := strings.Split(pattern, "/")
	tt := strings.Split(topic, "/")
	for i, p := range pp {
		if p == "#" {
			return true
		}
		if i >= len(tt) {
			return false
		}
		if p == "+" {
			continue
		}
		if p != tt[i] {
			return false
		}
	}
	return len(pp) == len(tt)
}

var (
	_ stores.MessageStore       = (*MessageStore)(nil)
	_ stores.MessageArchive     = (*MessageArchive)(nil)
	_ stores.SessionStore       = (*SessionStore)(nil)
	_ stores.QueueStore         = (*QueueStore)(nil)
	_ stores.UserStore          = (*UserStore)(nil)
	_ stores.ArchiveConfigStore = (*ArchiveConfigStore)(nil)
	_ stores.DeviceConfigStore  = (*DeviceConfigStore)(nil)
	_ stores.MetricsStore       = (*MetricsStore)(nil)
)
