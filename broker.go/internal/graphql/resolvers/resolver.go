package resolvers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/auth"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/graphql/generated"
	"monstermq.io/edge/internal/metrics"
	"monstermq.io/edge/internal/pubsub"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/version"
)

// Resolver is the root resolver. Built once at startup with handles to every
// service the GraphQL surface exposes.
type Resolver struct {
	Cfg       *config.Config
	Storage   *stores.Storage
	Bus       *pubsub.Bus
	Archives  *archive.Manager
	AuthCache *auth.Cache
	Collector *metrics.Collector
	Logger    *slog.Logger
	NodeID    string
	Version   string

	// Publish injects a message into the local broker (used by the publish mutation).
	Publish func(topic string, payload []byte, retain bool, qos byte) error
}

func New(cfg *config.Config, storage *stores.Storage, bus *pubsub.Bus, archives *archive.Manager,
	authCache *auth.Cache, collector *metrics.Collector, logger *slog.Logger,
	publish func(string, []byte, bool, byte) error) *Resolver {
	return &Resolver{
		Cfg:       cfg,
		Storage:   storage,
		Bus:       bus,
		Archives:  archives,
		AuthCache: authCache,
		Collector: collector,
		Logger:    logger,
		NodeID:    cfg.NodeID,
		Version:   version.Version,
		Publish:   publish,
	}
}

// ResolverRoot wiring -------------------------------------------------------

func (r *Resolver) Mutation() generated.MutationResolver         { return &mutationResolver{r} }
func (r *Resolver) Query() generated.QueryResolver               { return &queryResolver{r} }
func (r *Resolver) Subscription() generated.SubscriptionResolver { return &subscriptionResolver{r} }
func (r *Resolver) Broker() generated.BrokerResolver             { return &brokerResolver{r} }
func (r *Resolver) Session() generated.SessionResolver           { return &sessionResolver{r} }
func (r *Resolver) UserInfo() generated.UserInfoResolver         { return &userInfoResolver{r} }
func (r *Resolver) ArchiveGroupInfo() generated.ArchiveGroupInfoResolver {
	return &archiveGroupInfoResolver{r}
}
func (r *Resolver) ArchiveGroupMutations() generated.ArchiveGroupMutationsResolver {
	return &archiveGroupMutationsResolver{r}
}
func (r *Resolver) UserManagementMutations() generated.UserManagementMutationsResolver {
	return &userManagementMutationsResolver{r}
}
func (r *Resolver) SessionMutations() generated.SessionMutationsResolver {
	return &sessionMutationsResolver{r}
}
func (r *Resolver) MqttClient() generated.MqttClientResolver { return &mqttClientResolver{r} }
func (r *Resolver) MqttClientMutations() generated.MqttClientMutationsResolver {
	return &mqttClientMutationsResolver{r}
}
func (r *Resolver) Topic() generated.TopicResolver { return &topicResolver{r} }

type mutationResolver struct{ *Resolver }
type queryResolver struct{ *Resolver }
type subscriptionResolver struct{ *Resolver }
type brokerResolver struct{ *Resolver }
type sessionResolver struct{ *Resolver }
type userInfoResolver struct{ *Resolver }
type archiveGroupInfoResolver struct{ *Resolver }
type archiveGroupMutationsResolver struct{ *Resolver }
type userManagementMutationsResolver struct{ *Resolver }
type sessionMutationsResolver struct{ *Resolver }
type mqttClientResolver struct{ *Resolver }
type mqttClientMutationsResolver struct{ *Resolver }
type topicResolver struct{ *Resolver }

func (r *topicResolver) Value(ctx context.Context, obj *generated.Topic, format *generated.DataFormat) (*generated.TopicValue, error) {
	q := &queryResolver{r.Resolver}
	return q.CurrentValue(ctx, obj.Name, format, nil)
}

// Helpers -------------------------------------------------------------------

func ptr[T any](v T) *T { return &v }

func nowISO() string                  { return time.Now().UTC().Format(time.RFC3339Nano) }
func formatTime(t time.Time) string   { return t.UTC().Format(time.RFC3339Nano) }
func boolPtr(p *bool, def bool) bool  { if p == nil { return def }; return *p }
func intPtr(p *int, def int) int      { if p == nil { return def }; return *p }
func ptrIfNotEmpty(s string) *string  { if s == "" { return nil }; return &s }
func derefStr(s *string) string       { if s == nil { return "" }; return *s }

// encodePayload returns the payload string and the format it was encoded in,
// matching the JVM broker's contract:
//
//	requested = JSON   → return (json text, JSON) if it parses as JSON,
//	                     otherwise fall through to BINARY
//	requested = BINARY → return (base64, BINARY)
//	requested = nil    → default to JSON behaviour (try JSON first, then base64)
func encodePayload(raw []byte, requested *generated.DataFormat) (string, generated.DataFormat) {
	if raw == nil {
		return "", generated.DataFormatJSON
	}
	wantBinary := requested != nil && *requested == generated.DataFormatBinary
	if !wantBinary && isJSON(raw) {
		return string(raw), generated.DataFormatJSON
	}
	return base64.StdEncoding.EncodeToString(raw), generated.DataFormatBinary
}

func isJSON(raw []byte) bool {
	var v any
	return json.Unmarshal(raw, &v) == nil
}

func decodePayload(in *generated.PublishInput) ([]byte, error) {
	if in.Payload != nil {
		return []byte(*in.Payload), nil
	}
	if in.PayloadBase64 != nil {
		return base64.StdEncoding.DecodeString(*in.PayloadBase64)
	}
	if in.PayloadJSON != nil {
		return json.Marshal(in.PayloadJSON)
	}
	return nil, nil
}

func toMessageStoreType(s stores.MessageStoreType) generated.MessageStoreType {
	switch s {
	case stores.MessageStoreSQLite:
		return generated.MessageStoreTypeSQLIte
	case stores.MessageStorePostgres:
		return generated.MessageStoreTypePostgres
	case stores.MessageStoreMongoDB:
		return generated.MessageStoreTypeMongodb
	case stores.MessageStoreMemory:
		return generated.MessageStoreTypeMemory
	}
	return generated.MessageStoreTypeNone
}

func toMessageArchiveType(s stores.MessageArchiveType) generated.MessageArchiveType {
	switch s {
	case stores.ArchiveSQLite:
		return generated.MessageArchiveTypeSQLIte
	case stores.ArchivePostgres:
		return generated.MessageArchiveTypePostgres
	case stores.ArchiveMongoDB:
		return generated.MessageArchiveTypeMongodb
	}
	return generated.MessageArchiveTypeNone
}

func fromMessageStoreType(t generated.MessageStoreType) stores.MessageStoreType {
	switch t {
	case generated.MessageStoreTypeSQLIte:
		return stores.MessageStoreSQLite
	case generated.MessageStoreTypePostgres:
		return stores.MessageStorePostgres
	case generated.MessageStoreTypeMongodb:
		return stores.MessageStoreMongoDB
	case generated.MessageStoreTypeMemory:
		return stores.MessageStoreMemory
	}
	return stores.MessageStoreNone
}

func fromMessageArchiveType(t generated.MessageArchiveType) stores.MessageArchiveType {
	switch t {
	case generated.MessageArchiveTypeSQLIte:
		return stores.ArchiveSQLite
	case generated.MessageArchiveTypePostgres:
		return stores.ArchivePostgres
	case generated.MessageArchiveTypeMongodb:
		return stores.ArchiveMongoDB
	}
	return stores.ArchiveNone
}

func parseTimeArg(s *string) (*time.Time, error) {
	if s == nil || *s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, *s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, *s)
		if err != nil {
			return nil, fmt.Errorf("invalid time %q: %w", *s, err)
		}
	}
	return &t, nil
}

// Mutations -----------------------------------------------------------------

func (r *mutationResolver) Login(ctx context.Context, username, password string) (*generated.LoginResult, error) {
	// When user management is disabled (or anonymous is permitted), the dashboard
	// still calls login(). Match the JVM broker's response exactly so the UI
	// behaves identically: success=true, no token, username="anonymous",
	// isAdmin=true, message="Authentication disabled".
	if !r.Cfg.UserManagement.Enabled || r.Cfg.UserManagement.AnonymousEnabled {
		name := "anonymous"
		admin := true
		return &generated.LoginResult{
			Success:  true,
			Message:  ptr("Authentication disabled"),
			Username: &name,
			IsAdmin:  &admin,
		}, nil
	}
	if r.Storage == nil {
		return &generated.LoginResult{Success: false, Message: ptr("auth unavailable")}, nil
	}
	user, err := r.Storage.Users.ValidateCredentials(ctx, username, password)
	if err != nil {
		return &generated.LoginResult{Success: false, Message: ptr(err.Error())}, nil
	}
	if user == nil {
		return &generated.LoginResult{Success: false, Message: ptr("invalid credentials")}, nil
	}
	tok := fmt.Sprintf("session-%s-%d", user.Username, time.Now().UnixNano())
	return &generated.LoginResult{
		Success: true, Token: &tok, Username: &user.Username, IsAdmin: &user.IsAdmin,
	}, nil
}

func (r *mutationResolver) Publish(ctx context.Context, input generated.PublishInput) (*generated.PublishResult, error) {
	payload, err := decodePayload(&input)
	if err != nil {
		return &generated.PublishResult{Success: false, Topic: input.Topic, Message: ptr(err.Error())}, nil
	}
	qos := byte(0)
	if input.Qos != nil {
		qos = byte(*input.Qos)
	}
	retain := false
	if input.Retain != nil {
		retain = *input.Retain
	}
	if r.Resolver.Publish == nil {
		return &generated.PublishResult{Success: false, Topic: input.Topic, Message: ptr("publish unavailable")}, nil
	}
	if err := r.Resolver.Publish(input.Topic, payload, retain, qos); err != nil {
		return &generated.PublishResult{Success: false, Topic: input.Topic, Message: ptr(err.Error())}, nil
	}
	return &generated.PublishResult{Success: true, Topic: input.Topic}, nil
}

func (r *mutationResolver) PublishBatch(ctx context.Context, inputs []*generated.PublishInput) ([]*generated.PublishResult, error) {
	out := make([]*generated.PublishResult, 0, len(inputs))
	for _, in := range inputs {
		res, _ := r.Publish(ctx, *in)
		out = append(out, res)
	}
	return out, nil
}

func (r *mutationResolver) PurgeQueuedMessages(ctx context.Context, clientID string) (*generated.PurgeResult, error) {
	n, err := r.Storage.Queue.PurgeForClient(ctx, clientID)
	if err != nil {
		return &generated.PurgeResult{Success: false, Message: ptr(err.Error()), PurgedCount: 0}, nil
	}
	return &generated.PurgeResult{Success: true, PurgedCount: n}, nil
}

func (r *mutationResolver) User(ctx context.Context) (*generated.UserManagementMutations, error) {
	return &generated.UserManagementMutations{}, nil
}
func (r *mutationResolver) Session(ctx context.Context) (*generated.SessionMutations, error) {
	return &generated.SessionMutations{}, nil
}
func (r *mutationResolver) ArchiveGroup(ctx context.Context) (*generated.ArchiveGroupMutations, error) {
	return &generated.ArchiveGroupMutations{}, nil
}
func (r *mutationResolver) MqttClient(ctx context.Context) (*generated.MqttClientMutations, error) {
	return &generated.MqttClientMutations{}, nil
}

// Queries -------------------------------------------------------------------

func (r *queryResolver) CurrentUser(ctx context.Context) (*generated.CurrentUser, error) {
	if !r.Cfg.UserManagement.Enabled {
		return &generated.CurrentUser{Username: "Anonymous", IsAdmin: true}, nil
	}
	return &generated.CurrentUser{Username: "Anonymous", IsAdmin: false}, nil
}

func (r *queryResolver) BrokerConfig(ctx context.Context) (*generated.BrokerConfig, error) {
	c := r.Cfg
	return &generated.BrokerConfig{
		NodeID: c.NodeID, Version: r.Version, Clustered: false,
		TCPPort: c.TCP.Port, WsPort: c.WS.Port, TcpsPort: c.TCPS.Port, WssPort: c.WSS.Port, NatsPort: 0,
		SessionStoreType: string(c.SessionStore()), RetainedStoreType: string(c.RetainedStore()), ConfigStoreType: string(c.ConfigStore()),
		UserManagementEnabled: c.UserManagement.Enabled, AnonymousEnabled: c.UserManagement.AnonymousEnabled,
		McpEnabled: false, McpPort: 0, PrometheusEnabled: false, PrometheusPort: 0,
		I3xEnabled: false, I3xPort: 0,
		GraphqlEnabled: c.GraphQL.Enabled, GraphqlPort: c.GraphQL.Port,
		MetricsEnabled: c.Metrics.Enabled,
		GenAiEnabled:   false, GenAiProvider: "", GenAiModel: "",
		PostgresURL: c.Postgres.URL, PostgresUser: c.Postgres.User,
		CrateDbURL: "", CrateDbUser: "",
		MongoDbURL: c.MongoDB.URL, MongoDbDatabase: c.MongoDB.Database,
		SqlitePath: c.SQLite.Path, KafkaServers: "",
	}, nil
}

func (r *queryResolver) Broker(ctx context.Context, nodeID *string) (*generated.Broker, error) {
	id := r.NodeID
	if nodeID != nil {
		id = *nodeID
	}
	if id != r.NodeID {
		return nil, nil
	}
	return r.brokerObj(), nil
}

func (r *queryResolver) Brokers(ctx context.Context) ([]*generated.Broker, error) {
	return []*generated.Broker{r.brokerObj()}, nil
}

func (r *Resolver) brokerObj() *generated.Broker {
	return &generated.Broker{
		NodeID: r.NodeID, Version: r.Version,
		UserManagementEnabled: r.Cfg.UserManagement.Enabled,
		AnonymousEnabled:      r.Cfg.UserManagement.AnonymousEnabled,
		IsLeader:              true, IsCurrent: true,
		EnabledFeatures: []string{"MqttClient"},
	}
}

func (r *queryResolver) Sessions(ctx context.Context, nodeID *string, cleanSession, connected *bool) ([]*generated.Session, error) {
	out := []*generated.Session{}
	err := r.Storage.Sessions.IterateSessions(ctx, func(info stores.SessionInfo) bool {
		if cleanSession != nil && info.CleanSession != *cleanSession {
			return true
		}
		if connected != nil && info.Connected != *connected {
			return true
		}
		out = append(out, sessionToGraphQL(info))
		return true
	})
	return out, err
}

func (r *queryResolver) Session(ctx context.Context, clientID string, nodeID *string) (*generated.Session, error) {
	info, err := r.Storage.Sessions.GetSession(ctx, clientID)
	if err != nil || info == nil {
		return nil, err
	}
	return sessionToGraphQL(*info), nil
}

func sessionToGraphQL(info stores.SessionInfo) *generated.Session {
	addr := info.ClientAddress
	infoStr := info.Information
	pv := info.ProtocolVersion
	rm := info.ReceiveMaximum
	mps := info.MaximumPacketSize
	tam := info.TopicAliasMaximum
	return &generated.Session{
		ClientID: info.ClientID, NodeID: info.NodeID,
		CleanSession:          info.CleanSession,
		Connected:             info.Connected,
		ClientAddress:         &addr,
		Information:           &infoStr,
		ProtocolVersion:       &pv,
		SessionExpiryInterval: info.SessionExpiryInterval,
		ReceiveMaximum:        &rm,
		MaximumPacketSize:     &mps,
		TopicAliasMaximum:     &tam,
	}
}

func (r *queryResolver) Users(ctx context.Context, username *string) ([]*generated.UserInfo, error) {
	users, err := r.Storage.Users.GetAllUsers(ctx)
	if err != nil {
		return nil, err
	}
	out := []*generated.UserInfo{}
	for _, u := range users {
		if username != nil && u.Username != *username {
			continue
		}
		out = append(out, userToGraphQL(u))
	}
	return out, nil
}

func userToGraphQL(u stores.User) *generated.UserInfo {
	created := formatTime(u.CreatedAt)
	updated := formatTime(u.UpdatedAt)
	return &generated.UserInfo{
		Username: u.Username, Enabled: u.Enabled,
		CanSubscribe: u.CanSubscribe, CanPublish: u.CanPublish, IsAdmin: u.IsAdmin,
		CreatedAt: &created, UpdatedAt: &updated,
	}
}

func (r *queryResolver) RetainedMessage(ctx context.Context, topic string, format *generated.DataFormat) (*generated.RetainedMessage, error) {
	msg, err := r.Storage.Retained.Get(ctx, topic)
	if err != nil || msg == nil {
		return nil, err
	}
	return brokerMsgToRetained(*msg, format), nil
}

func (r *queryResolver) RetainedMessages(ctx context.Context, topicFilter string, format *generated.DataFormat, limit *int) ([]*generated.RetainedMessage, error) {
	max := intPtr(limit, 1000)
	out := []*generated.RetainedMessage{}
	err := r.Storage.Retained.FindMatchingMessages(ctx, topicFilter, func(m stores.BrokerMessage) bool {
		out = append(out, brokerMsgToRetained(m, format))
		return len(out) < max
	})
	return out, err
}

func brokerMsgToRetained(m stores.BrokerMessage, fmt *generated.DataFormat) *generated.RetainedMessage {
	payload, fm := encodePayload(m.Payload, fmt)
	r := &generated.RetainedMessage{
		Topic:          m.TopicName,
		Payload:        payload,
		Format:         fm,
		Timestamp:      m.Time.UnixMilli(),
		Qos:            int(m.QoS),
		UserProperties: userPropsTo(m.UserProperties),
	}
	if m.MessageExpiryInterval != nil {
		v := int64(*m.MessageExpiryInterval)
		r.MessageExpiryInterval = &v
	}
	if m.ContentType != "" {
		ct := m.ContentType
		r.ContentType = &ct
	}
	if m.ResponseTopic != "" {
		rt := m.ResponseTopic
		r.ResponseTopic = &rt
	}
	if m.PayloadFormatIndicator != nil {
		v := *m.PayloadFormatIndicator != 0
		r.PayloadFormatIndicator = &v
	}
	return r
}

func userPropsTo(p map[string]string) []*generated.UserProperty {
	if len(p) == 0 {
		return nil
	}
	out := make([]*generated.UserProperty, 0, len(p))
	for k, v := range p {
		out = append(out, &generated.UserProperty{Key: k, Value: v})
	}
	return out
}

func (r *queryResolver) CurrentValue(ctx context.Context, topic string, format *generated.DataFormat, archiveGroup *string) (*generated.TopicValue, error) {
	store := r.lastValueStore(archiveGroup)
	if store == nil {
		return nil, nil
	}
	msg, err := store.Get(ctx, topic)
	if err != nil || msg == nil {
		return nil, err
	}
	return brokerMsgToTopicValue(*msg, format), nil
}

func (r *queryResolver) CurrentValues(ctx context.Context, topicFilter string, format *generated.DataFormat, limit *int, archiveGroup *string) ([]*generated.TopicValue, error) {
	store := r.lastValueStore(archiveGroup)
	if store == nil {
		return nil, nil
	}
	max := intPtr(limit, 1000)
	out := []*generated.TopicValue{}
	err := store.FindMatchingMessages(ctx, topicFilter, func(m stores.BrokerMessage) bool {
		out = append(out, brokerMsgToTopicValue(m, format))
		return len(out) < max
	})
	return out, err
}

func brokerMsgToTopicValue(m stores.BrokerMessage, fmt *generated.DataFormat) *generated.TopicValue {
	payload, fm := encodePayload(m.Payload, fmt)
	tv := &generated.TopicValue{
		Topic:          m.TopicName,
		Payload:        payload,
		Format:         fm,
		Timestamp:      m.Time.UnixMilli(),
		Qos:            int(m.QoS),
		UserProperties: userPropsTo(m.UserProperties),
	}
	if m.MessageExpiryInterval != nil {
		v := int64(*m.MessageExpiryInterval)
		tv.MessageExpiryInterval = &v
	}
	if m.ContentType != "" {
		ct := m.ContentType
		tv.ContentType = &ct
	}
	if m.ResponseTopic != "" {
		rt := m.ResponseTopic
		tv.ResponseTopic = &rt
	}
	if m.PayloadFormatIndicator != nil {
		v := *m.PayloadFormatIndicator != 0
		tv.PayloadFormatIndicator = &v
	}
	return tv
}

func (r *Resolver) lastValueStore(group *string) stores.MessageStore {
	name := "Default"
	if group != nil && *group != "" {
		name = *group
	}
	for _, g := range r.Archives.Snapshot() {
		if g.Name() == name {
			return g.LastValue()
		}
	}
	return nil
}

func (r *Resolver) archive(group *string) stores.MessageArchive {
	name := "Default"
	if group != nil && *group != "" {
		name = *group
	}
	for _, g := range r.Archives.Snapshot() {
		if g.Name() == name {
			return g.Archive()
		}
	}
	return nil
}

func (r *queryResolver) ArchivedMessages(ctx context.Context, topicFilter string, startTime, endTime *string, format *generated.DataFormat, limit *int, archiveGroup *string, includeTopic *bool) ([]*generated.ArchivedMessage, error) {
	arc := r.archive(archiveGroup)
	if arc == nil {
		return []*generated.ArchivedMessage{}, nil
	}
	from, _ := parseTimeArg(startTime)
	to, _ := parseTimeArg(endTime)
	rows, err := arc.GetHistory(ctx, topicFilter, from, to, intPtr(limit, 1000))
	if err != nil {
		return nil, err
	}
	out := make([]*generated.ArchivedMessage, 0, len(rows))
	for _, row := range rows {
		payload, fm := encodePayload(row.Payload, format)
		cid := row.ClientID
		out = append(out, &generated.ArchivedMessage{
			Topic:     row.Topic,
			Payload:   payload,
			Format:    fm,
			Timestamp: row.Timestamp.UnixMilli(),
			Qos:       int(row.QoS),
			ClientID:  &cid,
		})
	}
	return out, nil
}

func (r *queryResolver) AggregatedMessages(ctx context.Context, topics []string, interval int, startTime, endTime string, functions, fields []string, archiveGroup *string) (*generated.AggregatedResult, error) {
	return &generated.AggregatedResult{Columns: []string{"timestamp"}, Rows: [][]map[string]any{}}, nil
}

func (r *queryResolver) SearchTopics(ctx context.Context, pattern string, limit *int, archiveGroup *string) ([]string, error) {
	store := r.lastValueStore(archiveGroup)
	if store == nil {
		return []string{}, nil
	}
	max := intPtr(limit, 1000)
	out := []string{}
	err := store.FindMatchingTopics(ctx, "#", func(topic string) bool {
		if pattern == "" || strings.Contains(topic, pattern) {
			out = append(out, topic)
		}
		return len(out) < max
	})
	return out, err
}

// BrowseTopics returns the distinct topic prefixes truncated at the level of
// the trailing "+". Matches the JVM broker's contract so the dashboard's
// topic browser can walk the tree level-by-level.
//
// Examples:
//
//	pattern = "+"           with topics {"a", "a/b", "c/d"}     → {"a", "c"}
//	pattern = "sensor/+"    with topics {"sensor/temp",
//	                                     "sensor/temp/celsius",
//	                                     "sensor/humid"}        → {"sensor/temp", "sensor/humid"}
//	pattern = "sensor/temp" (no wildcards, exact match)         → {"sensor/temp"} if it exists
func (r *queryResolver) BrowseTopics(ctx context.Context, topic string, archiveGroup *string) ([]*generated.Topic, error) {
	store := r.lastValueStore(archiveGroup)
	if store == nil {
		return []*generated.Topic{}, nil
	}
	if topic == "" {
		topic = "+"
	}
	patternLevels := strings.Split(topic, "/")
	extractDepth := len(patternLevels)
	hasWildcard := strings.ContainsAny(topic, "+#")

	// Exact topic — return it iff it has a value.
	if !hasWildcard {
		msg, err := store.Get(ctx, topic)
		if err != nil || msg == nil {
			return []*generated.Topic{}, err
		}
		return []*generated.Topic{{Name: topic, IsLeaf: true}}, nil
	}

	seen := map[string]struct{}{}
	leaves := map[string]bool{}
	err := store.FindMatchingTopics(ctx, "#", func(t string) bool {
		topicLevels := strings.Split(t, "/")
		if len(topicLevels) < extractDepth {
			return true
		}
		// Each non-wildcard level in the pattern must match the topic.
		for i, lvl := range patternLevels {
			if lvl == "+" || lvl == "#" {
				continue
			}
			if lvl != topicLevels[i] {
				return true
			}
		}
		prefix := strings.Join(topicLevels[:extractDepth], "/")
		seen[prefix] = struct{}{}
		// "Leaf" means the topic itself terminates at this depth — i.e. there
		// is a stored value at exactly this prefix.
		if len(topicLevels) == extractDepth {
			leaves[prefix] = true
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	out := make([]*generated.Topic, 0, len(seen))
	for name := range seen {
		out = append(out, &generated.Topic{Name: name, IsLeaf: leaves[name]})
	}
	return out, nil
}

func (r *queryResolver) ArchiveGroups(ctx context.Context, enabled *bool, lastValTypeEquals, lastValTypeNotEquals *generated.MessageStoreType) ([]*generated.ArchiveGroupInfo, error) {
	configs, err := r.Storage.ArchiveConfig.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	out := []*generated.ArchiveGroupInfo{}
	for _, c := range configs {
		if enabled != nil && c.Enabled != *enabled {
			continue
		}
		lvType := toMessageStoreType(c.LastValType)
		if lastValTypeEquals != nil && lvType != *lastValTypeEquals {
			continue
		}
		if lastValTypeNotEquals != nil && lvType == *lastValTypeNotEquals {
			continue
		}
		out = append(out, r.archiveGroupInfoTo(c))
	}
	return out, nil
}

func (r *queryResolver) ArchiveGroup(ctx context.Context, name string) (*generated.ArchiveGroupInfo, error) {
	c, err := r.Storage.ArchiveConfig.Get(ctx, name)
	if err != nil || c == nil {
		return nil, err
	}
	return r.archiveGroupInfoTo(*c), nil
}

func (r *Resolver) archiveGroupInfoTo(c stores.ArchiveGroupConfig) *generated.ArchiveGroupInfo {
	deployed := false
	var deploymentID *string
	if r.Archives != nil {
		for _, g := range r.Archives.Snapshot() {
			if g.Name() == c.Name {
				deployed = true
				id := r.NodeID + ":" + c.Name
				deploymentID = &id
				break
			}
		}
	}
	return &generated.ArchiveGroupInfo{
		Name: c.Name, Enabled: c.Enabled,
		Deployed: deployed, DeploymentID: deploymentID,
		TopicFilter: c.TopicFilters, RetainedOnly: c.RetainedOnly,
		LastValType: toMessageStoreType(c.LastValType), ArchiveType: toMessageArchiveType(c.ArchiveType),
		PayloadFormat:    generated.PayloadFormat(c.PayloadFormat),
		LastValRetention: ptrIfNotEmpty(c.LastValRetention),
		ArchiveRetention: ptrIfNotEmpty(c.ArchiveRetention),
		PurgeInterval:    ptrIfNotEmpty(c.PurgeInterval),
		// createdAt/updatedAt aren't tracked in ArchiveGroupConfig today;
		// surface as nil so the dashboard renders "—".
	}
}

func (r *queryResolver) MqttClients(ctx context.Context, name, node *string) ([]*generated.MqttClient, error) {
	devices, err := r.Storage.DeviceConfig.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	out := []*generated.MqttClient{}
	for _, d := range devices {
		if d.Type != "" && d.Type != "MQTT_CLIENT" {
			continue
		}
		if name != nil && d.Name != *name {
			continue
		}
		if node != nil && d.NodeID != *node {
			continue
		}
		var cfgMap map[string]any
		_ = json.Unmarshal([]byte(d.Config), &cfgMap)
		out = append(out, &generated.MqttClient{
			Name: d.Name, Namespace: d.Namespace, NodeID: d.NodeID,
			Enabled: d.Enabled, Config: cfgMap,
		})
	}
	return out, nil
}

func (r *queryResolver) SystemLogs(ctx context.Context, startTime, endTime *string, lastMinutes *int, node, level, logger, sourceClass, sourceMethod, message *string, limit *int, orderByTime *generated.OrderDirection) ([]*generated.SystemLogEntry, error) {
	return []*generated.SystemLogEntry{}, nil
}

// Subscriptions -------------------------------------------------------------

func (r *subscriptionResolver) TopicUpdates(ctx context.Context, topicFilters []string, format *generated.DataFormat) (<-chan *generated.TopicUpdate, error) {
	id, msgCh := r.Bus.Subscribe(topicFilters, 64)
	out := make(chan *generated.TopicUpdate, 64)
	go func() {
		defer r.Bus.Unsubscribe(id)
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-msgCh:
				if !ok {
					return
				}
				out <- brokerMsgToTopicUpdate(m, format)
			}
		}
	}()
	return out, nil
}

func (r *subscriptionResolver) TopicUpdatesBulk(ctx context.Context, topicFilters []string, format *generated.DataFormat, timeoutMs, maxSize *int) (<-chan *generated.TopicUpdateBulk, error) {
	timeout := 250 * time.Millisecond
	if timeoutMs != nil && *timeoutMs > 0 {
		timeout = time.Duration(*timeoutMs) * time.Millisecond
	}
	max := 100
	if maxSize != nil && *maxSize > 0 {
		max = *maxSize
	}
	id, msgCh := r.Bus.Subscribe(topicFilters, 256)
	out := make(chan *generated.TopicUpdateBulk, 16)
	go func() {
		defer r.Bus.Unsubscribe(id)
		defer close(out)
		t := time.NewTimer(timeout)
		batch := make([]*generated.TopicUpdate, 0, max)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			out <- &generated.TopicUpdateBulk{
				Updates:   batch,
				Count:     len(batch),
				Timestamp: time.Now().UnixMilli(),
			}
			batch = make([]*generated.TopicUpdate, 0, max)
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				flush()
				t.Reset(timeout)
			case m, ok := <-msgCh:
				if !ok {
					flush()
					return
				}
				batch = append(batch, brokerMsgToTopicUpdate(m, format))
				if len(batch) >= max {
					flush()
					if !t.Stop() {
						<-t.C
					}
					t.Reset(timeout)
				}
			}
		}
	}()
	return out, nil
}

func brokerMsgToTopicUpdate(m stores.BrokerMessage, fmt *generated.DataFormat) *generated.TopicUpdate {
	payload, fm := encodePayload(m.Payload, fmt)
	cid := m.ClientID
	return &generated.TopicUpdate{
		Topic:     m.TopicName,
		Payload:   payload,
		Format:    fm,
		Timestamp: m.Time.UnixMilli(),
		Qos:       int(m.QoS),
		Retained:  m.IsRetain,
		ClientID:  &cid,
	}
}

func (r *subscriptionResolver) SystemLogs(ctx context.Context, node, level, logger, thread, sourceClass, sourceMethod, message *string) (<-chan *generated.SystemLogEntry, error) {
	out := make(chan *generated.SystemLogEntry)
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out, nil
}

// Sub-resolvers (Broker / Session / etc.) ----------------------------------

func (r *brokerResolver) Metrics(ctx context.Context, _ *generated.Broker) ([]*generated.BrokerMetrics, error) {
	if r.Collector == nil {
		return []*generated.BrokerMetrics{{Timestamp: nowISO()}}, nil
	}
	snap := r.Collector.Latest()
	return []*generated.BrokerMetrics{snapshotToBrokerMetrics(snap)}, nil
}

func (r *brokerResolver) MetricsHistory(ctx context.Context, _ *generated.Broker, from, to *string, lastMinutes *int) ([]*generated.BrokerMetrics, error) {
	now := time.Now()
	end := now
	start := now.Add(-24 * time.Hour)
	if lastMinutes != nil && *lastMinutes > 0 {
		start = now.Add(-time.Duration(*lastMinutes) * time.Minute)
	} else {
		if t, err := parseTimeArg(from); err == nil && t != nil {
			start = *t
		}
		if t, err := parseTimeArg(to); err == nil && t != nil {
			end = *t
		}
	}
	rows, err := r.Storage.Metrics.GetHistory(ctx, stores.MetricBroker, r.NodeID, start, end, 1000)
	if err != nil {
		return nil, err
	}
	out := make([]*generated.BrokerMetrics, 0, len(rows))
	for _, row := range rows {
		var snap metrics.BrokerSnapshot
		_ = json.Unmarshal([]byte(row.Payload), &snap)
		bm := snapshotToBrokerMetrics(snap)
		bm.Timestamp = formatTime(row.Timestamp)
		out = append(out, bm)
	}
	return out, nil
}

func snapshotToBrokerMetrics(s metrics.BrokerSnapshot) *generated.BrokerMetrics {
	return &generated.BrokerMetrics{
		MessagesIn: s.MessagesIn, MessagesOut: s.MessagesOut,
		MqttClientIn: s.MqttClientIn, MqttClientOut: s.MqttClientOut,
		NodeSessionCount:  s.NodeSessionCount,
		ClusterSessionCount: s.NodeSessionCount,
		QueuedMessagesCount: s.QueuedMessages,
		SubscriptionCount:   s.SubscriptionCount,
		Timestamp:           nowISO(),
	}
}

func (r *brokerResolver) Sessions(ctx context.Context, _ *generated.Broker, cleanSession, connected *bool) ([]*generated.Session, error) {
	q := &queryResolver{r.Resolver}
	return q.Sessions(ctx, nil, cleanSession, connected)
}

func (r *sessionResolver) Metrics(ctx context.Context, _ *generated.Session) ([]*generated.SessionMetrics, error) {
	return []*generated.SessionMetrics{{Timestamp: nowISO()}}, nil
}
func (r *sessionResolver) MetricsHistory(ctx context.Context, _ *generated.Session, from, to *string, lastMinutes *int) ([]*generated.SessionMetrics, error) {
	return []*generated.SessionMetrics{}, nil
}
func (r *sessionResolver) Subscriptions(ctx context.Context, obj *generated.Session) ([]*generated.MqttSubscription, error) {
	subs, err := r.Storage.Subscriptions.GetSubscriptionsForClient(ctx, obj.ClientID)
	if err != nil {
		return nil, err
	}
	out := make([]*generated.MqttSubscription, 0, len(subs))
	for _, s := range subs {
		nl := s.NoLocal
		rh := int(s.RetainHandling)
		rap := s.RetainAsPublished
		out = append(out, &generated.MqttSubscription{
			TopicFilter: s.TopicFilter, Qos: int(s.QoS),
			NoLocal: &nl, RetainHandling: &rh, RetainAsPublished: &rap,
		})
	}
	return out, nil
}
func (r *sessionResolver) QueuedMessageCount(ctx context.Context, obj *generated.Session) (int64, error) {
	return r.Storage.Queue.Count(ctx, obj.ClientID)
}

func (r *userInfoResolver) ACLRules(ctx context.Context, obj *generated.UserInfo) ([]*generated.ACLRuleInfo, error) {
	rules, err := r.Storage.Users.GetUserAclRules(ctx, obj.Username)
	if err != nil {
		return nil, err
	}
	out := make([]*generated.ACLRuleInfo, 0, len(rules))
	for _, ru := range rules {
		ts := formatTime(ru.CreatedAt)
		out = append(out, &generated.ACLRuleInfo{
			ID: ru.ID, Username: ru.Username, TopicPattern: ru.TopicPattern,
			CanSubscribe: ru.CanSubscribe, CanPublish: ru.CanPublish, Priority: ru.Priority, CreatedAt: &ts,
		})
	}
	return out, nil
}

func (r *archiveGroupInfoResolver) Metrics(ctx context.Context, _ *generated.ArchiveGroupInfo) ([]*generated.ArchiveGroupMetrics, error) {
	return []*generated.ArchiveGroupMetrics{{Timestamp: nowISO()}}, nil
}

func (r *archiveGroupInfoResolver) MetricsHistory(ctx context.Context, _ *generated.ArchiveGroupInfo, from, to *string, lastMinutes *int) ([]*generated.ArchiveGroupMetrics, error) {
	return []*generated.ArchiveGroupMetrics{}, nil
}

func (r *archiveGroupInfoResolver) ConnectionStatus(ctx context.Context, obj *generated.ArchiveGroupInfo) ([]*generated.NodeConnectionStatus, error) {
	// Single-node broker: report this node's status. Each underlying store
	// (last-value and archive) is reported independently. If the manager
	// failed to start the group (e.g. wrong backend type for the broker),
	// surface the error so the dashboard can show it.
	var (
		hasLV bool
		hasAR bool
	)
	for _, g := range r.Archives.Snapshot() {
		if g.Name() != obj.Name {
			continue
		}
		hasLV = g.LastValue() != nil
		hasAR = g.Archive() != nil
		break
	}
	status := &generated.NodeConnectionStatus{
		NodeID:    r.NodeID,
		Timestamp: time.Now().UnixMilli(),
	}
	if obj.LastValType != generated.MessageStoreTypeNone {
		status.LastValueStore = &hasLV
	}
	if obj.ArchiveType != generated.MessageArchiveTypeNone {
		status.MessageArchive = &hasAR
	}
	if msg := r.Archives.DeployError(obj.Name); msg != "" {
		status.Error = &msg
	}
	return []*generated.NodeConnectionStatus{status}, nil
}

func (r *mqttClientResolver) Metrics(ctx context.Context, _ *generated.MqttClient) ([]*generated.MqttClientMetrics, error) {
	return []*generated.MqttClientMetrics{{Timestamp: nowISO()}}, nil
}
func (r *mqttClientResolver) MetricsHistory(ctx context.Context, _ *generated.MqttClient, from, to *string, lastMinutes *int) ([]*generated.MqttClientMetrics, error) {
	return []*generated.MqttClientMetrics{}, nil
}

// Grouped mutation resolvers -----------------------------------------------

func (r *archiveGroupMutationsResolver) Create(ctx context.Context, _ *generated.ArchiveGroupMutations, input generated.CreateArchiveGroupInput) (*generated.ArchiveGroupResult, error) {
	if err := archive.ValidateGroupName(input.Name); err != nil {
		return &generated.ArchiveGroupResult{Success: false, Message: ptr(err.Error())}, nil
	}
	cfg := stores.ArchiveGroupConfig{
		Name:             input.Name,
		Enabled:          true,
		TopicFilters:     input.TopicFilter,
		RetainedOnly:     boolPtr(input.RetainedOnly, false),
		LastValType:      fromMessageStoreType(input.LastValType),
		ArchiveType:      fromMessageArchiveType(input.ArchiveType),
		PayloadFormat:    stores.PayloadDefault,
		LastValRetention: derefStr(input.LastValRetention),
		ArchiveRetention: derefStr(input.ArchiveRetention),
		PurgeInterval:    derefStr(input.PurgeInterval),
	}
	if input.PayloadFormat != nil {
		cfg.PayloadFormat = stores.PayloadFormat(*input.PayloadFormat)
	}
	if err := r.Storage.ArchiveConfig.Save(ctx, cfg); err != nil {
		return &generated.ArchiveGroupResult{Success: false, Message: ptr(err.Error())}, nil
	}
	if err := r.Archives.Reload(ctx); err != nil {
		r.Logger.Warn("archive reload after create failed", "name", cfg.Name, "err", err)
	}
	return &generated.ArchiveGroupResult{Success: true, ArchiveGroup: r.archiveGroupInfoTo(cfg)}, nil
}

func (r *archiveGroupMutationsResolver) Update(ctx context.Context, _ *generated.ArchiveGroupMutations, input generated.UpdateArchiveGroupInput) (*generated.ArchiveGroupResult, error) {
	// Update merges the input over the existing config — only fields the
	// caller supplied are changed.
	existing, err := r.Storage.ArchiveConfig.Get(ctx, input.Name)
	if err != nil {
		return &generated.ArchiveGroupResult{Success: false, Message: ptr(err.Error())}, nil
	}
	if existing == nil {
		return &generated.ArchiveGroupResult{Success: false, Message: ptr("archive group not found")}, nil
	}
	if input.TopicFilter != nil {
		existing.TopicFilters = input.TopicFilter
	}
	if input.RetainedOnly != nil {
		existing.RetainedOnly = *input.RetainedOnly
	}
	if input.LastValType != nil {
		existing.LastValType = fromMessageStoreType(*input.LastValType)
	}
	if input.ArchiveType != nil {
		existing.ArchiveType = fromMessageArchiveType(*input.ArchiveType)
	}
	if input.PayloadFormat != nil {
		existing.PayloadFormat = stores.PayloadFormat(*input.PayloadFormat)
	}
	if input.LastValRetention != nil {
		existing.LastValRetention = *input.LastValRetention
	}
	if input.ArchiveRetention != nil {
		existing.ArchiveRetention = *input.ArchiveRetention
	}
	if input.PurgeInterval != nil {
		existing.PurgeInterval = *input.PurgeInterval
	}
	if err := r.Storage.ArchiveConfig.Save(ctx, *existing); err != nil {
		return &generated.ArchiveGroupResult{Success: false, Message: ptr(err.Error())}, nil
	}
	if err := r.Archives.Reload(ctx); err != nil {
		r.Logger.Warn("archive reload after update failed", "name", existing.Name, "err", err)
	}
	return &generated.ArchiveGroupResult{Success: true, ArchiveGroup: r.archiveGroupInfoTo(*existing)}, nil
}
func (r *archiveGroupMutationsResolver) Delete(ctx context.Context, _ *generated.ArchiveGroupMutations, name string) (*generated.ArchiveGroupResult, error) {
	if err := r.Storage.ArchiveConfig.Delete(ctx, name); err != nil {
		return &generated.ArchiveGroupResult{Success: false, Message: ptr(err.Error())}, nil
	}
	if err := r.Archives.Reload(ctx); err != nil {
		r.Logger.Warn("archive reload after delete failed", "name", name, "err", err)
	}
	return &generated.ArchiveGroupResult{Success: true}, nil
}
func (r *archiveGroupMutationsResolver) Enable(ctx context.Context, _ *generated.ArchiveGroupMutations, name string) (*generated.ArchiveGroupResult, error) {
	return r.toggleArchive(ctx, name, true)
}
func (r *archiveGroupMutationsResolver) Disable(ctx context.Context, _ *generated.ArchiveGroupMutations, name string) (*generated.ArchiveGroupResult, error) {
	return r.toggleArchive(ctx, name, false)
}
func (r *archiveGroupMutationsResolver) toggleArchive(ctx context.Context, name string, enabled bool) (*generated.ArchiveGroupResult, error) {
	cfg, err := r.Storage.ArchiveConfig.Get(ctx, name)
	if err != nil || cfg == nil {
		return &generated.ArchiveGroupResult{Success: false, Message: ptr("not found")}, nil
	}
	cfg.Enabled = enabled
	if err := r.Storage.ArchiveConfig.Save(ctx, *cfg); err != nil {
		return &generated.ArchiveGroupResult{Success: false, Message: ptr(err.Error())}, nil
	}
	if err := r.Archives.Reload(ctx); err != nil {
		r.Logger.Warn("archive reload after toggle failed", "name", name, "err", err)
	}
	return &generated.ArchiveGroupResult{Success: true, ArchiveGroup: r.archiveGroupInfoTo(*cfg)}, nil
}

func (r *userManagementMutationsResolver) CreateUser(ctx context.Context, _ *generated.UserManagementMutations, input generated.CreateUserInput) (*generated.UserManagementResult, error) {
	hash, err := hashPassword(input.Password)
	if err != nil {
		return &generated.UserManagementResult{Success: false, Message: ptr(err.Error())}, nil
	}
	u := stores.User{
		Username: input.Username, PasswordHash: hash,
		Enabled: boolPtr(input.Enabled, true), CanSubscribe: boolPtr(input.CanSubscribe, true),
		CanPublish: boolPtr(input.CanPublish, true), IsAdmin: boolPtr(input.IsAdmin, false),
	}
	if err := r.Storage.Users.CreateUser(ctx, u); err != nil {
		return &generated.UserManagementResult{Success: false, Message: ptr(err.Error())}, nil
	}
	_ = r.AuthCache.Refresh(ctx)
	return &generated.UserManagementResult{Success: true, User: userToGraphQL(u)}, nil
}
func (r *userManagementMutationsResolver) UpdateUser(ctx context.Context, _ *generated.UserManagementMutations, input generated.UpdateUserInput) (*generated.UserManagementResult, error) {
	existing, err := r.Storage.Users.GetUser(ctx, input.Username)
	if err != nil || existing == nil {
		return &generated.UserManagementResult{Success: false, Message: ptr("not found")}, nil
	}
	if input.Enabled != nil {
		existing.Enabled = *input.Enabled
	}
	if input.CanSubscribe != nil {
		existing.CanSubscribe = *input.CanSubscribe
	}
	if input.CanPublish != nil {
		existing.CanPublish = *input.CanPublish
	}
	if input.IsAdmin != nil {
		existing.IsAdmin = *input.IsAdmin
	}
	if err := r.Storage.Users.UpdateUser(ctx, *existing); err != nil {
		return &generated.UserManagementResult{Success: false, Message: ptr(err.Error())}, nil
	}
	_ = r.AuthCache.Refresh(ctx)
	return &generated.UserManagementResult{Success: true, User: userToGraphQL(*existing)}, nil
}
func (r *userManagementMutationsResolver) DeleteUser(ctx context.Context, _ *generated.UserManagementMutations, username string) (*generated.UserManagementResult, error) {
	if err := r.Storage.Users.DeleteUser(ctx, username); err != nil {
		return &generated.UserManagementResult{Success: false, Message: ptr(err.Error())}, nil
	}
	_ = r.AuthCache.Refresh(ctx)
	return &generated.UserManagementResult{Success: true}, nil
}
func (r *userManagementMutationsResolver) SetPassword(ctx context.Context, _ *generated.UserManagementMutations, input generated.SetPasswordInput) (*generated.UserManagementResult, error) {
	existing, err := r.Storage.Users.GetUser(ctx, input.Username)
	if err != nil || existing == nil {
		return &generated.UserManagementResult{Success: false, Message: ptr("not found")}, nil
	}
	hash, err := hashPassword(input.Password)
	if err != nil {
		return &generated.UserManagementResult{Success: false, Message: ptr(err.Error())}, nil
	}
	existing.PasswordHash = hash
	if err := r.Storage.Users.UpdateUser(ctx, *existing); err != nil {
		return &generated.UserManagementResult{Success: false, Message: ptr(err.Error())}, nil
	}
	return &generated.UserManagementResult{Success: true, User: userToGraphQL(*existing)}, nil
}
func (r *userManagementMutationsResolver) CreateACLRule(ctx context.Context, _ *generated.UserManagementMutations, input generated.CreateACLRuleInput) (*generated.UserManagementResult, error) {
	rule := stores.AclRule{
		Username: input.Username, TopicPattern: input.TopicPattern,
		CanSubscribe: boolPtr(input.CanSubscribe, false), CanPublish: boolPtr(input.CanPublish, false),
		Priority: intPtr(input.Priority, 0),
	}
	if err := r.Storage.Users.CreateAclRule(ctx, rule); err != nil {
		return &generated.UserManagementResult{Success: false, Message: ptr(err.Error())}, nil
	}
	_ = r.AuthCache.Refresh(ctx)
	return &generated.UserManagementResult{Success: true}, nil
}
func (r *userManagementMutationsResolver) UpdateACLRule(ctx context.Context, _ *generated.UserManagementMutations, input generated.UpdateACLRuleInput) (*generated.UserManagementResult, error) {
	rule := stores.AclRule{
		ID: input.ID, Username: input.Username, TopicPattern: input.TopicPattern,
		CanSubscribe: boolPtr(input.CanSubscribe, false), CanPublish: boolPtr(input.CanPublish, false),
		Priority: intPtr(input.Priority, 0),
	}
	if err := r.Storage.Users.UpdateAclRule(ctx, rule); err != nil {
		return &generated.UserManagementResult{Success: false, Message: ptr(err.Error())}, nil
	}
	_ = r.AuthCache.Refresh(ctx)
	return &generated.UserManagementResult{Success: true}, nil
}
func (r *userManagementMutationsResolver) DeleteACLRule(ctx context.Context, _ *generated.UserManagementMutations, id string) (*generated.UserManagementResult, error) {
	if err := r.Storage.Users.DeleteAclRule(ctx, id); err != nil {
		return &generated.UserManagementResult{Success: false, Message: ptr(err.Error())}, nil
	}
	_ = r.AuthCache.Refresh(ctx)
	return &generated.UserManagementResult{Success: true}, nil
}

func (r *sessionMutationsResolver) RemoveSessions(ctx context.Context, _ *generated.SessionMutations, clientIds []string) (*generated.SessionRemovalResult, error) {
	count := 0
	for _, id := range clientIds {
		if err := r.Storage.Sessions.DelClient(ctx, id); err == nil {
			count++
		}
	}
	return &generated.SessionRemovalResult{Success: true, RemovedCount: count}, nil
}

func (r *mqttClientMutationsResolver) Create(ctx context.Context, _ *generated.MqttClientMutations, input generated.MqttClientInput) (*generated.MqttClientResult, error) {
	cfgBytes, _ := json.Marshal(input.Config)
	d := stores.DeviceConfig{
		Name: input.Name, Namespace: input.Namespace, NodeID: input.NodeID,
		Type: "MQTT_CLIENT", Enabled: boolPtr(input.Enabled, true), Config: string(cfgBytes),
	}
	if err := r.Storage.DeviceConfig.Save(ctx, d); err != nil {
		return &generated.MqttClientResult{Success: false, Message: ptr(err.Error())}, nil
	}
	return &generated.MqttClientResult{Success: true, Client: deviceToMqttClient(d, input.Config)}, nil
}
func (r *mqttClientMutationsResolver) Update(ctx context.Context, obj *generated.MqttClientMutations, input generated.MqttClientInput) (*generated.MqttClientResult, error) {
	return r.Create(ctx, obj, input)
}
func (r *mqttClientMutationsResolver) Delete(ctx context.Context, _ *generated.MqttClientMutations, name string) (*generated.MqttClientResult, error) {
	if err := r.Storage.DeviceConfig.Delete(ctx, name); err != nil {
		return &generated.MqttClientResult{Success: false, Message: ptr(err.Error())}, nil
	}
	return &generated.MqttClientResult{Success: true}, nil
}
func (r *mqttClientMutationsResolver) Start(ctx context.Context, _ *generated.MqttClientMutations, name string) (*generated.MqttClientResult, error) {
	return r.toggleDevice(ctx, name, true)
}
func (r *mqttClientMutationsResolver) Stop(ctx context.Context, _ *generated.MqttClientMutations, name string) (*generated.MqttClientResult, error) {
	return r.toggleDevice(ctx, name, false)
}
func (r *mqttClientMutationsResolver) Toggle(ctx context.Context, _ *generated.MqttClientMutations, name string) (*generated.MqttClientResult, error) {
	d, err := r.Storage.DeviceConfig.Get(ctx, name)
	if err != nil || d == nil {
		return &generated.MqttClientResult{Success: false, Message: ptr("not found")}, nil
	}
	return r.toggleDevice(ctx, name, !d.Enabled)
}
func (r *mqttClientMutationsResolver) Reassign(ctx context.Context, _ *generated.MqttClientMutations, name, nodeID string) (*generated.MqttClientResult, error) {
	d, err := r.Storage.DeviceConfig.Reassign(ctx, name, nodeID)
	if err != nil || d == nil {
		return &generated.MqttClientResult{Success: false, Message: ptr("not found")}, nil
	}
	var cfg map[string]any
	_ = json.Unmarshal([]byte(d.Config), &cfg)
	return &generated.MqttClientResult{Success: true, Client: deviceToMqttClient(*d, cfg)}, nil
}
func (r *mqttClientMutationsResolver) toggleDevice(ctx context.Context, name string, enabled bool) (*generated.MqttClientResult, error) {
	d, err := r.Storage.DeviceConfig.Toggle(ctx, name, enabled)
	if err != nil || d == nil {
		return &generated.MqttClientResult{Success: false, Message: ptr("not found")}, nil
	}
	var cfg map[string]any
	_ = json.Unmarshal([]byte(d.Config), &cfg)
	return &generated.MqttClientResult{Success: true, Client: deviceToMqttClient(*d, cfg)}, nil
}

func deviceToMqttClient(d stores.DeviceConfig, cfg map[string]any) *generated.MqttClient {
	return &generated.MqttClient{
		Name: d.Name, Namespace: d.Namespace, NodeID: d.NodeID,
		Enabled: d.Enabled, Config: cfg,
	}
}
