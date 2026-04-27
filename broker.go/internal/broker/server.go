package broker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	mauth "monstermq.io/edge/internal/auth"
	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/bridge/mqttclient"
	"monstermq.io/edge/internal/config"
	gql "monstermq.io/edge/internal/graphql"
	"monstermq.io/edge/internal/graphql/resolvers"
	mlog "monstermq.io/edge/internal/log"
	"monstermq.io/edge/internal/metrics"
	"monstermq.io/edge/internal/pubsub"
	"monstermq.io/edge/internal/stores"
	storemongo "monstermq.io/edge/internal/stores/mongodb"
	storepg "monstermq.io/edge/internal/stores/postgres"
	storesqlite "monstermq.io/edge/internal/stores/sqlite"
)

// Server is the top-level lifecycle holder for the edge broker.
type Server struct {
	cfg         *config.Config
	logger      *slog.Logger
	mochi       *mqtt.Server
	storage     *stores.Storage
	bus         *pubsub.Bus
	archives    *archive.Manager
	authCache   *mauth.Cache
	collector   *metrics.Collector
	bridges     *mqttclient.Manager
	gqlSrv      *gql.Server
	metricsCtx  context.Context
	metricsStop context.CancelFunc
}

func New(cfg *config.Config, logger *slog.Logger, logBus *mlog.Bus) (*Server, error) {
	ctx := context.Background()

	// 1. Storage — picks the backend based on DefaultStoreType.
	// SQLITE additionally exposes a *DB handle so the archive manager can
	// create per-group last-value/archive tables on the same connection.
	var (
		storage  *stores.Storage
		sqliteDB *storesqlite.DB
		pgDB     *storepg.DB
		mongoDB  *storemongo.DB
		err      error
	)
	switch cfg.DefaultStoreType {
	case config.StoreSQLite, "":
		storage, sqliteDB, err = storesqlite.Build(ctx, cfg)
	case config.StorePostgres:
		storage, pgDB, err = storepg.Build(ctx, cfg)
	case config.StoreMongoDB:
		storage, mongoDB, err = storemongo.Build(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported DefaultStoreType %q", cfg.DefaultStoreType)
	}
	if err != nil {
		return nil, fmt.Errorf("storage init: %w", err)
	}

	// 2. Auth cache
	authCache := mauth.NewCache(storage.Users, cfg.UserManagement.AnonymousEnabled || !cfg.UserManagement.Enabled)
	if err := authCache.Refresh(ctx); err != nil {
		logger.Warn("user cache refresh failed", "err", err)
	}
	authCache.StartRefresher(context.Background(), 30*time.Second)

	// 3. Pub/sub bus + archive manager
	bus := pubsub.NewBus()
	archives := archive.NewManager(cfg, storage, sqliteDB, pgDB, mongoDB, logger)
	if err := archives.Load(ctx); err != nil {
		logger.Warn("archive groups load failed", "err", err)
	}

	// 4. Mochi broker
	server := mqtt.New(&mqtt.Options{InlineClient: true, Logger: logger})

	if cfg.UserManagement.Enabled {
		if err := server.AddHook(NewAuthHook(authCache), nil); err != nil {
			return nil, fmt.Errorf("add monstermq auth hook: %w", err)
		}
	} else {
		if err := server.AddHook(new(auth.AllowHook), nil); err != nil {
			return nil, fmt.Errorf("add allow-all hook: %w", err)
		}
	}

	// Metrics collector (counts hooked into the storage hook)
	interval := time.Duration(cfg.Metrics.CollectionIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Second
	}
	var collector *metrics.Collector
	if cfg.Metrics.Enabled {
		collector = metrics.New(storage.Metrics, cfg.NodeID, interval, logger)
	}

	var counter MetricsCounter // nil interface, not interface-holding-nil-pointer
	if collector != nil {
		counter = collector
	}
	storageHook := NewStorageHook(storage, bus, archives, cfg.NodeID, logger, counter)
	if err := server.AddHook(storageHook, nil); err != nil {
		return nil, fmt.Errorf("add storage hook: %w", err)
	}

	if cfg.QueuedMessagesEnabled {
		if err := server.AddHook(NewQueueHook(storage, server, logger), nil); err != nil {
			return nil, fmt.Errorf("add queue hook: %w", err)
		}
	}

	// 5. Restore retained messages from SQLite into mochi's in-memory retained map.
	if err := restoreRetained(ctx, server, storage); err != nil {
		logger.Warn("retained restore failed", "err", err)
	}

	// 6. Listeners
	if cfg.TCP.Enabled {
		l := listeners.NewTCP(listeners.Config{ID: "tcp", Address: fmt.Sprintf(":%d", cfg.TCP.Port)})
		if err := server.AddListener(l); err != nil {
			return nil, fmt.Errorf("add tcp listener: %w", err)
		}
		logger.Info("mqtt listener", "type", "tcp", "port", cfg.TCP.Port)
	}
	if cfg.WS.Enabled {
		l := listeners.NewWebsocket(listeners.Config{ID: "ws", Address: fmt.Sprintf(":%d", cfg.WS.Port)})
		if err := server.AddListener(l); err != nil {
			return nil, fmt.Errorf("add ws listener: %w", err)
		}
		logger.Info("mqtt listener", "type", "ws", "port", cfg.WS.Port)
	}
	if cfg.TCPS.Enabled {
		tlsCfg, err := loadTLS(cfg.TCPS.KeyStorePath, cfg.TCPS.KeyStorePassword)
		if err != nil {
			return nil, fmt.Errorf("tls config: %w", err)
		}
		l := listeners.NewTCP(listeners.Config{ID: "tcps", Address: fmt.Sprintf(":%d", cfg.TCPS.Port), TLSConfig: tlsCfg})
		if err := server.AddListener(l); err != nil {
			return nil, fmt.Errorf("add tcps listener: %w", err)
		}
		logger.Info("mqtt listener", "type", "tcps", "port", cfg.TCPS.Port)
	}
	if cfg.WSS.Enabled {
		tlsCfg, err := loadTLS(cfg.WSS.KeyStorePath, cfg.WSS.KeyStorePassword)
		if err != nil {
			return nil, fmt.Errorf("wss tls config: %w", err)
		}
		l := listeners.NewWebsocket(listeners.Config{ID: "wss", Address: fmt.Sprintf(":%d", cfg.WSS.Port), TLSConfig: tlsCfg})
		if err := server.AddListener(l); err != nil {
			return nil, fmt.Errorf("add wss listener: %w", err)
		}
		logger.Info("mqtt listener", "type", "wss", "port", cfg.WSS.Port)
	}

	// 7. MQTT bridge manager
	publishFn := func(topic string, payload []byte, retain bool, qos byte) error {
		return server.Publish(topic, payload, retain, qos)
	}
	var bridges *mqttclient.Manager
	if cfg.Bridges.Mqtt.Enabled {
		bridges = mqttclient.NewManager(storage.DeviceConfig, publishFn, &mqttclient.BusAdapter{Bus: bus}, cfg.NodeID, logger)
	}

	// 8. GraphQL server (HTTP + WebSocket)
	var gqlSrv *gql.Server
	if cfg.GraphQL.Enabled {
		resolver := resolvers.New(cfg, storage, bus, archives, bridges, authCache, collector, logBus, logger, publishFn)
		gqlSrv = gql.NewServer(cfg, resolver, logger)
		if cfg.Dashboard.Enabled {
			gqlSrv.AttachDashboard(gql.DashboardHandler(cfg.Dashboard.Path))
		}
	}

	return &Server{
		cfg: cfg, logger: logger, mochi: server,
		storage: storage, bus: bus, archives: archives, authCache: authCache,
		collector: collector, bridges: bridges, gqlSrv: gqlSrv,
	}, nil
}

func restoreRetained(ctx context.Context, server *mqtt.Server, storage *stores.Storage) error {
	// Collect topics first so the read iterator is fully drained before we issue
	// per-topic Get calls (which acquire their own connections).
	topics := []string{}
	if err := storage.Retained.FindMatchingTopics(ctx, "#", func(topic string) bool {
		topics = append(topics, topic)
		return true
	}); err != nil {
		return err
	}
	for _, topic := range topics {
		msg, err := storage.Retained.Get(ctx, topic)
		if err != nil || msg == nil {
			continue
		}
		_ = server.Publish(topic, msg.Payload, true, msg.QoS)
	}
	return nil
}

func (s *Server) Serve() error {
	if s.collector != nil {
		s.metricsCtx, s.metricsStop = context.WithCancel(context.Background())
		s.collector.Start(s.metricsCtx, func() (sessions, subs int, queued int64) {
			ctx := context.Background()
			_ = s.storage.Sessions.IterateSessions(ctx, func(stores.SessionInfo) bool { sessions++; return true })
			_ = s.storage.Subscriptions.IterateSubscriptions(ctx, func(stores.MqttSubscription) bool { subs++; return true })
			queued, _ = s.storage.Queue.CountAll(ctx)
			return
		})
	}
	if s.archives != nil {
		s.archives.RunRetention(context.Background())
	}
	if s.bridges != nil {
		if err := s.bridges.Start(context.Background()); err != nil {
			s.logger.Warn("bridges start error", "err", err)
		}
	}
	if s.gqlSrv != nil {
		go func() {
			if err := s.gqlSrv.Start(); err != nil {
				s.logger.Error("graphql server error", "err", err)
			}
		}()
	}
	return s.mochi.Serve()
}

func (s *Server) Close() error {
	if s.bridges != nil {
		s.bridges.Stop()
	}
	if s.metricsStop != nil {
		s.metricsStop()
	}
	if s.collector != nil {
		s.collector.Stop()
	}
	if s.gqlSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.gqlSrv.Stop(ctx)
	}
	if s.archives != nil {
		s.archives.Stop()
	}
	if err := s.mochi.Close(); err != nil {
		return err
	}
	if s.storage != nil {
		return s.storage.Close()
	}
	return nil
}

// Storage exposes the store stack for GraphQL resolvers (M6+).
func (s *Server) Storage() *stores.Storage   { return s.storage }
func (s *Server) Bus() *pubsub.Bus           { return s.bus }
func (s *Server) Archives() *archive.Manager { return s.archives }
func (s *Server) Mochi() *mqtt.Server        { return s.mochi }
