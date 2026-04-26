package graphql

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/gorilla/websocket"
	"github.com/vektah/gqlparser/v2/ast"

	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/graphql/generated"
	"monstermq.io/edge/internal/graphql/resolvers"
)

// Server hosts the GraphQL HTTP and WebSocket endpoints. The dashboard is
// optionally served at the root.
type Server struct {
	cfg     *config.Config
	logger  *slog.Logger
	router  *chi.Mux
	httpSrv *http.Server
}

func NewServer(cfg *config.Config, resolver *resolvers.Resolver, logger *slog.Logger) *Server {
	es := generated.NewExecutableSchema(generated.Config{Resolvers: resolver})
	gql := handler.New(es)
	gql.AddTransport(transport.Options{})
	gql.AddTransport(transport.GET{})
	gql.AddTransport(transport.POST{})
	gql.AddTransport(transport.MultipartForm{})
	gql.AddTransport(transport.Websocket{
		KeepAlivePingInterval: 10 * time.Second,
		Upgrader: websocket.Upgrader{
			CheckOrigin:     func(r *http.Request) bool { return true },
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	})
	gql.SetQueryCache(lru.New[*ast.QueryDocument](100))
	gql.Use(extension.Introspection{})

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware)
	r.Handle("/graphql", gql)
	r.Handle("/graphql/", gql)
	// Apollo-style alias the existing dashboard might use.
	r.Handle("/query", gql)
	r.Get("/playground", playground.Handler("MonsterMQ Edge", "/graphql"))
	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return &Server{
		cfg: cfg, logger: logger, router: r,
	}
}

// AttachDashboard mounts a static file handler at /. Called from the broker
// bootstrap once the dashboard FS has been resolved.
func (s *Server) AttachDashboard(fs http.Handler) {
	s.router.Handle("/*", fs)
}

func (s *Server) Start() error {
	s.httpSrv = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.cfg.GraphQL.Port),
		Handler:           s.router,
		ReadHeaderTimeout: 10 * time.Second,
	}
	s.logger.Info("graphql listening", "port", s.cfg.GraphQL.Port)
	if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
