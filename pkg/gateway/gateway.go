package gateway

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"

	"github.com/maxgio92/homework-object-storage/internal/output"
	"github.com/maxgio92/homework-object-storage/pkg/minio"
)

const (
	bindIP   = "127.0.01"
	bindPort = 3000
)

// Gateway is a MinIO gateway.
type Gateway struct {
	logger *log.Logger

	r   *mux.Router
	srv *http.Server

	nodePool *minio.NodePool
}

type Option func(gw *Gateway)

func WithLogger(logger *log.Logger) Option {
	return func(gw *Gateway) {
		gw.logger = logger
	}
}

func WithHTTPServer(srv *http.Server) Option {
	return func(gw *Gateway) {
		gw.srv = srv
	}
}

func WithRouter(router *mux.Router) Option {
	return func(gw *Gateway) {
		gw.r = router
	}
}

func WithNodePool(nodePool *minio.NodePool) Option {
	return func(gw *Gateway) {
		gw.nodePool = nodePool
	}
}

// NewGateway returns a new Gateway.
func NewGateway(opts ...Option) *Gateway {
	gw := new(Gateway)

	for _, f := range opts {
		f(gw)
	}

	gw.init()

	if gw.r == nil {
		gw.r = mux.NewRouter()
	}
	gw.r.HandleFunc("/", gw.HomeHandler)
	gw.AddObjectRoutes(gw.r)
	http.Handle("/", gw.r)

	gw.srv.Handler = gw.r

	return gw
}

func (g *Gateway) init() {
	if g.logger == nil {
		g.logger = output.NewJSONLogger(
			output.WithOutput(os.Stderr),
		)
	}
	if g.srv == nil {
		g.srv = &http.Server{
			Addr:         fmt.Sprintf("%s:%d", bindIP, bindPort),
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  15 * time.Second,
		}
	}
}

func (g *Gateway) Run() error {
	if err := g.nodePool.Init(); err != nil {
		return err
	}
	if err := g.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (g *Gateway) Shutdown(ctx context.Context) error {
	return g.srv.Shutdown(ctx)
}
