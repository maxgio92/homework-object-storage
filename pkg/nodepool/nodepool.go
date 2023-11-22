package nodepool

import (
	"net"
	"sync"
	"time"

	"github.com/maxgio92/consistenthash"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	maxHealthCheckRetry = 20
)

// NodePool represents a sharding pool of MinIO instances.
// Each node is supposed to serve a specific object, in a sharding manner.
type NodePool struct {
	// ring is a network for consistent hashed nodes.
	ring *consistenthash.Ring

	// nodeIdToClient is an in-memory storage of node-specific MinIO clients.
	nodeIdToClient map[string]*minio.Client

	// nodeIdToConfig is an in-memory storage of node configs.
	nodeIdToConfig map[string]*NodeConfig

	sync.RWMutex

	logger *log.Logger
}

type Option func(p *NodePool)

func WithLogger(logger *log.Logger) Option {
	return func(p *NodePool) {
		p.logger = logger
	}
}

func WithNodeConfigs(configs ...*NodeConfig) Option {
	return func(p *NodePool) {
		p.nodeIdToConfig = make(map[string]*NodeConfig, len(configs))

		for _, v := range configs {
			p.ring.AddNode(v.endpoint)
			p.nodeIdToConfig[v.endpoint] = v
		}
	}
}

func NewNodePool(opts ...Option) *NodePool {
	np := new(NodePool)

	np.ring = consistenthash.NewRing()

	np.nodeIdToClient = make(map[string]*minio.Client)

	np.nodeIdToConfig = make(map[string]*NodeConfig)

	for _, f := range opts {
		f(np)
	}

	return np
}

func (p *NodePool) Init() error {
	if err := p.validate(); err != nil {
		return errors.Wrap(err, "error validating the node pool")
	}
	if err := p.buildClients(); err != nil {
		return errors.Wrap(err, "error building clients")
	}
	if err := p.healthcheck(); err != nil {
		return errors.Wrap(err, "error running healthcheck")
	}

	return nil
}

func (p *NodePool) validate() error {
	if p.nodeIdToConfig == nil {
		return errors.New("the node pool is empty")
	}
	if p.logger == nil {
		return errors.New("the node pool logger is nil")
	}

	for _, node := range p.nodeIdToConfig {
		if node.accessKey == "" || node.secretKey == "" {
			return errors.New("the node config is missing credentials")
		}
		if node.endpoint == "" {
			return errors.New("the node config is missing endpoint")
		}
	}

	return nil
}

func (p *NodePool) healthcheck() error {
	for _, node := range p.nodeIdToConfig {
		p.logger.Debugf("running health check on node %s", node.endpoint)

		// TODO: improve retry logic with smarter algorithm.
		retry := maxHealthCheckRetry
		for retry > 0 {
			_, err := net.Dial("tcp", node.endpoint)
			if err != nil {
				p.logger.WithError(err).Errorf("can't connect to backend instance %s", node.endpoint)
				retry--
				time.Sleep(1 * time.Second)

				return err
			}
			retry = 0
			p.logger.Debugf("connection to backend instance %s accepted", node.endpoint)
		}
	}

	return nil
}

func (p *NodePool) buildClients() error {
	p.nodeIdToClient = make(map[string]*minio.Client, len(p.nodeIdToConfig))
	for _, node := range p.nodeIdToConfig {
		minioClient, err := minio.New(node.endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(node.accessKey, node.secretKey, ""),
			Secure: false,
		})
		if err != nil {
			return err
		}
		p.nodeIdToClient[node.endpoint] = minioClient
	}

	return nil
}

func (p *NodePool) NodeClient(id string) *minio.Client {
	p.RLock()
	defer p.RUnlock()

	return p.nodeIdToClient[id]
}

func (p *NodePool) ObjectToNodeID(key string) string {
	return p.ring.Get(key)
}
