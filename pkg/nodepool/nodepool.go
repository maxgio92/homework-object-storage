package minio

import (
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"time"
)

const (
	maxHealthCheckRetry = 20
)

// NodePool represents a sharding pool of MinIO instances.
// Each node is supposed to serve a specific object, in a sharding manner.
type NodePool struct {
	// objectsToNodeID is an in-memory storage of objects to node-relations.
	objectsToNodeID map[string]string

	// nodeIDToClient is an in-memory storage of node-specific MinIO clients.
	nodeIDToClient map[string]*minio.Client

	// nodeIDToConfig is an in-memory storage of node configs.
	nodeIDToConfig map[string]*NodeConfig

	// RoundRobin is an in-memory storage to balance write operations.
	*roundRobin

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
		p.nodeIDToConfig = make(map[string]*NodeConfig, len(configs))
		ids := []string{}
		for _, v := range configs {
			p.nodeIDToConfig[v.endpoint] = v
			ids = append(ids, v.endpoint)
		}
		p.roundRobin = newRoundRobin(ids...)
	}
}

func NewNodePool(opts ...Option) *NodePool {
	np := new(NodePool)

	np.objectsToNodeID = make(map[string]string)
	np.nodeIDToClient = make(map[string]*minio.Client)
	np.nodeIDToConfig = make(map[string]*NodeConfig)

	for _, f := range opts {
		f(np)
	}

	return np
}

func (p *NodePool) Init() error {
	if err := p.validate(); err != nil {
		return errors.Wrap(err, "error validating the node pool")
	}

	p.nodeIDToClient = make(map[string]*minio.Client, len(p.nodeIDToConfig))
	for _, node := range p.nodeIDToConfig {
		minioClient, err := minio.New(node.endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(node.accessKey, node.secretKey, ""),
			Secure: false,
		})
		if err != nil {
			return err
		}
		p.nodeIDToClient[node.endpoint] = minioClient
	}

	return nil
}

func (p *NodePool) validate() error {
	if p.nodeIDToConfig == nil || len(p.nodeIDToConfig) == 0 {
		return errors.New("the node pool is empty")
	}
	if p.logger == nil {
		return errors.New("the node pool logger is nil")
	}

	for _, node := range p.nodeIDToConfig {
		if node.accessKey == "" || node.secretKey == "" {
			return errors.New("the node config is missing credentials")
		}
		if node.endpoint == "" {
			return errors.New("the node config is missing endpoint")
		}

		p.logger.Debugf("validating node %s", node.endpoint)

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

func (p *NodePool) NodeConfig(id string) *NodeConfig {
	p.RLock()
	defer p.RUnlock()

	return p.nodeIDToConfig[id]
}

func (p *NodePool) NodeClient(id string) *minio.Client {
	p.RLock()
	defer p.RUnlock()

	return p.nodeIDToClient[id]
}

func (p *NodePool) ObjectToNodeID(id string) string {
	p.RLock()
	defer p.RUnlock()

	return p.objectsToNodeID[id]
}

func (p *NodePool) SetObjectNodeID(objectID, nodeID string) {
	p.Lock()
	defer p.Unlock()

	p.objectsToNodeID[objectID] = nodeID
}
