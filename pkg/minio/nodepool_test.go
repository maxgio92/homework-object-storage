package minio

import (
	"github.com/pkg/errors"
	"net"
	"reflect"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

func TestNewNodePool(t *testing.T) {
	logger := logrus.New()

	nodeConfig := NewNodeConfig("localhost:3000", "mykey", "mysecret")
	nodeConfig2 := NewNodeConfig("localhost:3001", "mykey", "mysecret")

	testCases := []struct {
		name  string
		given []Option
		want  *NodePool
	}{
		{name: "with no option", given: []Option{}, want: &NodePool{
			objectsToNodeID: make(map[string]string),
			nodeIDToClient:  make(map[string]*minio.Client),
			nodeIDToConfig:  make(map[string]*NodeConfig),
		}},
		{name: "with logger", given: []Option{WithLogger(logger)}, want: &NodePool{
			logger:          logger,
			objectsToNodeID: make(map[string]string),
			nodeIDToClient:  make(map[string]*minio.Client),
			nodeIDToConfig:  make(map[string]*NodeConfig),
		}},
		{name: "with node configs", given: []Option{WithNodeConfigs(nodeConfig, nodeConfig2)}, want: &NodePool{
			objectsToNodeID: make(map[string]string),
			nodeIDToClient:  make(map[string]*minio.Client),
			nodeIDToConfig:  map[string]*NodeConfig{nodeConfig.endpoint: nodeConfig, nodeConfig2.endpoint: nodeConfig2},
			roundRobin:      newRoundRobin(nodeConfig.endpoint, nodeConfig2.endpoint),
		}},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			got := NewNodePool(tt.given...)
			if got == nil {
				t.Errorf("node pool is nil")
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodePoolInit(t *testing.T) {
	logger := logrus.StandardLogger()

	// Node server listening.
	node := NewNodeConfig("localhost:3000", "mykey", "mysecret")

	l, _ := net.Listen("tcp", node.endpoint)
	defer l.Close()

	// Node server closed.
	node2 := NewNodeConfig("localhost:3001", "mykey", "mysecret")

	testCases := []struct {
		name  string
		given *NodePool
		want  error
	}{
		{name: "with online nodes", given: NewNodePool(WithLogger(logger), WithNodeConfigs(node)), want: nil},
		{name: "with one offline node", given: NewNodePool(WithLogger(logger), WithNodeConfigs(node, node2)), want: unix.ECONNREFUSED},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.given.Init()
			got := tt.given
			if !errors.Is(err, tt.want) {
				t.Errorf("got error %v, want error %v", got, tt.want)
			}
			if tt.want == nil {
				if got == nil {
					t.Errorf("node pool is nil after init")
				}
				for _, c := range got.nodeIDToClient {
					if c == nil {
						t.Error("client is nil")
					}
					if c.IsOffline() {
						t.Error("client is offline")
					}
				}
			}
		})
	}
}
