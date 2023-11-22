package gateway

import (
	"github.com/gorilla/mux"
	"net/http"
	"reflect"
	"testing"

	"github.com/maxgio92/homework-object-storage/pkg/nodepool"
	"github.com/sirupsen/logrus"
)

func TestNewGateway(t *testing.T) {
	logger := logrus.StandardLogger()

	node := nodepool.NewNodeConfig("localhost:9004", "mykey", "mysecret")
	node2 := nodepool.NewNodeConfig("localhost:9005", "mykey", "mysecret")

	nodePool := nodepool.NewNodePool(nodepool.WithNodeConfigs(node, node2), nodepool.WithLogger(logger))

	srv := &http.Server{Addr: "127.0.0.1:3000"}
	router := mux.NewRouter()

	testCases := []struct {
		name  string
		given []Option
		want  *Gateway
	}{
		{
			name:  "with logger, http server and running nodes",
			given: []Option{WithLogger(logger), WithHTTPServer(srv), WithNodePool(nodePool), WithRouter(router)},
			want:  &Gateway{logger: logger, r: router, srv: srv, nodePool: nodePool},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			got := NewGateway(tt.given...)
			if got == nil {
				t.Errorf("gateway is nil")
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
