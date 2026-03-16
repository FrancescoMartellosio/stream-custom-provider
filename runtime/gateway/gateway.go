package gateway

import (
	"github.com/nitrictech/nitric/core/pkg/gateway"
)

type GatewayServer struct {
	stop chan struct{}
}

var _ gateway.GatewayService = &GatewayServer{}

func (g *GatewayServer) Start(opts *gateway.GatewayStartOpts) error {
	// Block until Stop closes the channel
	<-g.stop
	return nil
}

func (g *GatewayServer) Stop() error {
	// Signal Start() to stop blocking
	close(g.stop)
	return nil
}

func New() (*GatewayServer, error) {
	return &GatewayServer{
		stop: make(chan struct{}),
	}, nil
}
