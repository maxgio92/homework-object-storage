package discovery

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	docker "github.com/docker/docker/client"
	"strings"
)

const (
	containerStatusRunning = "running"
)

// DockerDiscoverer is a discoverer of Docker containers.
type DockerDiscoverer struct {
	client  *docker.Client
	network string
}

type DockerOption func(d *DockerDiscoverer)

func WithNetwork(network string) DockerOption {
	return func(d *DockerDiscoverer) {
		d.network = network
	}
}

func NewDockerDiscovererFromClient(client *docker.Client, options ...DockerOption) *DockerDiscoverer {
	discoverer := new(DockerDiscoverer)
	discoverer.client = client

	for _, f := range options {
		f(discoverer)
	}

	return discoverer
}

// DiscoverEndpoints returns a list of container endpoints and container environment,
// selected by label.
// The endpoint port can be overridden with portOverride argument.
func (c *DockerDiscoverer) DiscoverEndpoints(ctx context.Context, labelSelectors []string,
	portOverride ...uint16) ([]Endpoint, error) {
	cli, err := docker.NewClientWithOpts(docker.FromEnv)
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	// Supported filters: https://docs.docker.com/engine/api/v1.24/.
	args := []filters.KeyValuePair{}
	for _, v := range labelSelectors {
		args = append(args, filters.Arg("label", fmt.Sprintf("%s", v)))
	}

	args = append(args, filters.Arg("network", c.network))
	args = append(args, filters.Arg("status", containerStatusRunning))

	containers, err := cli.ContainerList(ctx, types.ContainerListOptions{
		Filters: filters.NewArgs(args...),
	})
	if err != nil {
		return nil, err
	}

	var port uint16
	if len(portOverride) > 0 {
		port = portOverride[0]
	}

	endpoints := []Endpoint{}
	for _, container := range containers {
		e := new(Endpoint)

		if port == 0 && len(container.Ports) > 0 {
			port = container.Ports[0].PrivatePort
		}

		e.Address = fmt.Sprintf("%s:%d", container.NetworkSettings.Networks[c.network].IPAddress, port)
		inspect, err := cli.ContainerInspect(ctx, container.ID)
		if err != nil {
			return nil, err
		}

		if inspect.Config != nil {
			e.Env = make(map[string]string, len(inspect.Config.Env))
			for _, env := range inspect.Config.Env {
				s := strings.Split(env, "=")
				e.Env[s[0]] = s[1]
			}
		}

		endpoints = append(endpoints, *e)
	}

	return endpoints, nil
}
