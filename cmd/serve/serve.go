package serve

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maxgio92/homework-object-storage/internal/output"

	"github.com/docker/docker/client"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/maxgio92/homework-object-storage/pkg/discovery"
	"github.com/maxgio92/homework-object-storage/pkg/gateway"
	"github.com/maxgio92/homework-object-storage/pkg/minio"
)

// Command represents the serve command.
type Command struct {
	logger   *log.Logger
	logLevel string

	// Gateway's server parameters.
	serverListenAddress string
	serverReadTimeout   time.Duration
	serverWriteTimeout  time.Duration
	serverIdleTimeout   time.Duration

	// Gateway's backend parameters.
	minioDockerContainerSelector []string
	minioAccessKeyEnvVar         string
	minioSecretKeyEnvVar         string
}

func Execute() {
	cmd := NewCmd()
	err := cmd.Execute()
	if err != nil {
		panic(err)
	}
}

// NewCmd returns a new find command.
func NewCmd() *cobra.Command {
	c := new(Command)

	cmd := &cobra.Command{
		Use:               "serve",
		Short:             fmt.Sprintf("Serve the %s", programDescription),
		DisableAutoGenTag: true,
		Args:              cobra.MinimumNArgs(1),
		RunE:              c.Run,
		PersistentPreRun: func(_ *cobra.Command, _ []string) {
			c.initLogs()
		},
	}

	cmd.PersistentFlags().StringVarP(&c.logLevel, "verbosity", "v", log.DebugLevel.String(),
		"The log verbosity level.")

	cmd.Flags().StringVarP(&c.serverListenAddress, "listen-address", "l", serverListenAddress,
		"The address to listen on.")
	cmd.Flags().DurationVar(&c.serverReadTimeout, "read-timeout", serverReadTimeout,
		"Server read timeout")
	cmd.Flags().DurationVar(&c.serverWriteTimeout, "write-timeout", serverWriteTimeout,
		"Server write timeout")
	cmd.Flags().DurationVar(&c.serverIdleTimeout, "idle-timeout", serverIdleTimeout,
		"Server idle timeout")
	cmd.Flags().StringSliceVar(&c.minioDockerContainerSelector, "minio-label", minIoDockerContainerLabelSelector,
		"The label selector for MinIO Docker containers")
	cmd.Flags().StringVar(&c.minioAccessKeyEnvVar, "minio-access-key-env-var", minioEnvAccessKey,
		"The environment variable name of the MinIO access key")
	cmd.Flags().StringVar(&c.minioSecretKeyEnvVar, "minio-secret-key-env-var", minioEnvSecretKey,
		"The environment variable name of the MinIO secret key")

	return cmd
}

func (c *Command) initLogs() {
	logger := output.NewJSONLogger(
		output.WithLevel(c.logLevel),
		output.WithOutput(os.Stderr),
	)
	c.logger = logger
}

func (c *Command) Run(_ *cobra.Command, _ []string) error {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Build the Docker client.
	c.logger.Debug("building docker client")

	dockerC, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return errors.Wrap(err, "error building docker client")
	}
	c.logger.Debug("discovery minio docker endpoints")

	// Discover the MinIO Docker containers.
	endpoints, err := discovery.NewDockerDiscovererFromClient(
		dockerC,
		discovery.WithNetwork(dockerNetworkName),
	).DiscoverEndpoints(
		context.Background(),
		c.minioDockerContainerSelector,
		minioPort,
	)
	if err != nil {
		return errors.Wrap(err, "error getting minio endpoints")
	}
	c.logger.Debug("building minio node pool config")

	// Build the MinIO node pool as the gateway backend.
	nodeConfigs := []*minio.NodeConfig{}
	for _, v := range endpoints {
		nodeConfigs = append(nodeConfigs,
			minio.NewNodeConfig(
				v.Address,
				v.Env[c.minioAccessKeyEnvVar],
				v.Env[c.minioSecretKeyEnvVar],
			),
		)
	}

	backend := minio.NewNodePool(
		minio.WithNodeConfigs(nodeConfigs...),
		minio.WithLogger(c.logger),
	)
	c.logger.Debug("building minio gateway")

	// Build the MinIO gateway.
	srv := &http.Server{
		Addr:         c.serverListenAddress,
		WriteTimeout: c.serverWriteTimeout,
		ReadTimeout:  c.serverReadTimeout,
		IdleTimeout:  c.serverIdleTimeout,
	}

	gtw := gateway.NewGateway(
		gateway.WithLogger(c.logger),
		gateway.WithHTTPServer(srv),
		gateway.WithNodePool(backend),
	)

	// Run the MinIO gateway.
	go func() {
		c.logger.Infof("Gateway listening at: %s", c.serverListenAddress)

		if err := gtw.Run(); err != nil {
			c.logger.Fatal(errors.Wrap(err, "error running the gateway"))
		}
	}()

	// Wait for termination.
	<-signalCh
	c.logger.Println("Terminating the gateway...")

	// Gracefully shut down the gateway.
	ctx, cancel := context.WithTimeout(context.Background(), serverGracefulShutdownTimeout)
	defer cancel()

	if err := gtw.Shutdown(ctx); err != nil {
		c.logger.Fatal(errors.Wrap(err, "error shutting down the gateway"))
		return err
	}

	return nil
}
