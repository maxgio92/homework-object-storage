package serve

import (
	"fmt"
	"time"
)

const (
	programDescription = "Object Storage Gateway"

	serverListenAddress           = "127.0.0.1:3000"
	serverIdleTimeout             = 15 * time.Second
	serverReadTimeout             = 15 * time.Second
	serverWriteTimeout            = 15 * time.Second
	serverGracefulShutdownTimeout = 30 * time.Second

	dockerNetworkName = "homework-object-storage_amazin-object-storage"
	dockerMinIoName   = "MinIO"

	minioPort         = 9000
	minioEnvAccessKey = "MINIO_ACCESS_KEY"
	minioEnvSecretKey = "MINIO_SECRET_KEY"
)

var (
	minIoDockerContainerLabelSelector = []string{
		fmt.Sprintf("name=%s", dockerMinIoName),
	}
)
