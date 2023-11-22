package nodepool

// NodeConfig represents the set of configurations of a MinIO node.
type NodeConfig struct {
	endpoint             string
	accessKey, secretKey string
}

func NewNodeConfig(endpoint, accessKey, secretKey string) *NodeConfig {
	config := new(NodeConfig)
	config.endpoint = endpoint
	config.accessKey = accessKey
	config.secretKey = secretKey

	return config
}
