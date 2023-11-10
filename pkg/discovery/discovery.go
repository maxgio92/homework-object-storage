package discovery

type Discoverer interface {
	DiscoverEndpoints() ([]Endpoint, error)
}

type Endpoint struct {
	Address string
	Env     map[string]string
}
