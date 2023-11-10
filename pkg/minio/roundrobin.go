package minio

import (
	"sync/atomic"
)

// roundRobin is a storge for minIO round-robin load balancing.
type roundRobin struct {
	ids  []string
	next uint32
}

func newRoundRobin(ids ...string) *roundRobin {
	return &roundRobin{ids: ids}
}

// Next returns the next MinIO node id.
func (r *roundRobin) Next() string {
	n := atomic.AddUint32(&r.next, 1)
	return r.ids[(int(n)-1)%len(r.ids)]
}
