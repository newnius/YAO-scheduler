package main

import (
	"sync"
)

type ResourcePool struct {
	mu sync.Mutex

	nodes map[int][]Status
}

func (pool *ResourcePool) update(node MsgAgent) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.nodes[node.ClientID] = node.Status
}

func (pool *ResourcePool) getByID(id int) []Status {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	status, ok := pool.nodes[id]
	if ok {
		return status
	}
	return []Status{}
}
