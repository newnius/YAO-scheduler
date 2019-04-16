package main

import (
	"sync"
)

type ResourcePool struct {
	mu sync.Mutex

	nodes map[int]NodeStatus
}

func (pool *ResourcePool) update(node NodeStatus) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	status, ok := pool.nodes[node.ClientID]
	if ok {
		for i, GPU := range status.Status {
			if GPU.UUID == node.Status[i].UUID {
				node.Status[i].MemoryAllocated = GPU.MemoryAllocated
			}
		}
	}
	pool.nodes[node.ClientID] = node

	//log.Println(pool.nodes)
}

func (pool *ResourcePool) getByID(id int) NodeStatus {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	status, ok := pool.nodes[id]
	if ok {
		return status
	}
	return NodeStatus{}
}
