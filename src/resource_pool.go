package main

import (
	"sync"
	)

type ResourcePool struct {
	mu sync.Mutex

	nodes map[int][]NodeStatus
}

func (pool *ResourcePool) update(node MsgAgent) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	status, ok := pool.nodes[node.ClientID]
	if ok {
		for i := range status {
			if status[i].UUID == node.Status[i].UUID {
				node.Status[i].MemoryAllocated = status[i].MemoryAllocated
			}
		}
	}
	pool.nodes[node.ClientID] = node.Status

	//log.Println(pool.nodes)
}

func (pool *ResourcePool) getByID(id int) []NodeStatus {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	status, ok := pool.nodes[id]
	if ok {
		return status
	}
	return []NodeStatus{}
}
