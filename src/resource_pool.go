package main

import (
	"sync"
	"time"
)

type ResourcePool struct {
	mu    sync.Mutex
	nodes map[string]NodeStatus

	history []PoolStatus
}

func (pool *ResourcePool) start() {
	go func() {
		/* waiting for data */
		time.Sleep(time.Second * 30)
		for {
			summary := PoolStatus{}

			UtilCPU := 0.0
			TotalCPU := 0
			TotalMem := 0
			AvailableMem := 0

			TotalGPU := 0
			UtilGPU := 0
			TotalMemGPU := 0
			AvailableMemGPU := 0
			for _, node := range pool.nodes {
				UtilCPU += node.UtilCPU
				TotalCPU += node.NumCPU
				TotalMem += node.MemTotal
				AvailableMem += node.MemAvailable

				for _, GPU := range node.Status {
					UtilGPU += GPU.UtilizationGPU
					TotalGPU ++
					TotalMemGPU += GPU.MemoryTotal
					AvailableMemGPU += GPU.MemoryFree
				}
			}
			summary.TimeStamp = time.Now().Format("2006-01-02 15:04:05")
			summary.UtilCPU = UtilCPU / (float64(len(pool.nodes)) + 0.001)
			summary.TotalCPU = TotalCPU
			summary.TotalMem = TotalMem
			summary.AvailableMem = AvailableMem
			summary.TotalGPU = TotalGPU
			if TotalGPU == 0 {
				summary.UtilGPU = 0.0
			} else {
				summary.UtilGPU = UtilGPU / TotalGPU
			}
			summary.TotalMemGPU = TotalMemGPU
			summary.AvailableMemGPU = AvailableMemGPU

			pool.history = append(pool.history, summary)

			if len(pool.history) > 60 {
				pool.history = pool.history[0:60]
			}
			time.Sleep(time.Second * 60)
		}
	}()
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

func (pool *ResourcePool) getByID(id string) NodeStatus {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	status, ok := pool.nodes[id]
	if ok {
		return status
	}
	return NodeStatus{}
}

func (pool *ResourcePool) list() MsgResource {
	return MsgResource{Code: 0, Resource: pool.nodes}
}

func (pool *ResourcePool) statusHistory() MsgPoolStatusHistory {
	return MsgPoolStatusHistory{Code: 0, Data: pool.history}
}
