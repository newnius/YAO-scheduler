package main

import (
	"sync"
	"time"
	"net/url"
	"strings"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"strconv"
)

type ResourcePool struct {
	mu    sync.Mutex
	nodes map[string]NodeStatus

	history []PoolStatus

	heartBeat map[string]time.Time

	networks     map[string]bool
	networksFree map[string]bool
	networkMu    sync.Mutex

	versions map[string]float64

	counter      int
	counterTotal int
}

func (pool *ResourcePool) start() {
	//TODO: retrieve networks from yao-agent-master in blocking io
	pool.networks = map[string]bool{}
	pool.networksFree = map[string]bool{}
	pool.versions = map[string]float64{}

	/* check dead nodes */
	go func() {
		pool.heartBeat = map[string]time.Time{}

		for {
			for k, v := range pool.heartBeat {
				if v.Add(time.Second * 30).Before(time.Now()) {
					delete(pool.nodes, k)
					delete(pool.versions, k)
				}
			}
			time.Sleep(time.Second * 10)
		}
	}()

	/* save pool status periodically */
	go func() {
		/* waiting for data */
		pool.history = []PoolStatus{}
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
				pool.history = pool.history[len(pool.history)-60:]
			}
			time.Sleep(time.Second * 60)
		}
	}()
}

func (pool *ResourcePool) update(node NodeStatus) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.counterTotal++
	if version, ok := pool.versions[node.ClientID]; ok && version == node.Version {
		return
	}

	pool.counter++
	status, ok := pool.nodes[node.ClientID]
	if ok {
		for i, GPU := range status.Status {
			if GPU.UUID == node.Status[i].UUID {
				node.Status[i].MemoryAllocated = GPU.MemoryAllocated
			}
		}
	}
	pool.nodes[node.ClientID] = node
	pool.heartBeat[node.ClientID] = time.Now()
	pool.versions[node.ClientID] = node.Version
	log.Debug(pool.nodes)
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

func (pool *ResourcePool) getCounter() map[string]int {
	return map[string]int{"counter": pool.counter, "counterTotal": pool.counterTotal}
}

func (pool *ResourcePool) acquireNetwork() string {
	pool.networkMu.Lock()
	defer pool.networkMu.Unlock()
	var network string
	log.Info(pool.networksFree)
	if len(pool.networksFree) == 0 {
		for {
			for {
				network = "yao-net-" + strconv.Itoa(rand.Intn(999999))
				if _, ok := pool.networks[network]; !ok {
					break
				}
			}
			v := url.Values{}
			v.Set("name", network)
			resp, err := doRequest("POST", "http://yao-agent-master:8000/create", strings.NewReader(v.Encode()), "application/x-www-form-urlencoded", "")
			if err != nil {
				log.Println(err.Error())
				continue
			}
			defer resp.Body.Close()
			pool.networksFree[network] = true
			pool.networks[network] = true
			break
		}
	}

	for k := range pool.networksFree {
		network = k
		delete(pool.networksFree, k)
	}
	return network
}

func (pool *ResourcePool) releaseNetwork(network string) {
	pool.networkMu.Lock()
	pool.networksFree[network] = true
	pool.networkMu.Unlock()
}
