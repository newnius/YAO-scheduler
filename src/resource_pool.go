package main

import (
	"sync"
	"time"
	"net/url"
	"strings"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"strconv"
	"hash/fnv"
)

type ResourcePool struct {
	//mu    sync.Mutex
	//nodes map[string]NodeStatus
	pools      []map[string]NodeStatus
	poolsMu    []sync.Mutex
	poolsCount int

	history []PoolStatus

	heartBeat   map[string]time.Time
	heartBeatMu sync.Mutex

	networks     map[string]bool
	networksFree map[string]bool
	networkMu    sync.Mutex

	versions map[string]float64

	counter      int
	counterTotal int

	bindings   map[string]map[string]bool
	bindingsMu sync.Mutex
	utils      map[string][]int
}

func (pool *ResourcePool) GPUModelToPower(model string) int {
	mapper := map[string]int{"k40": 1, "K80": 2, "P100": 3}
	if power, err := mapper[model]; !err {
		return power
	}
	return 0
}

func (pool *ResourcePool) getNodePool(name string) int {
	h := fnv.New32a()
	h.Write([]byte(name))
	return int(h.Sum32()) % pool.poolsCount
}

func (pool *ResourcePool) start() {
	//TODO: retrieve networks from yao-agent-master in blocking io
	pool.networks = map[string]bool{}
	pool.networksFree = map[string]bool{}
	pool.versions = map[string]float64{}

	pool.bindings = map[string]map[string]bool{}
	pool.utils = map[string][]int{}

	pool.poolsCount = 100
	for i := 0; i < pool.poolsCount; i++ {
		pool.pools = append(pool.pools, map[string]NodeStatus{})
		pool.poolsMu = append(pool.poolsMu, sync.Mutex{})
	}

	/* check dead nodes */
	go func() {
		pool.heartBeat = map[string]time.Time{}

		for {
			pool.heartBeatMu.Lock()
			for k, v := range pool.heartBeat {
				if v.Add(time.Second * 30).Before(time.Now()) {
					poolID := pool.getNodePool(k)
					pool.poolsMu[poolID].Lock()
					delete(pool.pools[poolID], k)
					delete(pool.versions, k)
					pool.poolsMu[poolID].Unlock()
				}
			}
			pool.heartBeatMu.Unlock()
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
			nodesCount := 0
			for i := 0; i < pool.poolsCount; i++ {
				pool.poolsMu[i].Lock()
				for _, node := range pool.pools[i] {
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
				nodesCount += len(pool.pools[i])
				pool.poolsMu[i].Unlock()
			}
			summary.TimeStamp = time.Now().Format("2006-01-02 15:04:05")
			summary.UtilCPU = UtilCPU / (float64(nodesCount) + 0.001)
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
	poolID := pool.getNodePool(node.ClientID)

	pool.poolsMu[poolID].Lock()
	defer pool.poolsMu[poolID].Unlock()

	go func(node NodeStatus) {
		pool.bindingsMu.Lock()
		defer pool.bindingsMu.Unlock()
		for _, gpu := range node.Status {
			if _, ok := pool.bindings[gpu.UUID]; ok {
				if len(pool.bindings[gpu.UUID]) == 1 {
					pool.utils[gpu.UUID] = append(pool.utils[gpu.UUID], gpu.UtilizationGPU)
				}
			}
		}
		pool.heartBeatMu.Lock()
		pool.heartBeat[node.ClientID] = time.Now()
		pool.heartBeatMu.Unlock()
	}(node)

	pool.counterTotal++
	if version, ok := pool.versions[node.ClientID]; ok && version == node.Version {
		return
	}

	log.Debug(node.Version, "!=", pool.versions[node.ClientID])

	pool.counter++
	status, ok := pool.pools[poolID][node.ClientID]
	if ok {
		for i, GPU := range status.Status {
			if GPU.UUID == node.Status[i].UUID {
				node.Status[i].MemoryAllocated = GPU.MemoryAllocated
			}
		}
	}
	pool.pools[poolID][node.ClientID] = node
	pool.versions[node.ClientID] = node.Version
}

func (pool *ResourcePool) getByID(id string) NodeStatus {
	poolID := pool.getNodePool(id)

	pool.poolsMu[poolID].Lock()
	defer pool.poolsMu[poolID].Unlock()

	status, ok := pool.pools[poolID][id]
	if ok {
		return status
	}
	return NodeStatus{}
}

func (pool *ResourcePool) list() MsgResource {
	nodes := map[string]NodeStatus{}
	for i := 0; i < pool.poolsCount; i++ {
		pool.poolsMu[i].Lock()
		for k, node := range pool.pools[i] {
			nodes[k] = node
		}
		pool.poolsMu[i].Unlock()
	}
	return MsgResource{Code: 0, Resource: nodes}
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
	log.Debug(pool.networksFree)
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

func (pool *ResourcePool) attach(GPU string, job string) {
	pool.bindingsMu.Lock()
	defer pool.bindingsMu.Unlock()
	if _, ok := pool.bindings[GPU]; !ok {
		pool.bindings[GPU] = map[string]bool{}
	}
	pool.bindings[GPU][job] = true

	if _, ok := pool.utils[GPU]; !ok {
		pool.utils[GPU] = []int{}
	}
}

func (pool *ResourcePool) detach(GPU string, jobName string) {
	pool.bindingsMu.Lock()
	defer pool.bindingsMu.Unlock()
	if _, ok := pool.bindings[GPU]; ok {
		if len(pool.bindings[GPU]) == 1 {
			InstanceOfOptimizer().feed(jobName, pool.utils[GPU])
			pool.utils[GPU] = []int{}
		}
	}

	if list, ok := pool.bindings[GPU]; ok {
		delete(list, jobName)
	}
}

func (pool *ResourcePool) getBindings() map[string]map[string]bool {
	return pool.bindings
}
