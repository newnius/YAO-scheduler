package main

import (
	"sync"
	"time"
	"net/url"
	"strings"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"strconv"
	"sort"
	"hash/fnv"
)

type ResourcePool struct {
	poolsCount int
	pools      []PoolSeg
	poolsMu    sync.Mutex

	history []PoolStatus

	heartBeat   map[string]time.Time
	heartBeatMu sync.Mutex

	networks     map[string]bool
	networksFree map[string]bool
	networkMu    sync.Mutex

	versions   map[string]float64
	versionsMu sync.Mutex

	counter      int
	counterTotal int

	bindings   map[string]map[string]int
	bindingsMu sync.Mutex
	utils      map[string][]UtilGPUTimeSeries

	TotalGPU int
}

func (pool *ResourcePool) start() {
	pool.networks = map[string]bool{}
	pool.networksFree = map[string]bool{}
	pool.versions = map[string]float64{}

	pool.bindings = map[string]map[string]int{}
	pool.utils = map[string][]UtilGPUTimeSeries{}

	pool.TotalGPU = 0

	/* init pools */
	pool.poolsCount = 300
	for i := 0; i < pool.poolsCount; i++ {
		pool.pools = append(pool.pools, PoolSeg{Lock: sync.Mutex{}, IsVirtual: true, ID: i})
	}
	/* make non-virtual seg */
	for i := 0; i < pool.poolsCount/3; i++ {
		pool.pools[rand.Intn(pool.poolsCount)].IsVirtual = false
	}
	/* make working srg */
	for i := 0; i < 10; i++ {
		pool.pools[rand.Intn(pool.poolsCount)].Nodes = map[string]*NodeStatus{}
	}
	/* init Next pointer */
	var pre *PoolSeg
	for i := pool.poolsCount*2 - 1; ; i-- {
		if pool.pools[i%pool.poolsCount].Next != nil {
			break
		}
		pool.pools[i%pool.poolsCount].Next = pre
		if pool.pools[i%pool.poolsCount].Nodes != nil {
			pre = &pool.pools[i%pool.poolsCount]
		}
	}

	pool.heartBeat = map[string]time.Time{}
	go func() {
		pool.checkDeadNodes()
	}()

	pool.history = []PoolStatus{}
	go func() {
		pool.saveStatusHistory()
	}()

	segID := rand.Intn(pool.poolsCount)
	start := &pool.pools[segID]
	if start.Nodes == nil {
		start = start.Next
	}
	for cur := start; ; {
		log.Info(cur.ID)
		cur = cur.Next
		if cur.ID == start.ID {
			break
		}
	}
}

/* check dead nodes periodically */
func (pool *ResourcePool) checkDeadNodes() {
	for {
		pool.heartBeatMu.Lock()
		for k, v := range pool.heartBeat {
			if v.Add(time.Second * 30).Before(time.Now()) {
				poolID := pool.getNodePool(k)
				seg := &pool.pools[poolID]
				if seg.Nodes == nil {
					seg = seg.Next
				}
				seg.Lock.Lock()
				delete(seg.Nodes, k)
				seg.Lock.Unlock()
				pool.versionsMu.Lock()
				delete(pool.versions, k)
				pool.versionsMu.Unlock()
				log.Info(" node ", k, " is offline")
			}
		}
		pool.heartBeatMu.Unlock()
		time.Sleep(time.Second * 10)
	}
}

func (pool *ResourcePool) GPUModelToPower(model string) int {
	mapper := map[string]int{
		"K40": 1, "Tesla K40": 1,
		"K80": 2, "Tesla K80": 2,
		"P100": 3, "Tesla P100": 3,
	}
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

/* save pool status periodically */
func (pool *ResourcePool) saveStatusHistory() {
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
		nodesCount := 0

		start := pool.pools[0].Next
		for cur := start; ; {
			cur.Lock.Lock()
			for _, node := range cur.Nodes {
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
			nodesCount += len(cur.Nodes)
			cur.Lock.Unlock()
			cur = cur.Next
			if cur.ID == start.ID {
				break
			}
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

		pool.TotalGPU = TotalGPU
		time.Sleep(time.Second * 60)
	}
}

/* update node info */
func (pool *ResourcePool) update(node NodeStatus) {
	segID := pool.getNodePool(node.ClientID)
	seg := &pool.pools[segID]
	if seg.Nodes == nil {
		seg = seg.Next
	}
	seg.Lock.Lock()
	defer seg.Lock.Unlock()

	/* init bindings */
	go func(node NodeStatus) {
		pool.bindingsMu.Lock()
		defer pool.bindingsMu.Unlock()
		for _, gpu := range node.Status {
			if _, ok := pool.bindings[gpu.UUID]; ok {
				if len(pool.bindings[gpu.UUID]) == 1 {
					pool.utils[gpu.UUID] = append(pool.utils[gpu.UUID],
						UtilGPUTimeSeries{Time: (int)(time.Now().Unix()), Util: gpu.UtilizationGPU})
				}
			}
		}
		pool.heartBeatMu.Lock()
		pool.heartBeat[node.ClientID] = time.Now()
		pool.heartBeatMu.Unlock()
	}(node)

	pool.counterTotal++
	pool.versionsMu.Lock()
	if version, ok := pool.versions[node.ClientID]; ok && version == node.Version {
		pool.versionsMu.Unlock()
		return
	}
	pool.versionsMu.Unlock()
	pool.counter++
	log.Debug(node.Version, "!=", pool.versions[node.ClientID])

	status, ok := seg.Nodes[node.ClientID]
	if ok {
		/* remain allocation info */
		for i, GPU := range status.Status {
			if GPU.UUID == node.Status[i].UUID {
				node.Status[i].MemoryAllocated = GPU.MemoryAllocated
			}
		}
	}
	seg.Nodes[node.ClientID] = &node
	if len(seg.Nodes) > 10 {
		pool.scaleSeg(seg)
	}
	pool.versions[node.ClientID] = node.Version
}

/* spilt seg */
func (pool *ResourcePool) scaleSeg(seg *PoolSeg) {
	log.Info("Scaling seg ", seg.ID)
	go func() {
		pool.poolsMu.Lock()
		defer pool.poolsMu.Unlock()

		var candidate *PoolSeg
		seg.Lock.Lock()

		/* find previous seg */
		var pre *PoolSeg
		for i := seg.ID + pool.poolsCount - 1; i >= 0; i-- {
			if pool.pools[i%pool.poolsCount].Next != seg {
				pre = &pool.pools[i%pool.poolsCount]
				break
			}
		}

		step := seg.ID - pre.ID
		if step < 0 {
			step += pool.poolsCount
		}

		/* find seg in the nearest middle */
		minDistance := step
		for i := 1; i < step; i++ {
			if !pool.pools[(i+pre.ID)%pool.poolsCount].IsVirtual {
				distance := i - step/2
				if distance < 0 {
					distance = -distance
				}
				if candidate == nil || distance < minDistance {
					candidate = &pool.pools[i]
					minDistance = distance
				}
			}
		}

		/* update Next */
		if candidate != nil {
			distance := candidate.ID - seg.ID
			if distance < 0 {
				distance = -distance
			}
			for i := 0; i < distance; i++ {
				pool.pools[(i+pre.ID)%pool.poolsCount].Lock.Lock()
				pool.pools[(i+pre.ID)%pool.poolsCount].Next = candidate
				pool.pools[(i+pre.ID)%pool.poolsCount].Lock.Unlock()
			}
			candidate.Lock.Lock()
			candidate.Next = seg
			/* move nodes */
			nodesToMove := map[string]*NodeStatus{}
			for _, node := range seg.Nodes {
				seg2ID := pool.getNodePool(node.ClientID)
				seg2 := &pool.pools[seg2ID]
				if seg2.Nodes == nil {
					seg2 = seg2.Next
				}
				if seg2 != seg {
					nodesToMove[node.ClientID] = node
				}
			}
			for _, node := range nodesToMove {
				delete(seg.Nodes, node.ClientID)
			}
			candidate.Nodes = nodesToMove
			candidate.Lock.Unlock()
		}
		seg.Lock.Unlock()
	}()
}

/* get node by ClientID */
func (pool *ResourcePool) getByID(id string) NodeStatus {
	poolID := pool.getNodePool(id)
	seg := &pool.pools[poolID]
	if seg.Nodes == nil {
		seg = seg.Next
	}
	seg.Lock.Lock()
	defer seg.Lock.Unlock()

	status, ok := seg.Nodes[id]
	if ok {
		return *status
	}
	return NodeStatus{}
}

/* get all nodes */
func (pool *ResourcePool) list() MsgResource {
	nodes := map[string]NodeStatus{}

	start := pool.pools[0].Next
	for cur := start; ; {
		log.Info(cur.ID)
		cur.Lock.Lock()
		for k, node := range cur.Nodes {
			nodes[k] = *node
		}
		cur.Lock.Unlock()
		cur = cur.Next
		if cur == start {
			break
		}
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
		break
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
		pool.bindings[GPU] = map[string]int{}
	}
	pool.bindings[GPU][job] = int(time.Now().Unix())

	if _, ok := pool.utils[GPU]; !ok {
		pool.utils[GPU] = []UtilGPUTimeSeries{}
	}
}

func (pool *ResourcePool) detach(GPU string, jobName string) {
	pool.bindingsMu.Lock()
	defer pool.bindingsMu.Unlock()
	if _, ok := pool.bindings[GPU]; ok {
		if len(pool.bindings[GPU]) == 1 {
			InstanceOfOptimizer().feed(jobName, pool.utils[GPU])
			pool.utils[GPU] = []UtilGPUTimeSeries{}
		}
	}

	if list, ok := pool.bindings[GPU]; ok {
		delete(list, jobName)
	}
}

func (pool *ResourcePool) getBindings() map[string]map[string]int {
	return pool.bindings
}

func (pool *ResourcePool) pickNode(candidates []*NodeStatus, availableGPUs map[string][]GPUStatus, task Task, job Job, nodes []NodeStatus) *NodeStatus {

	/* shuffle */
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for n := len(candidates); n > 0; n-- {
		randIndex := r.Intn(n)
		candidates[n-1], candidates[randIndex] = candidates[randIndex], candidates[n-1]
	}

	/* sort */
	// single node, single GPU
	sort.Slice(candidates, func(a, b int) bool {
		diffA := pool.GPUModelToPower(candidates[a].Status[0].ProductName) - pool.GPUModelToPower(task.ModelGPU)
		diffB := pool.GPUModelToPower(candidates[b].Status[0].ProductName) - pool.GPUModelToPower(task.ModelGPU)

		if diffA > 0 && diffB >= 0 && diffA > diffB {
			return false //b
		}
		if diffA < 0 && diffB < 0 && diffA > diffB {
			return false
		}
		if diffA < 0 && diffB >= 0 {
			return false
		}
		if diffA == diffB {
			if len(availableGPUs[candidates[a].ClientID]) == len(availableGPUs[candidates[b].ClientID]) {
				return candidates[a].UtilCPU > candidates[b].UtilCPU
			}
			return len(availableGPUs[candidates[a].ClientID]) < len(availableGPUs[candidates[b].ClientID])
		}
		return true //a
	})

	var t []*NodeStatus
	bestGPU := candidates[0].Status[0].ProductName
	for _, node := range candidates {
		if node.Status[0].ProductName != bestGPU {
			break
		}
		t = append(t, node)
	}
	candidates = t

	if (len(job.Tasks) == 1) && task.NumberGPU > 1 { //single node, multi GPUs
		sort.Slice(candidates, func(a, b int) bool {
			if len(availableGPUs[candidates[a].ClientID]) == len(availableGPUs[candidates[b].ClientID]) {
				return candidates[a].UtilCPU > candidates[b].UtilCPU
			}
			return len(availableGPUs[candidates[a].ClientID]) < len(availableGPUs[candidates[b].ClientID])
		})
	}

	if len(job.Tasks) > 1 { //multi nodes, multi GPUs
		sort.Slice(candidates, func(a, b int) bool {
			distanceA := 0
			distanceB := 0
			for _, node := range nodes {
				if node.Rack != candidates[a].Rack {
					distanceA += 10
				}
				if node.ClientID != candidates[a].ClientID {
					distanceA += 1
				}
				if node.Rack != candidates[b].Rack {
					distanceB += 10
				}
				if node.ClientID != candidates[b].ClientID {
					distanceB += 1
				}
			}
			if distanceA == distanceB {
				return len(availableGPUs[candidates[a].ClientID]) > len(availableGPUs[candidates[b].ClientID])
			}
			return distanceA*job.Locality < distanceB*job.Locality
		})
	}

	return candidates[0]
}
