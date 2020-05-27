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

var resourcePoolInstance *ResourcePool
var resourcePoolInstanceLock sync.Mutex

func InstanceOfResourcePool() *ResourcePool {
	defer resourcePoolInstanceLock.Unlock()
	resourcePoolInstanceLock.Lock()

	if resourcePoolInstance == nil {
		resourcePoolInstance = &ResourcePool{}
	}
	return resourcePoolInstance
}

type ResourcePool struct {
	poolsCount int
	pools      []PoolSeg
	poolsMu    sync.Mutex

	history []PoolStatus

	heartBeat    map[string]time.Time
	heartBeatMu  sync.Mutex
	versions     map[string]float64
	versionsMu   sync.Mutex
	counter      int
	counterTotal int

	subscriptions   map[string]map[string]int
	subscriptionsMu sync.Mutex

	networks     map[string]bool
	networksFree map[string]bool
	networkMu    sync.Mutex

	bindings   map[string]map[string]int
	bindingsMu sync.Mutex
	utils      map[string][]UtilGPUTimeSeries

	TotalGPU   int
	TotalGPUMu sync.Mutex
	UsingGPU   int
	UsingGPUMu sync.Mutex

	enableShare            bool
	enableShareRatio       float64
	enablePreSchedule      bool
	enablePreScheduleRatio float64
}

func (pool *ResourcePool) init(conf Configuration) {
	log.Info("RM started ")

	pool.networks = map[string]bool{}
	pool.networksFree = map[string]bool{}

	pool.bindings = map[string]map[string]int{}
	pool.utils = map[string][]UtilGPUTimeSeries{}

	pool.TotalGPU = 0
	pool.UsingGPU = 0

	pool.enableShare = true
	pool.enableShareRatio = 0.75
	pool.enablePreSchedule = true
	pool.enablePreScheduleRatio = 0.95

	/* init pools */
	pool.poolsCount = 300
	for i := 0; i < pool.poolsCount; i++ {
		pool.pools = append(pool.pools, PoolSeg{Lock: sync.Mutex{}, ID: i})
	}
	/* generate working segs */
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

	pool.versions = map[string]float64{}
	pool.subscriptions = map[string]map[string]int{}
	pool.heartBeat = map[string]time.Time{}
	go func() {
		pool.checkDeadNodes()
	}()

	pool.history = []PoolStatus{}
	go func() {
		pool.saveStatusHistory()
	}()
}

/* check dead nodes periodically */
func (pool *ResourcePool) checkDeadNodes() {
	for {
		pool.heartBeatMu.Lock()
		var nodesToDel []string
		for k, v := range pool.heartBeat {
			if v.Add(time.Second * 30).Before(time.Now()) {
				segID := pool.getNodePool(k)
				seg := &pool.pools[segID]
				if seg.Nodes == nil {
					seg = seg.Next
				}

				seg.Lock.Lock()
				pool.TotalGPUMu.Lock()
				if _, ok := seg.Nodes[k]; ok {
					pool.TotalGPU -= len(seg.Nodes[k].Status)
				}
				pool.TotalGPUMu.Unlock()
				delete(seg.Nodes, k)
				seg.Lock.Unlock()
				pool.versionsMu.Lock()
				delete(pool.versions, k)
				pool.versionsMu.Unlock()
				nodesToDel = append(nodesToDel, k)
				log.Info(" node ", k, " is offline")
			}
		}
		for _, v := range nodesToDel {
			segID := pool.getNodePool(v)
			seg := &pool.pools[segID]
			if seg.Nodes == nil {
				seg = seg.Next
			}
			seg.Lock.Lock()
			delete(seg.Nodes, v)
			seg.Lock.Unlock()
			delete(pool.heartBeat, v)
		}
		pool.heartBeatMu.Unlock()
		time.Sleep(time.Second * 10)
	}
}

func (pool *ResourcePool) GPUModelToPower(model string) int {
	mapper := map[string]int{
		"K40": 2, "Tesla K40": 2,
		"K80": 3, "Tesla K80": 3,
		"P100": 4, "Tesla P100": 4,
	}
	if power, err := mapper[model]; !err {
		return power
	}
	return 1
}

func (pool *ResourcePool) getNodePool(name string) int {
	h := fnv.New32a()
	h.Write([]byte(name))
	return int(h.Sum32()) % pool.poolsCount
}

/* save pool status periodically */
func (pool *ResourcePool) saveStatusHistory() {
	/* waiting for nodes */
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

		start := pool.pools[0]
		if start.Nodes == nil {
			start = *start.Next
		}
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
			cur = *cur.Next
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

		pool.TotalGPUMu.Lock()
		pool.TotalGPU = TotalGPU
		pool.TotalGPUMu.Unlock()
		time.Sleep(time.Second * 60)
	}
}

/* update node info */
func (pool *ResourcePool) update(node NodeStatus) {
	pool.poolsMu.Lock()
	defer pool.poolsMu.Unlock()
	segID := pool.getNodePool(node.ClientID)
	seg := &pool.pools[segID]
	if seg.Nodes == nil {
		seg = seg.Next
	}
	seg.Lock.Lock()
	defer seg.Lock.Unlock()

	/* init bindings */
	go func(node NodeStatus) {
		pool.subscriptionsMu.Lock()
		defer pool.subscriptionsMu.Unlock()
		pool.bindingsMu.Lock()
		defer pool.bindingsMu.Unlock()
		for _, gpu := range node.Status {
			if _, ok := pool.bindings[gpu.UUID]; ok {
				if _, ok2 := pool.utils[gpu.UUID]; ok2 {
					pool.utils[gpu.UUID] = append(pool.utils[gpu.UUID],
						UtilGPUTimeSeries{Time: (int)(time.Now().Unix()), Util: gpu.UtilizationGPU})
				}
			}

			if _, ok := pool.subscriptions[gpu.UUID]; ok {
				for jobName := range pool.subscriptions[gpu.UUID] {
					go func(name string) {
						/* ask to update job status */
						scheduler.QueryState(name)
					}(jobName)
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
		//pool.versionsMu.Unlock()
		//return
	}
	pool.versionsMu.Unlock()
	pool.counter++
	log.Debug(node.Version, "!=", pool.versions[node.ClientID])

	status, ok := seg.Nodes[node.ClientID]
	if ok {
		/* keep allocation info */
		for i, GPU := range status.Status {
			if GPU.UUID == node.Status[i].UUID {
				node.Status[i].MemoryAllocated = GPU.MemoryAllocated
			}
		}
	} else {
		/* TODO: double check node do belong to this seg */
		pool.TotalGPUMu.Lock()
		pool.TotalGPU += len(node.Status)
		pool.TotalGPUMu.Unlock()
		log.Info("node ", node.ClientID, " is online")
	}
	seg.Nodes[node.ClientID] = &node
	if len(seg.Nodes) > 10 {
		go func() {
			pool.scaleSeg(seg)
		}()
	}
	pool.versions[node.ClientID] = node.Version
}

/* spilt seg */
func (pool *ResourcePool) scaleSeg(seg *PoolSeg) {
	log.Info("Scaling seg ", seg.ID)

	pool.poolsMu.Lock()
	defer pool.poolsMu.Unlock()

	var segIDs []int
	segIDs = append(segIDs, seg.ID)

	/* find previous seg */
	var pre *PoolSeg
	for i := seg.ID + pool.poolsCount - 1; i >= 0; i-- {
		segIDs = append(segIDs, i%pool.poolsCount)
		if pool.pools[i%pool.poolsCount].Next.ID != seg.ID {
			break
		}
		pre = &pool.pools[i%pool.poolsCount]
	}

	distance := seg.ID - pre.ID
	if distance < 0 {
		distance += pool.poolsCount
	}
	if distance <= 1 {
		log.Warn("Unable to scale, ", seg.ID, ", already full")
		return
	}

	candidate := pre
	/* walk to the nearest middle */
	if pre.ID < seg.ID {
		candidate = &pool.pools[(pre.ID+seg.ID)/2]
	} else {
		candidate = &pool.pools[(pre.ID+seg.ID+pool.poolsCount)/2%pool.poolsCount]
	}
	candidate.Next = seg
	candidate.Nodes = map[string]*NodeStatus{}

	/* lock in asc sequence to avoid deadlock */
	sort.Ints(segIDs)
	for _, id := range segIDs {
		pool.pools[id].Lock.Lock()
	}
	//log.Println(segIDs)

	/* update Next */
	for i := 0; ; i++ {
		id := (pre.ID + i) % pool.poolsCount
		if id == candidate.ID {
			break
		}
		pool.pools[id].Next = candidate
	}

	/* move nodes */
	nodesToMove := map[string]*NodeStatus{}
	for _, node := range seg.Nodes {
		seg2ID := pool.getNodePool(node.ClientID)
		seg2 := &pool.pools[seg2ID]
		if seg2.Nodes == nil {
			seg2 = seg2.Next
		}
		if seg2.ID != seg.ID {
			nodesToMove[node.ClientID] = node
		}
	}
	for _, node := range nodesToMove {
		delete(seg.Nodes, node.ClientID)
	}
	candidate.Nodes = nodesToMove
	//log.Info("pre=", pre.ID, " active=", candidate.ID, " seg=", seg.ID)
	for _, id := range segIDs {
		pool.pools[id].Lock.Unlock()
	}
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

	start := pool.pools[0]
	if start.Nodes == nil {
		start = *start.Next
	}
	for cur := start; ; {
		cur.Lock.Lock()
		for k, node := range cur.Nodes {
			nodes[k] = *node
		}
		cur.Lock.Unlock()
		cur = *cur.Next
		if cur.ID == start.ID {
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
			resp.Body.Close()
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
	pool.subscriptionsMu.Lock()
	defer pool.subscriptionsMu.Unlock()
	pool.bindingsMu.Lock()
	defer pool.bindingsMu.Unlock()

	if _, ok := pool.subscriptions[GPU]; !ok {
		pool.subscriptions[GPU] = map[string]int{}
	}
	pool.subscriptions[GPU][job] = int(time.Now().Unix())

	if _, ok := pool.bindings[GPU]; !ok {
		pool.bindings[GPU] = map[string]int{}
	}
	pool.bindings[GPU][job] = int(time.Now().Unix())

	if _, ok := pool.utils[GPU]; !ok {
		pool.utils[GPU] = []UtilGPUTimeSeries{}
	}

	if len(pool.bindings[GPU]) > 1 {
		delete(pool.utils, GPU)
	}
}

func (pool *ResourcePool) detach(GPU string, job Job) {
	pool.subscriptionsMu.Lock()
	defer pool.subscriptionsMu.Unlock()
	pool.bindingsMu.Lock()
	defer pool.bindingsMu.Unlock()

	if _, ok := pool.subscriptions[GPU]; ok {
		delete(pool.subscriptions[GPU], job.Name)
	}

	if _, ok := pool.bindings[GPU]; ok {
		if _, ok2 := pool.utils[GPU]; ok2 {
			if len(pool.bindings[GPU]) == 1 && job.Status == Finished {
				InstanceOfOptimizer().feed(job.Name, pool.utils[GPU])
			}
			delete(pool.utils, GPU)
		}
	}

	if list, ok := pool.bindings[GPU]; ok {
		delete(list, job.Name)
	}
}

/* return free & using GPUs */
func (pool *ResourcePool) countGPU() (int, int) {
	return pool.TotalGPU - pool.UsingGPU, pool.UsingGPU
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

func (pool *ResourcePool) acquireResource(job Job) []NodeStatus {
	if len(job.Tasks) == 0 {
		return []NodeStatus{}
	}
	task := job.Tasks[0]
	segID := rand.Intn(pool.poolsCount)
	if pool.TotalGPU < 100 {
		segID = 0
	}
	start := &pool.pools[segID]
	if start.Nodes == nil {
		start = start.Next
	}

	locks := map[int]*sync.Mutex{}

	allocationType := 0

	var candidates []NodeStatus

	if pool.TotalGPU == 0 {
		return []NodeStatus{}
	}
	loadRatio := float64(pool.UsingGPU) / float64(pool.TotalGPU)

	/* first, choose sharable GPUs */
	if pool.enableShare && len(job.Tasks) == 1 && task.NumberGPU == 1 && loadRatio >= pool.enableShareRatio {
		// check sharable
		allocationType = 1
		if util, valid := InstanceOfOptimizer().predictUtilGPU(job.Name); valid {

			for cur := start; ; {
				if _, ok := locks[cur.ID]; !ok {
					cur.Lock.Lock()
					locks[cur.ID] = &cur.Lock
				}

				for _, node := range cur.Nodes {
					var available []GPUStatus
					for _, status := range node.Status {
						if status.MemoryAllocated > 0 && status.MemoryTotal > task.MemoryGPU+status.MemoryAllocated {

							if jobs, ok := pool.bindings[status.UUID]; ok {
								totalUtil := util
								for job := range jobs {
									if utilT, ok := InstanceOfOptimizer().predictUtilGPU(job); ok {
										totalUtil += utilT
									} else {
										totalUtil += 100
									}
								}
								if totalUtil < 100 {
									available = append(available, status)
								}
							}
						}
					}
					if len(available) >= task.NumberGPU {
						candidates = append(candidates, *node)
						if len(candidates) >= len(job.Tasks)*3+5 {
							break
						}
					}
				}
				if len(candidates) >= len(job.Tasks)*3+5 {
					break
				}
				if cur.ID > cur.Next.ID {
					break
				}
				cur = cur.Next
			}
		}
		//log.Info(candidates)
	}

	/* second round, find vacant gpu */
	if len(candidates) == 0 {
		allocationType = 2
		for cur := start; ; {
			if _, ok := locks[cur.ID]; !ok {
				cur.Lock.Lock()
				locks[cur.ID] = &cur.Lock
			}
			for _, node := range cur.Nodes {
				var available []GPUStatus
				for _, status := range node.Status {
					/* make sure GPU is not used by in-system and outer-system */
					if status.MemoryAllocated == 0 && status.MemoryUsed < 100 {
						available = append(available, status)
					}
				}
				if len(available) >= task.NumberGPU {
					candidates = append(candidates, *node)
					if len(candidates) >= len(job.Tasks)*3+5 {
						break
					}
				}
			}
			if len(candidates) >= len(job.Tasks)*3+5 {
				break
			}
			if cur.ID > cur.Next.ID {
				break
			}
			cur = cur.Next
		}
		//log.Info(candidates)
	}

	/* third round, find gpu to be released */
	if len(candidates) == 0 && len(job.Tasks) == 1 && task.NumberGPU == 1 && pool.enablePreSchedule {
		estimate, valid := InstanceOfOptimizer().predictTime(job.Name)

		if loadRatio >= pool.enablePreScheduleRatio && valid {
			allocationType = 3
			for cur := start; ; {
				if _, ok := locks[cur.ID]; !ok {
					cur.Lock.Lock()
					locks[cur.ID] = &cur.Lock
				}
				for _, node := range cur.Nodes {
					var available []GPUStatus
					for _, status := range node.Status {
						bindings := pool.getBindings()
						if tasks, ok := bindings[status.UUID]; ok {
							if len(tasks) > 1 || status.MemoryAllocated == 0 {
								continue
							}
							for taskT, s := range tasks {
								est, valid2 := InstanceOfOptimizer().predictTime(taskT)
								if valid2 {
									now := (int)(time.Now().Unix())
									log.Info(s, now, estimate, est)
									if now-s > est.Total-est.Post-estimate.Pre-15 {
										available = append(available, status)
									}
								}
							}
						}
					}
					if len(available) >= task.NumberGPU {
						candidates = append(candidates, *node)
						if len(candidates) >= len(job.Tasks)*3+5 {
							break
						}
					}
				}
				if len(candidates) >= len(job.Tasks)*3+5 {
					break
				}
				if cur.ID > cur.Next.ID {
					break
				}
				cur = cur.Next
			}
			//log.Info(candidates)
		}
	}

	if len(candidates) > 0 {
		log.Info("allocationType is ", allocationType)
		//log.Info(candidates)
	}

	/* assign */
	var ress []NodeStatus
	if len(candidates) > 0 {
		var nodesT []NodeStatus
		for _, node := range candidates {
			nodesT = append(nodesT, node.Copy())
		}

		tasks := make([]Task, len(job.Tasks))
		var tasksPS []Task
		var tasksWorker []Task
		for _, taskT := range job.Tasks {
			if taskT.IsPS {
				tasksPS = append(tasksPS, taskT)
			} else {
				tasksWorker = append(tasksWorker, taskT)
			}
		}
		idxPS := 0
		idxWorker := 0
		factor := float64(len(tasksWorker)) / (float64(len(tasksPS)) + 0.001)
		for i := range tasks {
			if float64(idxPS)*factor <= float64(idxWorker) && idxPS < len(tasksPS) {
				tasks[i] = tasksPS[idxPS]
				idxPS++
			} else if idxWorker < len(tasksWorker) {
				tasks[i] = tasksWorker[idxWorker]
				idxWorker++
			} else {
				tasks[i] = tasksPS[idxPS]
				idxPS++
			}
		}

		allocation := fastBestFit(nodesT, tasks)
		if allocation.Flags["valid"] {

			for range job.Tasks { //append would cause uncertain order
				ress = append(ress, NodeStatus{ClientID: "null"})
			}

			for nodeID, tasks := range allocation.TasksOnNode {
				var node *NodeStatus
				for i := range candidates {
					if candidates[i].ClientID == nodeID {
						node = &candidates[i]
					}
				}

				var available []GPUStatus
				for _, gpu := range node.Status {
					if gpu.MemoryAllocated == 0 {
						available = append(available, gpu)
					}
				}
				for _, task := range tasks {
					res := NodeStatus{}
					res.ClientID = node.ClientID
					res.ClientHost = node.ClientHost
					res.NumCPU = task.NumberCPU
					res.MemTotal = task.Memory
					res.Status = available[0:task.NumberGPU]
					available = available[task.NumberGPU:]

					for i := range res.Status {
						for j := range node.Status {
							if res.Status[i].UUID == node.Status[j].UUID {
								if node.Status[j].MemoryAllocated == 0 {
									pool.UsingGPUMu.Lock()
									pool.UsingGPU ++
									pool.UsingGPUMu.Unlock()
								}
								node.Status[j].MemoryAllocated += task.MemoryGPU
								res.Status[i].MemoryTotal = task.MemoryGPU
							}
						}
					}
					for _, t := range res.Status {
						pool.attach(t.UUID, job.Name)
					}

					for i := range job.Tasks {
						if job.Tasks[i].Name == task.Name {
							ress[i] = res
						}
					}

				}
			}

		}
	}

	for segID, lock := range locks {
		log.Debug("Unlock ", segID)
		lock.Unlock()
	}
	return ress
}

func (pool *ResourcePool) releaseResource(job Job, agent NodeStatus) {
	segID := pool.getNodePool(agent.ClientID)
	seg := pool.pools[segID]
	if seg.Nodes == nil {
		seg = *seg.Next
	}
	seg.Lock.Lock()
	defer seg.Lock.Unlock()

	node, ok := seg.Nodes[agent.ClientID]
	if !ok {
		/* in case node is offline */
		/* TODO, update usingTotalGPU correctly */
		return
	}
	for _, gpu := range agent.Status {
		for j := range node.Status {
			if gpu.UUID == node.Status[j].UUID {
				node.Status[j].MemoryAllocated -= gpu.MemoryTotal
				if node.Status[j].MemoryAllocated < 0 {
					// in case of error
					log.Warn(node.ClientID, "More Memory Allocated")
					node.Status[j].MemoryAllocated = 0
				}
				if node.Status[j].MemoryAllocated == 0 {
					pool.UsingGPUMu.Lock()
					pool.UsingGPU--
					pool.UsingGPUMu.Unlock()
					log.Info(node.Status[j].UUID, " is released")
				}
				//log.Info(node.Status[j].MemoryAllocated)
			}
		}
	}
}

func (pool *ResourcePool) SetShareRatio(ratio float64) bool {
	pool.enableShareRatio = ratio
	log.Info("enableShareRatio is updated to ", ratio)
	return true
}

func (pool *ResourcePool) SetPreScheduleRatio(ratio float64) bool {
	pool.enablePreScheduleRatio = ratio
	log.Info("enablePreScheduleRatio is updated to ", ratio)
	return true
}
