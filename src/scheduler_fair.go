package main

import (
	"sync"
	"time"
	log "github.com/sirupsen/logrus"
	"sort"
	"math/rand"
)

type ResourceCount struct {
	NumberGPU int
	MemoryGPU int
	CPU       int
	Memory    int
}

type SchedulerFair struct {
	history               []*Job
	historyMu             sync.Mutex
	queues                map[string][]Job
	queueMu               sync.Mutex
	schedulingMu          sync.Mutex
	schedulingJobsCnt     int
	jobs                  map[string]*JobManager
	nextQueue             string
	resourceAllocations   map[string]*ResourceCount
	resourceAllocationsMu sync.Mutex
	enabled               bool
	parallelism           int

	enableShare            bool
	enableShareRatio       float64
	enablePreSchedule      bool
	enablePreScheduleRatio float64

	UsingGPU int
}

type FairJobSorter []Job

func (s FairJobSorter) Len() int {
	return len(s)
}
func (s FairJobSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s FairJobSorter) Less(i, j int) bool {
	return s[i].CreatedAt < s[j].CreatedAt
}

func (scheduler *SchedulerFair) Start() {
	scheduler.jobs = map[string]*JobManager{}
	scheduler.history = []*Job{}
	scheduler.nextQueue = "default"
	scheduler.queues = map[string][]Job{}
	scheduler.queues["default"] = []Job{}
	scheduler.resourceAllocations = map[string]*ResourceCount{}
	scheduler.enabled = true
	scheduler.schedulingJobsCnt = 0

	scheduler.enableShare = true
	scheduler.enableShareRatio = 0.75
	scheduler.enablePreSchedule = true
	scheduler.enablePreScheduleRatio = 0.95

	scheduler.UsingGPU = 0

	scheduler.parallelism = 1

	go func() {
		for {
			log.Debug("Scheduling")
			if !scheduler.enabled {
				time.Sleep(time.Millisecond * 100)
				continue
			}
			scheduler.schedulingMu.Lock()
			if scheduler.schedulingJobsCnt >= scheduler.parallelism {
				scheduler.schedulingMu.Unlock()
				time.Sleep(time.Millisecond * 100)
				continue
			}
			scheduler.schedulingJobsCnt++
			scheduler.schedulingMu.Unlock()
			scheduler.queueMu.Lock()
			queue := scheduler.nextQueue
			if len(scheduler.queues[queue]) > 0 {
				jm := JobManager{}
				jm.job = scheduler.queues[queue][0]

				scheduler.queues[queue] = scheduler.queues[queue][1:]
				jm.scheduler = scheduler
				scheduler.jobs[jm.job.Name] = &jm

				jm.job.Status = Starting
				scheduler.historyMu.Lock()
				scheduler.history = append(scheduler.history, &jm.job)
				scheduler.historyMu.Unlock()

				go func() {
					jm.start()
				}()
			} else {
				log.Debug("No more jobs to scheduling", time.Now())
				scheduler.schedulingMu.Lock()
				scheduler.schedulingJobsCnt--
				scheduler.schedulingMu.Unlock()
				time.Sleep(time.Millisecond * 100)
				go func() {
					scheduler.UpdateNextQueue()
				}()
			}
			scheduler.queueMu.Unlock()
		}
	}()
}

func (scheduler *SchedulerFair) UpdateProgress(jobName string, state State) {
	scheduler.historyMu.Lock()
	defer scheduler.historyMu.Unlock()

	switch state {
	case Running:
		scheduler.schedulingMu.Lock()
		scheduler.schedulingJobsCnt--
		scheduler.schedulingMu.Unlock()

		for i := range scheduler.history {
			if scheduler.history[i].Name == jobName {
				scheduler.history[i].Status = Running
				scheduler.history[i].UpdatedAt = int(time.Now().Unix())
			}
		}
		break
	case Finished:
		for i := range scheduler.history {
			if scheduler.history[i].Name == jobName {
				scheduler.history[i].Status = Finished
				scheduler.history[i].UpdatedAt = int(time.Now().Unix())
			}
		}
		break
	case Stopped:
		for i := range scheduler.history {
			if scheduler.history[i].Name == jobName {
				scheduler.history[i].Status = Stopped
				scheduler.history[i].UpdatedAt = int(time.Now().Unix())
			}
		}
		break
	}
}

func (scheduler *SchedulerFair) Schedule(job Job) {
	scheduler.queueMu.Lock()
	defer scheduler.queueMu.Unlock()

	queue := job.Group
	_, ok := scheduler.queues[queue]
	if !ok {
		if InstanceOfGroupManager().get(queue) != nil {
			scheduler.queues[queue] = []Job{}
		} else {
			queue = "default"
		}
	}

	index := 0
	left := 0
	right := len(scheduler.queues[queue]) - 1
	for ; left <= right; {
		mid := (left + right) / 2
		if scheduler.queues[queue][left].Priority < job.Priority {
			index = left
			break
		}
		if scheduler.queues[queue][right].Priority >= job.Priority {
			index = right + 1
			break
		}
		if scheduler.queues[queue][mid].Priority >= job.Priority {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	scheduler.queues[queue] = append(scheduler.queues[queue], Job{})

	copy(scheduler.queues[queue][index+1:], scheduler.queues[queue][index:])
	scheduler.queues[queue][index] = job

	job.Status = Created
}

func (scheduler *SchedulerFair) AcquireResource(job Job, task Task) NodeStatus {
	poolID := rand.Intn(pool.poolsCount)
	res := NodeStatus{}

	locks := map[int]sync.Mutex{}

	allocationType := 0
	availableGPUs := map[string][]GPUStatus{}

	var candidates []NodeStatus

	/* first, choose sharable GPUs */
	if scheduler.enableShare && (pool.TotalGPU != 0 && float64(scheduler.UsingGPU)/float64(pool.TotalGPU) >= scheduler.enableShareRatio) {
		// check sharable
		allocationType = 1
		if util, valid := InstanceOfOptimizer().predictUtilGPU(job.Name); valid {

			for i := 0; i < pool.poolsCount; i++ {
				if _, ok := locks[(i+poolID)%pool.poolsCount]; !ok {
					pool.poolsMu[(i+poolID)%pool.poolsCount].Lock()
					locks[(i+poolID)%pool.poolsCount] = pool.poolsMu[(i+poolID)%pool.poolsCount]
				}

				for _, node := range pool.pools[(i+poolID)%pool.poolsCount] {
					var available []GPUStatus
					for _, status := range node.Status {
						if status.MemoryTotal > task.MemoryGPU+status.MemoryAllocated && status.MemoryFree > task.MemoryGPU {

							if jobs, ok := pool.bindings[status.UUID]; ok {
								totalUtil := util
								for job := range jobs {
									if utilT, ok := InstanceOfOptimizer().predictUtilGPU(job); ok {
										totalUtil += utilT
									}
								}
								if totalUtil < 100 {
									available = append(available, status)
									availableGPUs[node.ClientID] = available
								}
							}
						}
					}
					if len(available) >= task.NumberGPU {
						candidates = append(candidates, node)
						if len(candidates) >= 8 {
							break
						}
					}
				}
				if len(candidates) >= 8 {
					break
				}
			}
		}
		log.Info(candidates)
	}

	/* second round, find vacant gpu */
	if len(candidates) == 0 {
		allocationType = 2
		for i := 0; i < pool.poolsCount; i++ {
			if _, ok := locks[(i+poolID)%pool.poolsCount]; !ok {
				pool.poolsMu[(i+poolID)%pool.poolsCount].Lock()
				locks[(i+poolID)%pool.poolsCount] = pool.poolsMu[(i+poolID)%pool.poolsCount]
			}
			for _, node := range pool.pools[(i+poolID)%pool.poolsCount] {
				var available []GPUStatus
				for _, status := range node.Status {
					if status.MemoryAllocated == 0 && status.MemoryUsed < 10 {
						available = append(available, status)
					}
				}
				if len(available) >= task.NumberGPU {
					candidates = append(candidates, node)
					availableGPUs[node.ClientID] = available
					if len(candidates) >= 8 {
						break
					}
				}
			}
			if len(candidates) >= 8 {
				break
			}
		}
		log.Info(candidates)
	}

	/* third round, find gpu to be released */
	if len(candidates) == 0 && len(job.Tasks) == 1 && task.NumberGPU == 1 && scheduler.enablePreSchedule {
		estimate, valid := InstanceOfOptimizer().predictTime(job.Name)

		log.Info(pool.TotalGPU)
		log.Info(estimate, valid)
		log.Info(scheduler.UsingGPU)

		if pool.TotalGPU != 0 && float64(scheduler.UsingGPU)/float64(pool.TotalGPU) >= scheduler.enablePreScheduleRatio && valid {
			allocationType = 3
			for i := 0; i < pool.poolsCount; i++ {
				if _, ok := locks[(i+poolID)%pool.poolsCount]; !ok {
					pool.poolsMu[(i+poolID)%pool.poolsCount].Lock()
					locks[(i+poolID)%pool.poolsCount] = pool.poolsMu[(i+poolID)%pool.poolsCount]
				}
				for _, node := range pool.pools[(i+poolID)%pool.poolsCount] {
					var available []GPUStatus
					for _, status := range node.Status {
						bindings := pool.getBindings()
						if tasks, ok := bindings[status.UUID]; ok {
							if len(tasks) > 1 {
								continue
							}
							for task_t, s := range tasks {
								est, valid2 := InstanceOfOptimizer().predictTime(task_t)
								if valid2 {
									t := s
									now := (int)(time.Now().Unix())
									if now-t > est.Total-est.Post-estimate.Pre && status.MemoryFree > task.MemoryGPU {
										available = append(available, status)
									}
								}
							}
						}
					}
					if len(available) >= task.NumberGPU {
						candidates = append(candidates, node)
						availableGPUs[node.ClientID] = available
						if len(candidates) >= 8 {
							break
						}
					}
				}
				if len(candidates) >= 8 {
					break
				}
			}
			log.Info(candidates)
		}
	}

	log.Info("allocationType is ", allocationType)

	/* assign */
	if len(candidates) > 0 {
		node := candidates[0]
		res.ClientID = node.ClientID
		res.ClientHost = node.ClientHost
		res.Status = availableGPUs[node.ClientID][0:task.NumberGPU]
		res.NumCPU = task.NumberCPU
		res.MemTotal = task.Memory

		for i := range res.Status {
			for j := range node.Status {
				if res.Status[i].UUID == node.Status[j].UUID {
					node.Status[j].MemoryAllocated += task.MemoryGPU
					res.Status[i].MemoryTotal = task.MemoryGPU
				}
			}
		}
	}

	for i := range locks {
		pool.poolsMu[i].Unlock()
	}
	go func(res NodeStatus) {
		if len(res.Status) == 0 {
			return
		}
		scheduler.resourceAllocationsMu.Lock()
		if _, ok := scheduler.resourceAllocations[job.Group]; !ok {
			scheduler.resourceAllocations[job.Group] = &ResourceCount{}
		}
		cnt, _ := scheduler.resourceAllocations[job.Group]
		cnt.CPU += res.MemTotal
		cnt.Memory += res.NumCPU
		for _, v := range res.Status {
			cnt.NumberGPU ++
			cnt.MemoryGPU += v.MemoryTotal
		}
		scheduler.resourceAllocationsMu.Unlock()
		scheduler.UpdateNextQueue()

	}(res)
	return res
}

func (scheduler *SchedulerFair) ReleaseResource(job Job, agent NodeStatus) {
	poolID := pool.getNodePool(agent.ClientID)
	pool.poolsMu[poolID].Lock()
	defer pool.poolsMu[poolID].Unlock()

	node := pool.pools[poolID][agent.ClientID]
	for _, gpu := range agent.Status {
		for j := range node.Status {
			if gpu.UUID == node.Status[j].UUID {
				node.Status[j].MemoryAllocated -= gpu.MemoryTotal
				if node.Status[j].MemoryAllocated < 0 {
					// in case of error
					log.Warn(node.ClientID, "More Memory Allocated")
					node.Status[j].MemoryAllocated = 0
				}
				log.Info(node.Status[j].MemoryAllocated)
			}
		}
	}
	go func(res NodeStatus) {
		scheduler.resourceAllocationsMu.Lock()
		if _, ok := scheduler.resourceAllocations[job.Group]; !ok {
			scheduler.resourceAllocations[job.Group] = &ResourceCount{}
		}
		cnt, _ := scheduler.resourceAllocations[job.Group]
		cnt.CPU -= res.MemTotal
		cnt.Memory -= res.NumCPU
		for _, v := range res.Status {
			cnt.NumberGPU --
			cnt.MemoryGPU -= v.MemoryTotal
		}
		scheduler.resourceAllocationsMu.Unlock()
		scheduler.UpdateNextQueue()
	}(agent)
}

func (scheduler *SchedulerFair) QueryState(jobName string) MsgJobStatus {
	jm, ok := scheduler.jobs[jobName]
	if !ok {
		return MsgJobStatus{Code: 1, Error: "Job not exist!"}
	}
	return jm.status()
}

func (scheduler *SchedulerFair) Stop(jobName string) MsgStop {
	jm, ok := scheduler.jobs[jobName]
	if !ok {
		return MsgStop{Code: 1, Error: "Job not exist!"}
	}
	return jm.stop()
}

func (scheduler *SchedulerFair) QueryLogs(jobName string, taskName string) MsgLog {
	jm, ok := scheduler.jobs[jobName]
	if !ok {
		return MsgLog{Code: 1, Error: "Job not exist!"}
	}
	return jm.logs(taskName)
}

func (scheduler *SchedulerFair) ListJobs() MsgJobList {
	var jobs []Job
	scheduler.historyMu.Lock()
	for _, job := range scheduler.history {
		jobs = append(jobs, *job)
	}
	scheduler.historyMu.Unlock()
	var tmp []Job
	for _, v := range scheduler.queues {
		tmp = append(tmp, v...)
	}
	sort.Sort(FairJobSorter(tmp))
	jobs = append(jobs, tmp...)
	return MsgJobList{Code: 0, Jobs: jobs}
}

func (scheduler *SchedulerFair) Summary() MsgSummary {
	summary := MsgSummary{}
	summary.Code = 0

	finishedJobsCounter := 0
	runningJobsCounter := 0
	pendingJobsCounter := 0

	var tmp []Job
	scheduler.historyMu.Lock()
	for _, job := range scheduler.history {
		tmp = append(tmp, *job)
	}
	scheduler.historyMu.Unlock()
	for _, v := range scheduler.queues {
		tmp = append(tmp, v...)
	}

	for _, job := range tmp {
		switch job.Status {
		case Created:
			pendingJobsCounter++
		case Starting:
			pendingJobsCounter++
			break
		case Running:
			runningJobsCounter++
			break;
		case Finished:
			finishedJobsCounter++
		case Stopped:
			finishedJobsCounter++
		}
	}
	summary.JobsFinished = finishedJobsCounter
	summary.JobsPending = pendingJobsCounter
	summary.JobsRunning = runningJobsCounter

	FreeGPU := 0
	UsingGPU := 0

	for i := 0; i < pool.poolsCount; i++ {
		pool.poolsMu[i].Lock()
		for _, node := range pool.pools[i] {
			for j := range node.Status {
				if node.Status[j].MemoryAllocated == 0 {
					FreeGPU++
				} else {
					UsingGPU++
				}
			}
		}
		pool.poolsMu[i].Unlock()
	}
	summary.FreeGPU = FreeGPU
	summary.UsingGPU = UsingGPU

	return summary
}

func (scheduler *SchedulerFair) AcquireNetwork() string {
	return pool.acquireNetwork()
}

func (scheduler *SchedulerFair) ReleaseNetwork(network string) {
	pool.releaseNetwork(network)
}

func (scheduler *SchedulerFair) UpdateNextQueue() {
	next := "default"
	quota := 9999.0

	NumberGPU := 0.00001
	MemoryGPU := 0.00001
	CPU := 0.00001
	Memory := 0.0001
	for i := 0; i < pool.poolsCount; i++ {
		pool.poolsMu[i].Lock()
		for _, node := range pool.pools[i] {
			CPU += float64(node.NumCPU)
			Memory += float64(node.MemTotal)
			for _, card := range node.Status {
				NumberGPU += 1.0
				MemoryGPU += float64(card.MemoryTotal)
			}
		}
		pool.poolsMu[i].Unlock()
	}

	for k, t := range scheduler.queues {
		if len(t) == 0 {
			continue
		}
		scheduler.resourceAllocationsMu.Lock()
		if _, ok := scheduler.resourceAllocations[k]; !ok {
			scheduler.resourceAllocations[k] = &ResourceCount{}
		}
		v := scheduler.resourceAllocations[k]

		tmp := 0.0
		tmp += float64(v.CPU) / CPU
		tmp += float64(v.Memory) / Memory
		tmp += float64(v.NumberGPU) / NumberGPU
		tmp += float64(v.MemoryGPU) / MemoryGPU
		scheduler.resourceAllocationsMu.Unlock()
		tmp /= 4
		weight := 10
		if g, ok2 := InstanceOfGroupManager().groups[k]; !ok2 {
			weight = g.Weight
		}
		tmp /= float64(weight)
		if tmp < quota {
			quota = tmp
			next = k
		}
	}
	scheduler.nextQueue = next
	log.Debug("updateNextQueue ->", next)
}

func (scheduler *SchedulerFair) Attach(GPU string, job string) {
	pool.attach(GPU, job)
}

func (scheduler *SchedulerFair) Detach(GPU string, job string) {
	pool.detach(GPU, job)
}

func (scheduler *SchedulerFair) Enable() bool {
	scheduler.enabled = true
	log.Info("scheduler is enabled", time.Now())
	return true
}

func (scheduler *SchedulerFair) Disable() bool {
	scheduler.enabled = false
	log.Info("scheduler is disabled", time.Now())
	return true
}

func (scheduler *SchedulerFair) UpdateParallelism(parallelism int) bool {
	scheduler.parallelism = parallelism
	log.Info("parallelism is updated to", parallelism)
	return true
}

func (scheduler *SchedulerFair) SetShareRatio(ratio float64) bool {
	scheduler.enableShareRatio = ratio
	log.Info("enableShareRatio is updated to", ratio)
	return true
}

func (scheduler *SchedulerFair) SetPreScheduleRatio(ratio float64) bool {
	scheduler.enablePreScheduleRatio = ratio
	log.Info("enablePreScheduleRatio is updated to", ratio)
	return true
}
