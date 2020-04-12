package main

import (
	"sync"
	"time"
	log "github.com/sirupsen/logrus"
	"sort"
)

type ResourceCount struct {
	NumberGPU int
	MemoryGPU int
	CPU       int
	Memory    int
}

type SchedulerFair struct {
	history             []*Job
	queues              map[string][]Job
	mu                  sync.Mutex
	scheduling          sync.Mutex
	jobs                map[string]*JobManager
	nextQueue           string
	resourceAllocations map[string]*ResourceCount
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

	go func() {
		for {
			log.Debug("Scheduling")
			time.Sleep(time.Millisecond * 100)
			scheduler.scheduling.Lock()
			scheduler.mu.Lock()
			queue := scheduler.nextQueue
			if len(scheduler.queues[queue]) > 0 {
				jm := JobManager{}
				jm.job = scheduler.queues[queue][0]

				scheduler.queues[queue] = scheduler.queues[queue][1:]
				jm.scheduler = scheduler
				scheduler.jobs[jm.job.Name] = &jm

				jm.job.Status = Starting
				scheduler.history = append(scheduler.history, &jm.job)

				go func() {
					jm.start()
				}()
			} else {
				scheduler.scheduling.Unlock()
				go func() {
					scheduler.UpdateNextQueue()
				}()
			}
			scheduler.mu.Unlock()
		}
	}()
}

func (scheduler *SchedulerFair) UpdateProgress(jobName string, state State) {
	switch state {
	case Running:
		scheduler.scheduling.Unlock()

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
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()

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
	pool.mu.Lock()
	defer pool.mu.Unlock()

	res := NodeStatus{}
	for id, node := range pool.nodes {
		var available []GPUStatus
		for _, status := range node.Status {
			if status.MemoryTotal-status.MemoryAllocated >= task.MemoryGPU {
				available = append(available, status)
			}
		}
		if len(available) >= task.NumberGPU {
			res.ClientID = id
			res.ClientHost = node.ClientHost
			res.Status = available[0:task.NumberGPU]
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
			break
		}
	}
	go func(res NodeStatus) {
		if len(res.Status) == 0 {
			return
		}
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
		scheduler.UpdateNextQueue()

	}(res)
	return res
}

func (scheduler *SchedulerFair) ReleaseResource(job Job, agent NodeStatus) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	nodes := pool.nodes[agent.ClientID]
	for _, gpu := range agent.Status {
		for j := range nodes.Status {
			if gpu.UUID == nodes.Status[j].UUID {
				nodes.Status[j].MemoryAllocated -= gpu.MemoryTotal
			}
		}
	}
	go func(res NodeStatus) {
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
	for _, job := range scheduler.history {
		jobs = append(jobs, *job)
	}
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
	for _, job := range scheduler.history {
		tmp = append(tmp, *job)
	}
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

	for _, node := range pool.nodes {
		for j := range node.Status {
			if node.Status[j].MemoryAllocated == 0 {
				FreeGPU++
			} else {
				UsingGPU++
			}
		}
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
	for _, node := range pool.nodes {
		CPU += float64(node.NumCPU)
		Memory += float64(node.MemTotal)
		for _, card := range node.Status {
			NumberGPU += 1.0
			MemoryGPU += float64(card.MemoryTotal)
		}
	}

	for k, t := range scheduler.queues {
		if len(t) == 0 {
			continue
		}
		if _, ok := scheduler.resourceAllocations[k]; !ok {
			scheduler.resourceAllocations[k] = &ResourceCount{}
		}
		v := scheduler.resourceAllocations[k]

		tmp := 0.0
		tmp += float64(v.CPU) / CPU
		tmp += float64(v.Memory) / Memory
		tmp += float64(v.NumberGPU) / NumberGPU
		tmp += float64(v.MemoryGPU) / MemoryGPU
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