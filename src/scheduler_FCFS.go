package main

import (
	"sync"
	"time"
	log "github.com/sirupsen/logrus"
	"math/rand"
)

type SchedulerFCFS struct {
	history    []*Job
	queue      []Job
	mu         sync.Mutex
	scheduling sync.Mutex

	jobs        map[string]*JobManager
	enabled     bool
	parallelism int
}

func (scheduler *SchedulerFCFS) Start() {
	scheduler.jobs = map[string]*JobManager{}
	scheduler.history = []*Job{}
	scheduler.enabled = true

	go func() {
		for {
			log.Info("Scheduling")
			time.Sleep(time.Second * 5)
			scheduler.scheduling.Lock()
			scheduler.mu.Lock()
			if len(scheduler.queue) > 0 {

				jm := JobManager{}
				jm.job = scheduler.queue[0]
				scheduler.queue = scheduler.queue[1:]
				jm.scheduler = scheduler
				scheduler.jobs[jm.job.Name] = &jm

				jm.job.Status = Starting
				scheduler.history = append(scheduler.history, &jm.job)

				go func() {
					jm.start()
				}()
			} else {
				scheduler.scheduling.Unlock()
			}
			scheduler.mu.Unlock()
		}
	}()
}

func (scheduler *SchedulerFCFS) UpdateProgress(job Job, state State) {
	switch state {
	case Running:
		scheduler.scheduling.Unlock()

		for i := range scheduler.history {
			if scheduler.history[i].Name == job.Name {
				scheduler.history[i].Status = Running
			}
		}
		break
	case Finished:
		for i := range scheduler.history {
			if scheduler.history[i].Name == job.Name {
				scheduler.history[i].Status = Finished
			}
		}
		break
	case Stopped:
		for i := range scheduler.history {
			if scheduler.history[i].Name == job.Name {
				scheduler.history[i].Status = Stopped
			}
		}
		break
	}
}

func (scheduler *SchedulerFCFS) Schedule(job Job) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()

	scheduler.queue = append(scheduler.queue, job)
	job.Status = Created
}

func (scheduler *SchedulerFCFS) AcquireResource(job Job, task Task, nodes []NodeStatus) NodeStatus {
	segID := rand.Intn(pool.poolsCount)
	seg := &pool.pools[segID]
	if seg.Nodes == nil {
		seg = seg.Next
	}

	res := NodeStatus{}
	for id, node := range seg.Nodes {
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
	return res
}

func (scheduler *SchedulerFCFS) ReleaseResource(job Job, agent NodeStatus) {
	segID := pool.getNodePool(agent.ClientID)
	seg := &pool.pools[segID]
	if seg.Nodes == nil {
		seg = seg.Next
	}
	seg.Lock.Lock()
	defer seg.Lock.Unlock()

	node := seg.Nodes[agent.ClientID]
	for _, gpu := range agent.Status {
		for j := range node.Status {
			if gpu.UUID == node.Status[j].UUID {
				node.Status[j].MemoryAllocated -= gpu.MemoryTotal
				if node.Status[j].MemoryAllocated < 0 {
					// in case of error
					log.Warn(node.ClientID, "More Memory Allocated")
					node.Status[j].MemoryAllocated = 0
				}
			}
		}
	}
}

func (scheduler *SchedulerFCFS) QueryState(jobName string) MsgJobStatus {
	jm, ok := scheduler.jobs[jobName]
	if !ok {
		return MsgJobStatus{Code: 1, Error: "Job not exist!"}
	}
	return jm.status()
}

func (scheduler *SchedulerFCFS) Stop(jobName string) MsgStop {
	jm, ok := scheduler.jobs[jobName]
	if !ok {
		return MsgStop{Code: 1, Error: "Job not exist!"}
	}
	return jm.stop()
}

func (scheduler *SchedulerFCFS) QueryLogs(jobName string, taskName string) MsgLog {
	jm, ok := scheduler.jobs[jobName]
	if !ok {
		return MsgLog{Code: 1, Error: "Job not exist!"}
	}
	return jm.logs(taskName)
}

func (scheduler *SchedulerFCFS) ListJobs() MsgJobList {
	var tmp []Job
	for _, job := range scheduler.history {
		tmp = append(tmp, *job)
	}
	tmp = append(tmp, scheduler.queue...)
	return MsgJobList{Code: 0, Jobs: tmp}
}

func (scheduler *SchedulerFCFS) Summary() MsgSummary {
	summary := MsgSummary{}
	summary.Code = 0

	finishedJobsCounter := 0
	runningJobsCounter := 0
	pendingJobsCounter := 0

	var tmp []Job
	for _, job := range scheduler.history {
		tmp = append(tmp, *job)
	}
	tmp = append(tmp, scheduler.queue...)

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

	start := pool.pools[0].Next
	for cur := start; ; {
		cur.Lock.Lock()
		for _, node := range cur.Nodes {
			for j := range node.Status {
				if node.Status[j].MemoryAllocated == 0 {
					FreeGPU++
				} else {
					UsingGPU++
				}
			}
		}
		cur.Lock.Unlock()
		cur = cur.Next
		if cur == start {
			break
		}
	}
	summary.FreeGPU = FreeGPU
	summary.UsingGPU = UsingGPU

	return summary
}

func (scheduler *SchedulerFCFS) AcquireNetwork() string {
	return pool.acquireNetwork()
}

func (scheduler *SchedulerFCFS) ReleaseNetwork(network string) {
	pool.releaseNetwork(network)
}

func (scheduler *SchedulerFCFS) Attach(GPU string, job string) {
	pool.attach(GPU, job)
}

func (scheduler *SchedulerFCFS) Detach(GPU string, job string) {
	pool.detach(GPU, job)
}

func (scheduler *SchedulerFCFS) Enable() bool {
	scheduler.enabled = true
	return true
}

func (scheduler *SchedulerFCFS) Disable() bool {
	scheduler.enabled = false
	return true
}

func (scheduler *SchedulerFCFS) UpdateParallelism(parallelism int) bool {
	scheduler.parallelism = parallelism
	log.Info("parallelism is updated to", parallelism)
	return true
}

func (scheduler *SchedulerFCFS) SetShareRatio(ratio float64) bool {
	//scheduler.enableShareRatio = ratio
	log.Info("enableShareRatio is updated to", ratio)
	return true
}

func (scheduler *SchedulerFCFS) SetPreScheduleRatio(ratio float64) bool {
	//scheduler.enablePreScheduleRatio = ratio
	log.Info("enablePreScheduleRatio is updated to", ratio)
	return true
}

func (scheduler *SchedulerFCFS) updateGroup(group Group) bool {
	return true
}
