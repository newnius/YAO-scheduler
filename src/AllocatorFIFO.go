package main

import (
	"sync"
	"time"
)

type AllocatorFIFO struct {
	history    []*Job
	queue      []Job
	mu         sync.Mutex
	scheduling sync.Mutex

	jobs map[string]*JobManager
}

func (allocator *AllocatorFIFO) start() {
	allocator.jobs = map[string]*JobManager{}
	allocator.history = []*Job{}

	go func() {
		for {
			//fmt.Print("Scheduling ")
			time.Sleep(time.Second * 5)
			allocator.scheduling.Lock()
			allocator.mu.Lock()
			if len(allocator.queue) > 0 {

				jm := JobManager{}
				jm.job = allocator.queue[0]
				allocator.queue = allocator.queue[1:]
				jm.allocator = allocator
				allocator.jobs[jm.job.Name] = &jm

				for i := range allocator.history {
					if allocator.history[i].Name == jm.job.Name {
						allocator.history[i].Status = Starting
					}
				}

				go func() {
					jm.start()
				}()
			} else {
				allocator.scheduling.Unlock()
			}
			allocator.mu.Unlock()
		}
	}()
}

func (allocator *AllocatorFIFO) ack(job *Job) {
	allocator.scheduling.Unlock()
}

func (allocator *AllocatorFIFO) running(job *Job) {
	for i := range allocator.history {
		if allocator.history[i].Name == job.Name {
			allocator.history[i].Status = Running
		}
	}
}

func (allocator *AllocatorFIFO) finish(job *Job) {
	for i := range allocator.history {
		if allocator.history[i].Name == job.Name {
			allocator.history[i].Status = Finished
		}
	}
}

func (allocator *AllocatorFIFO) schedule(job Job) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	allocator.queue = append(allocator.queue, job)
	allocator.history = append(allocator.history, &job)
}

func (allocator *AllocatorFIFO) requestResource(task Task) NodeStatus {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	res := NodeStatus{}
	for id, node := range pool.nodes {
		var available []GPUStatus
		for _, status := range node.Status {
			if status.MemoryAllocated == 0 {
				available = append(available, status)
			}
		}
		if len(available) >= task.NumberGPU {
			res.ClientID = id
			res.ClientHost = node.ClientHost
			res.Status = available[0:task.NumberGPU]

			for i := range res.Status {
				for j := range node.Status {
					if res.Status[i].UUID == node.Status[j].UUID {
						node.Status[j].MemoryAllocated = task.MemoryGPU
					}
				}
			}
		}
	}
	return res
}

func (allocator *AllocatorFIFO) returnResource(agent NodeStatus) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	nodes := pool.nodes[agent.ClientID]
	for _, gpu := range agent.Status {
		for j := range nodes.Status {
			if gpu.UUID == nodes.Status[j].UUID {
				nodes.Status[j].MemoryAllocated = 0
			}
		}
	}
}

func (allocator *AllocatorFIFO) status(jobName string) MsgJobStatus {
	jm, ok := allocator.jobs[jobName]
	if !ok {
		return MsgJobStatus{Code: 1, Error: "Job not exist!"}
	}
	return jm.status()
}

func (allocator *AllocatorFIFO) logs(jobName string, taskName string) MsgLog {
	jm, ok := allocator.jobs[jobName]
	if !ok {
		return MsgLog{Code: 1, Error: "Job not exist!"}
	}
	return jm.logs(taskName)
}

func (allocator *AllocatorFIFO) listJobs() MsgJobList {
	return MsgJobList{Code: 0, Jobs: allocator.history}
}

func (allocator *AllocatorFIFO) summary() MsgSummary {
	summary := MsgSummary{}
	summary.Code = 0

	finishedJobsCounter := 0
	runningJobsCounter := 0
	pendingJobsCounter := 0

	for _, job := range allocator.history {
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
