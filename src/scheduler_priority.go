package main

import (
	"sync"
	"time"
	log "github.com/sirupsen/logrus"
)

type SchedulerPriority struct {
	history    []*Job
	queue      []Job
	mu         sync.Mutex
	scheduling sync.Mutex

	jobs        map[string]*JobManager
	enabled     bool
	parallelism int
}

func (scheduler *SchedulerPriority) Start() {
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

func (scheduler *SchedulerPriority) UpdateProgress(job Job, state State) {
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

func (scheduler *SchedulerPriority) Schedule(job Job) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()

	index := 0

	left := 0
	right := len(scheduler.queue) - 1
	for ; left <= right; {
		mid := (left + right) / 2
		if scheduler.queue[left].Priority < job.Priority {
			index = left
			break
		}
		if scheduler.queue[right].Priority >= job.Priority {
			index = right + 1
			break
		}
		if scheduler.queue[mid].Priority >= job.Priority {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	scheduler.queue = append(scheduler.queue, Job{})

	copy(scheduler.queue[index+1:], scheduler.queue[index:])
	scheduler.queue[index] = job

	job.Status = Created
}

func (scheduler *SchedulerPriority) AcquireResource(job Job) []NodeStatus {
	res := InstanceOfResourcePool().acquireResource(job)
	return res
}

func (scheduler *SchedulerPriority) ReleaseResource(job Job, agent NodeStatus) {
	InstanceOfResourcePool().releaseResource(job, agent)
}

func (scheduler *SchedulerPriority) QueryState(jobName string) MsgJobStatus {
	jm, ok := scheduler.jobs[jobName]
	if !ok {
		return MsgJobStatus{Code: 1, Error: "Job not exist!"}
	}
	return jm.status()
}

func (scheduler *SchedulerPriority) Stop(jobName string) MsgStop {
	jm, ok := scheduler.jobs[jobName]
	if !ok {
		return MsgStop{Code: 1, Error: "Job not exist!"}
	}
	return jm.stop(true)
}

func (scheduler *SchedulerPriority) QueryLogs(jobName string, taskName string) MsgLog {
	jm, ok := scheduler.jobs[jobName]
	if !ok {
		return MsgLog{Code: 1, Error: "Job not exist!"}
	}
	return jm.logs(taskName)
}

func (scheduler *SchedulerPriority) ListJobs() MsgJobList {
	var tmp []Job
	for _, job := range scheduler.history {
		tmp = append(tmp, *job)
	}
	tmp = append(tmp, scheduler.queue...)
	return MsgJobList{Code: 0, Jobs: tmp}
}

func (scheduler *SchedulerPriority) Summary() MsgSummary {
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
			break
		case Finished:
			finishedJobsCounter++
		case Stopped:
			finishedJobsCounter++
		}
	}
	summary.JobsFinished = finishedJobsCounter
	summary.JobsPending = pendingJobsCounter
	summary.JobsRunning = runningJobsCounter

	summary.FreeGPU, summary.UsingGPU = InstanceOfResourcePool().countGPU()
	return summary
}

func (scheduler *SchedulerPriority) Enable() bool {
	scheduler.enabled = true
	return true
}

func (scheduler *SchedulerPriority) Disable() bool {
	scheduler.enabled = false
	return true
}

func (scheduler *SchedulerPriority) UpdateParallelism(parallelism int) bool {
	scheduler.parallelism = parallelism
	log.Info("parallelism is updated to", parallelism)
	return true
}

func (scheduler *SchedulerPriority) updateGroup(group Group) bool {
	return true
}

func (scheduler *SchedulerPriority) DebugDump() map[string]interface{} {
	res := map[string]interface{}{}
	return res
}
