package main

import (
	"sync"
	"time"
	log "github.com/sirupsen/logrus"
	"math"
	"sort"
)

type SchedulerFair struct {
	history   []*Job
	historyMu sync.Mutex

	jobs     map[string]*JobManager
	queues   map[string][]Job
	queuesMu sync.Mutex

	queuesQuota   map[string]*ResourceCount
	queuesQuotaMu sync.Mutex

	schedulingJobs map[string]bool
	schedulingMu   sync.Mutex

	resourceAllocations   map[string]*ResourceCount
	resourceAllocationsMu sync.Mutex

	enabled     bool
	parallelism int

	allocatingGPU   int
	allocatingGPUMu sync.Mutex

	totalQuota     ResourceCount
	allocatedQuota ResourceCount
	quotaMu        sync.Mutex
}

func (scheduler *SchedulerFair) Start() {
	log.Info("JS (fairness) started")

	scheduler.jobs = map[string]*JobManager{}
	scheduler.history = []*Job{}
	scheduler.queues = map[string][]Job{}
	scheduler.queues["default"] = []Job{}
	scheduler.queuesQuota = map[string]*ResourceCount{}
	scheduler.resourceAllocations = map[string]*ResourceCount{}
	scheduler.enabled = true
	scheduler.schedulingJobs = map[string]bool{}
	scheduler.allocatingGPU = 0

	scheduler.parallelism = 1

	go func() {
		flag := true
		for {
			log.Debug("Scheduling")
			if !flag { /* no more job */
				time.Sleep(time.Millisecond * 100)
			}
			flag = false
			if !scheduler.enabled {
				time.Sleep(time.Millisecond * 100)
				continue
			}
			scheduler.schedulingMu.Lock()
			if len(scheduler.schedulingJobs) >= scheduler.parallelism {
				scheduler.schedulingMu.Unlock()
				time.Sleep(time.Millisecond * 100)
				continue
			}
			scheduler.schedulingMu.Unlock()

			scheduler.queuesMu.Lock()
			scheduler.queuesQuotaMu.Lock()

			go func() {
				scheduler.UpdateQuota()
			}()

			bestQueue := ""
			numberGPU := math.MaxInt64
			numberCPU := math.MaxInt64
			/* phase 1 */
			for queue, jobs := range scheduler.queues {
				/* find smallest job */
				if len(jobs) > 0 {
					numberGPUtmp := 0
					numberCPUtmp := 0
					for _, task := range jobs[0].Tasks {
						numberGPUtmp += task.NumberGPU
						numberCPUtmp += task.NumberCPU
					}
					if quota, ok := scheduler.queuesQuota[queue]; !ok || quota.NumberGPU < numberGPUtmp || quota.CPU < numberCPUtmp {
						continue
					}
					if bestQueue == "" || numberGPUtmp < numberGPU || (numberGPUtmp == numberGPU && numberCPUtmp < numberCPU) {
						bestQueue = queue
						numberGPU = numberGPUtmp
						numberCPU = numberCPUtmp
					}
				}
			}

			/* phase 2 */
			if bestQueue == "" {

			}

			/* launch that job */
			if bestQueue != "" {
				numberGPUtmp := 0
				numberCPUtmp := 0
				for _, task := range scheduler.queues[bestQueue][0].Tasks {
					numberGPUtmp += task.NumberGPU
					numberCPUtmp += task.NumberCPU
				}

				log.Info("schedulingJobs are ", scheduler.schedulingJobs)
				pool := InstanceOfResourcePool()
				/* Make sure resource it enough */
				if len(scheduler.schedulingJobs) == 0 || (numberGPUtmp*10+(scheduler.allocatingGPU)*13 <= (pool.TotalGPU-pool.UsingGPU)*10) {
					flag = true

					log.Info("Before, ", scheduler.queuesQuota[bestQueue])
					if quota, ok := scheduler.queuesQuota[bestQueue]; ok {
						quota.NumberGPU -= numberGPUtmp
						quota.CPU -= numberCPUtmp
					}
					log.Info("After, ", scheduler.queuesQuota[bestQueue])

					scheduler.allocatingGPUMu.Lock()
					scheduler.allocatingGPU += numberGPUtmp
					scheduler.allocatingGPUMu.Unlock()
					log.Info("allocatingGPU is ", scheduler.allocatingGPU)

					jm := JobManager{}
					jm.job = scheduler.queues[bestQueue][0]
					jm.scheduler = scheduler
					jm.job.Status = Starting

					scheduler.jobs[jm.job.Name] = &jm
					scheduler.queues[bestQueue] = scheduler.queues[bestQueue][1:]

					scheduler.historyMu.Lock()
					scheduler.history = append(scheduler.history, &jm.job)
					scheduler.historyMu.Unlock()

					scheduler.schedulingMu.Lock()
					scheduler.schedulingJobs[jm.job.Name] = true
					scheduler.schedulingMu.Unlock()
					go func() {
						jm.start()
					}()
				}
			} else {
				log.Debug("No more jobs to scheduling ", time.Now())
			}

			scheduler.queuesQuotaMu.Unlock()
			scheduler.queuesMu.Unlock()
		}
	}()
}

func (scheduler *SchedulerFair) UpdateProgress(job Job, state State) {
	scheduler.historyMu.Lock()
	defer scheduler.historyMu.Unlock()

	scheduler.schedulingMu.Lock()
	delete(scheduler.schedulingJobs, job.Name)
	scheduler.schedulingMu.Unlock()

	switch state {
	case Running:
		for i := range scheduler.history {
			if scheduler.history[i].Name == job.Name {
				scheduler.history[i].Status = Running
				scheduler.history[i].UpdatedAt = int(time.Now().Unix())
			}
		}
		break
	case Finished:
		for i := range scheduler.history {
			if scheduler.history[i].Name == job.Name {
				scheduler.history[i].Status = Finished
				scheduler.history[i].UpdatedAt = int(time.Now().Unix())
			}
		}
		break
	case Stopped:
		for i := range scheduler.history {
			if scheduler.history[i].Name == job.Name {
				scheduler.history[i].Status = Stopped
				scheduler.history[i].UpdatedAt = int(time.Now().Unix())
			}
		}
		break
	case Failed:
		for i := range scheduler.history {
			if scheduler.history[i].Name == job.Name {
				scheduler.history[i].Status = Failed
				scheduler.history[i].UpdatedAt = int(time.Now().Unix())
			}
		}
		break
	}
}

func (scheduler *SchedulerFair) Schedule(job Job) {
	scheduler.queuesMu.Lock()
	defer scheduler.queuesMu.Unlock()

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

func (scheduler *SchedulerFair) AcquireResource(job Job) []NodeStatus {
	res := InstanceOfResourcePool().acquireResource(job)
	if len(res) != 0 {
		for _, task := range job.Tasks {

			scheduler.allocatingGPUMu.Lock()
			scheduler.allocatingGPU -= task.NumberGPU
			scheduler.allocatingGPUMu.Unlock()
		}
		log.Info("allocatingGPU is ", scheduler.allocatingGPU)

		go func(nodes []NodeStatus) {
			for _, node := range nodes {
				scheduler.resourceAllocationsMu.Lock()
				if _, ok := scheduler.resourceAllocations[job.Group]; !ok {
					scheduler.resourceAllocations[job.Group] = &ResourceCount{}
				}
				cnt, _ := scheduler.resourceAllocations[job.Group]
				cnt.CPU += node.MemTotal
				cnt.Memory += node.NumCPU
				for _, v := range node.Status {
					cnt.NumberGPU ++
					cnt.MemoryGPU += v.MemoryTotal
				}
				scheduler.resourceAllocationsMu.Unlock()
			}

		}(res)
	}
	return res
}

func (scheduler *SchedulerFair) ReleaseResource(job Job, agent NodeStatus) {
	InstanceOfResourcePool().releaseResource(job, agent)
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
	}(agent)
	scheduler.UpdateQuota()
}

/* allocate quota to queues */
func (scheduler *SchedulerFair) UpdateQuota() {
	scheduler.queuesMu.Lock()
	defer scheduler.queuesMu.Unlock()
	scheduler.queuesQuotaMu.Lock()
	defer scheduler.queuesQuotaMu.Unlock()
	scheduler.quotaMu.Lock()
	defer scheduler.quotaMu.Unlock()
	log.Info("Updating queues quota~")

	usingGPU := 0
	scheduler.resourceAllocationsMu.Lock()
	for _, queue := range scheduler.resourceAllocations {
		usingGPU += queue.NumberGPU
	}
	scheduler.resourceAllocationsMu.Unlock()

	pool := InstanceOfResourcePool()

	available := pool.TotalGPU - usingGPU
	log.Info("Can allocate ", available)
	log.Info("Before ")
	for _, quota := range scheduler.queuesQuota {
		log.Info("GPU:", quota.NumberGPU)
		log.Info("CPU:", quota.CPU)
		log.Info("Memory:", quota.Memory)
	}
	per := available / len(scheduler.queues)
	for queue := range scheduler.queues {
		if _, ok := scheduler.queuesQuota[queue]; !ok {
			scheduler.queuesQuota[queue] = &ResourceCount{}
		}
		quota := scheduler.queuesQuota[queue]
		quota.NumberGPU += per
		available -= per
	}
	if available > 0 {
		for queue := range scheduler.queues {
			quota := scheduler.queuesQuota[queue]
			quota.NumberGPU += available
			break
		}
	}
	log.Info("After ")
	for _, quota := range scheduler.queuesQuota {
		log.Info("GPU:", quota.NumberGPU)
		log.Info("CPU:", quota.CPU)
		log.Info("Memory:", quota.Memory)
	}
}

func (scheduler *SchedulerFair) QueryState(jobName string) MsgJobStatus {
	scheduler.queuesMu.Lock()
	jm, ok := scheduler.jobs[jobName]
	scheduler.queuesMu.Unlock()
	if !ok {
		return MsgJobStatus{Code: 1, Error: "Job not exist!"}
	}
	return jm.status()
}

func (scheduler *SchedulerFair) Stop(jobName string) MsgStop {
	scheduler.queuesMu.Lock()
	jm, ok := scheduler.jobs[jobName]
	scheduler.queuesMu.Unlock()
	if !ok {
		return MsgStop{Code: 1, Error: "Job not exist!"}
	}
	return jm.stop(true)
}

func (scheduler *SchedulerFair) QueryLogs(jobName string, taskName string) MsgLog {
	scheduler.queuesMu.Lock()
	jm, ok := scheduler.jobs[jobName]
	scheduler.queuesMu.Unlock()
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

	scheduler.queuesMu.Lock()
	for _, v := range scheduler.queues {
		tmp = append(tmp, v...)
	}
	scheduler.queuesMu.Unlock()

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

func (scheduler *SchedulerFair) Enable() bool {
	scheduler.enabled = true
	log.Info("scheduler is enabled ", time.Now())
	return true
}

func (scheduler *SchedulerFair) Disable() bool {
	scheduler.enabled = false
	log.Info("scheduler is disabled ", time.Now())
	return true
}

func (scheduler *SchedulerFair) UpdateParallelism(parallelism int) bool {
	scheduler.parallelism = parallelism
	log.Info("parallelism is updated to ", parallelism)
	return true
}

func (scheduler *SchedulerFair) updateGroup(group Group) bool {
	return true
}
