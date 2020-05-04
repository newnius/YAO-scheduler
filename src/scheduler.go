package main

type Scheduler interface {
	Start()

	Schedule(Job)

	UpdateProgress(job Job, state State)

	AcquireResource(Job, Task, []NodeStatus) NodeStatus

	ReleaseResource(Job, NodeStatus)

	AcquireNetwork() string

	ReleaseNetwork(network string)

	QueryState(jobName string) MsgJobStatus

	QueryLogs(jobName string, taskName string) MsgLog

	Stop(jobName string) MsgStop

	ListJobs() MsgJobList

	Summary() MsgSummary

	Attach(GPU string, job string)

	Detach(GPU string, job Job)

	Enable() bool

	Disable() bool

	UpdateParallelism(parallelism int) bool

	SetShareRatio(ratio float64) bool

	SetPreScheduleRatio(ratio float64) bool

	updateGroup(group Group) bool
}
