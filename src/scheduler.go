package main

type Scheduler interface {
	Start()

	Schedule(Job)

	UpdateProgress(jobName string, state State)

	AcquireResource(Job, Task) NodeStatus

	ReleaseResource(Job, NodeStatus)

	AcquireNetwork() string

	ReleaseNetwork(network string)

	QueryState(jobName string) MsgJobStatus

	QueryLogs(jobName string, taskName string) MsgLog

	Stop(jobName string) MsgStop

	ListJobs() MsgJobList

	Summary() MsgSummary

	Attach(GPU string, job string)

	Detach(GPU string, job string)

	Enable()

	Disable()
}
