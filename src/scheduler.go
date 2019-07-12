package main

type Scheduler interface {
	Start()

	Schedule(Job)

	UpdateProgress(jobName string, state State)

	AcquireResource(Task) NodeStatus

	ReleaseResource(NodeStatus)

	AcquireNetwork() string

	ReleaseNetwork(network string)

	QueryState(jobName string) MsgJobStatus

	QueryLogs(jobName string, taskName string) MsgLog

	Stop(jobName string) MsgStop

	ListJobs() MsgJobList

	Summary() MsgSummary
}