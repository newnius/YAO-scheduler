package main

import (
	"sync"
	"time"
	"log"
	"io/ioutil"
	"encoding/json"
)

type AllocatorFIFO struct {
	queue []Job
	mu    sync.Mutex
	scheduling sync.Mutex
}

func (allocator *AllocatorFIFO) start() {
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

func (allocator *AllocatorFIFO) ack() {
	allocator.scheduling.Unlock()
}

func (allocator *AllocatorFIFO) schedule(job Job) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	allocator.queue = append(allocator.queue, job)
}

func (allocator *AllocatorFIFO) requestResource(task Task) MsgAgent {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	res := MsgAgent{}
	for id, node := range pool.nodes {
		var available []NodeStatus
		for _, status := range node {
			if status.MemoryAllocated == 0 {
				available = append(available, status)
			}
		}
		if len(available) >= task.NumberGPU {
			res.ClientID = id
			res.Status = available[0:task.NumberGPU]

			for i := range res.Status {
				for j := range node {
					if res.Status[i].UUID == node[j].UUID {
						node[j].MemoryAllocated = task.MemoryGPU
					}
				}
			}
		}
	}
	return res
}

func (allocator *AllocatorFIFO) returnResource(agent MsgAgent) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	nodes := pool.nodes[agent.ClientID]
	for _, gpu := range agent.Status {
		for j := range nodes {
			if gpu.UUID == nodes[j].UUID {
				nodes[j].MemoryAllocated = 0
			}
		}
	}
}

func (allocator *AllocatorFIFO) status(jobName string) MsgJobStatus {

	var tasksStatus []TaskStatus
	tasksStatus = append(tasksStatus, TaskStatus{Id: "8b9b665fc4f1"})
	tasksStatus = append(tasksStatus, TaskStatus{Id: "4a4aeee2c5f9"})

	for i, taskStatus := range tasksStatus {
		spider := Spider{}
		spider.Method = "GET"
		spider.URL = "http://kafka_node1:8000/status?id=" + taskStatus.Id

		err := spider.do()
		if err != nil {
			continue
		}

		resp := spider.getResponse()
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			continue
		}

		var res MsgTaskStatus
		err = json.Unmarshal([]byte(string(body)), &res)
		if err != nil {
			continue
		}
		tasksStatus[i] = res.Status
	}

	return MsgJobStatus{Status: tasksStatus}
}

func (allocator *AllocatorFIFO) logs(taskName string) MsgLog {
	spider := Spider{}
	spider.Method = "GET"
	spider.URL = "http://kafka_node1:8000/logs?id=" + taskName

	err := spider.do()
	if err != nil {
		return MsgLog{Code: 1, Error: err.Error()}
	}

	resp := spider.getResponse()
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return MsgLog{Code: 1, Error: err.Error()}
	}

	var res MsgLog
	err = json.Unmarshal([]byte(string(body)), &res)
	if err != nil {
		log.Println(err)
		return MsgLog{Code: 1, Error: "Unknown"}
	}
	return res
}
