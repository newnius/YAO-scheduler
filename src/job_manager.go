package main

import (
	"time"
	"net/url"
	"strings"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"strconv"
	log "github.com/sirupsen/logrus"
)

type JobManager struct {
	scheduler Scheduler
	job       Job
	jobStatus JobStatus
	resources []NodeStatus
}

func (jm *JobManager) start() {
	log.Info("start job ", jm.job.Name)
	jm.jobStatus = JobStatus{Name: jm.job.Name, tasks: map[string]TaskStatus{}}

	network := jm.scheduler.AcquireNetwork()

	/* request for resources */
	for i := range jm.job.Tasks {
		var resource NodeStatus
		for {
			resource = jm.scheduler.AcquireResource(jm.job, jm.job.Tasks[i])
			if len(resource.Status) > 0 {
				break
			}
			time.Sleep(time.Second * 1)
		}
		log.Info("Receive resource", resource)
		jm.resources = append(jm.resources, resource)
	}
	jm.scheduler.UpdateProgress(jm.job.Name, Running)

	/* bring up containers */
	for i := range jm.job.Tasks {
		var GPUs []string
		for _, GPU := range jm.resources[i].Status {
			GPUs = append(GPUs, GPU.UUID)
		}

		v := url.Values{}
		v.Set("image", jm.job.Tasks[i].Image)
		v.Set("cmd", jm.job.Tasks[i].Cmd)
		v.Set("name", jm.job.Tasks[i].Name)
		v.Set("workspace", jm.job.Workspace)
		v.Set("gpus", strings.Join(GPUs, ","))
		v.Set("mem_limit", strconv.Itoa(jm.job.Tasks[i].Memory)+"m")
		v.Set("cpu_limit", strconv.Itoa(jm.job.Tasks[i].NumberCPU))
		v.Set("network", network)

		resp, err := doRequest("POST", "http://"+jm.resources[i].ClientHost+":8000/create", strings.NewReader(v.Encode()), "application/x-www-form-urlencoded", "")
		if err != nil {
			log.Warn(err.Error())
			return
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Warn(err)
			return
		}

		var res MsgCreate
		err = json.Unmarshal([]byte(string(body)), &res)
		if err != nil {
			log.Warn(err)
			return
		}

		jm.jobStatus.tasks[jm.job.Tasks[i].Name] = TaskStatus{Id: res.Id, Node: jm.resources[i].ClientHost}
	}

	/* monitor job execution */
	for {
		res := jm.status()
		flag := false
		for i := range res.Status {
			if res.Status[i].Status == "running" {
				log.Info(jm.job.Name, "-", i, " is running")
				flag = true
			} else {
				log.Println(jm.job.Name, "-", i, " ", res.Status[i].Status)

				/* save logs etc. */

				/* return resource */
				jm.scheduler.ReleaseResource(jm.job, jm.resources[i])
				fmt.Println("return resource ", jm.resources[i].ClientID)
			}
		}
		if !flag {
			break
		}
		time.Sleep(time.Second * 10)
	}

	jm.scheduler.ReleaseNetwork(network)

	jm.scheduler.UpdateProgress(jm.job.Name, Finished)
	log.Info("finish job", jm.job.Name)
}

func (jm *JobManager) logs(taskName string) MsgLog {
	spider := Spider{}
	spider.Method = "GET"
	spider.URL = "http://" + jm.jobStatus.tasks[taskName].Node + ":8000/logs?id=" + jm.jobStatus.tasks[taskName].Id

	task, ok := jm.jobStatus.tasks[taskName]
	if !ok {
		log.Warn(taskName, task)
	}

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

func (jm *JobManager) status() MsgJobStatus {
	var tasksStatus []TaskStatus
	for _, taskStatus := range jm.jobStatus.tasks {
		spider := Spider{}
		spider.Method = "GET"
		spider.URL = "http://" + taskStatus.Node + ":8000/status?id=" + taskStatus.Id

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
		tasksStatus = append(tasksStatus, res.Status)
	}

	return MsgJobStatus{Status: tasksStatus}
}

func (jm *JobManager) stop() MsgStop {
	for _, taskStatus := range jm.jobStatus.tasks {
		spider := Spider{}
		spider.Method = "POST"
		spider.URL = "http://" + taskStatus.Node + ":8000/stop?id=" + taskStatus.Id
		spider.do()
	}

	for i := range jm.resources {
		jm.scheduler.ReleaseResource(jm.job, jm.resources[i])
	}
	jm.scheduler.UpdateProgress(jm.job.Name, Stopped)
	return MsgStop{Code: 0}
}
