package main

import (
	"time"
	"log"
	"net/url"
	"strings"
	"io/ioutil"
	"encoding/json"
)

type JobManager struct {
	allocator *AllocatorFIFO
	job       Job
	jobStatus JobStatus
	resources []MsgAgent
}

func (jm *JobManager) start() {
	log.Println("start job ", jm.job.Name)
	jm.jobStatus = JobStatus{Name: jm.job.Name, tasks: map[string]TaskStatus{}}

	/* request for resources */
	for i := range jm.job.Tasks {
		var resource MsgAgent
		for {
			resource = jm.allocator.requestResource(jm.job.Tasks[i])
			if len(resource.Status) > 0 {
				break
			}
		}
		log.Println("Receive resource", resource)
		jm.resources = append(jm.resources, resource)
	}

	/* bring up containers */
	for i := range jm.job.Tasks {
		v := url.Values{}
		v.Set("image", jm.job.Image)
		v.Set("cmd", jm.job.Tasks[i].Cmd)
		resp, err := doRequest("POST", "http://kafka_node1:8000/create", strings.NewReader(v.Encode()), "application/x-www-form-urlencoded", "")
		if err != nil {
			log.Println(err)
			return
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			return
		}

		log.Println(string(body))

		var res MsgCreate
		err = json.Unmarshal([]byte(string(body)), &res)
		if err != nil {
			log.Println(err)
			return
		}

		jm.jobStatus.tasks[jm.job.Tasks[i].Name] = TaskStatus{Id: res.Id}
	}

	jm.allocator.ack(&jm.job)

	/* monitor job execution */
	for {
		res := jm.status()
		flag := false
		for i := range res.Status {
			if res.Status[i].Status == "running" {
				log.Println(jm.job.Name, "-", i, " is running")
				flag = true
			} else {
				log.Println(jm.job.Name, "-", i, " ", res.Status[i].Status)

				/* save logs etc. */

				/* return resource */
				jm.allocator.returnResource(jm.resources[i])
			}
		}
		if !flag {
			break
		}
		time.Sleep(time.Second * 10)
	}

	jm.allocator.finish(&jm.job)
	log.Println("finish job", jm.job.Name)
}

func (jm *JobManager) logs(taskName string) MsgLog {
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

func (jm *JobManager) status() MsgJobStatus {
	var tasksStatus []TaskStatus
	for _, taskStatus := range jm.jobStatus.tasks {
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
		tasksStatus = append(tasksStatus, res.Status)
	}

	return MsgJobStatus{Status: tasksStatus}
}
