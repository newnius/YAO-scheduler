package main

import (
	"time"
	"net/url"
	"strings"
	"io/ioutil"
	"encoding/json"
	"strconv"
	log "github.com/sirupsen/logrus"
)

type JobManager struct {
	scheduler  Scheduler
	job        Job
	jobStatus  JobStatus
	resources  []NodeStatus
	killedFlag bool
}

func (jm *JobManager) start() {
	log.Info("start job ", jm.job.Name, time.Now())
	jm.jobStatus = JobStatus{Name: jm.job.Name, tasks: map[string]TaskStatus{}}

	network := jm.scheduler.AcquireNetwork()

	InstanceJobHistoryLogger().submitJob(jm.job)

	/* request for resources */
	for i := range jm.job.Tasks {
		var resource NodeStatus
		for {
			if jm.killedFlag {
				break
			}
			resource = jm.scheduler.AcquireResource(jm.job, jm.job.Tasks[i])
			if len(resource.Status) > 0 {
				break
			}
			time.Sleep(time.Second * 1)
		}
		log.Info("Receive resource", resource)
		jm.resources = append(jm.resources, resource)

		for _, t := range resource.Status {
			jm.scheduler.Attach(t.UUID, jm.job.Name)
		}

	}
	jm.scheduler.UpdateProgress(jm.job.Name, Running)

	log.Info("ready to run job ", jm.job.Name, time.Now())

	/* bring up containers */
	for i := range jm.job.Tasks {
		if jm.killedFlag {
			break
		}
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
		v.Set("should_wait", "1")

		resp, err := doRequest("POST", "http://"+jm.resources[i].ClientHost+":8000/create", strings.NewReader(v.Encode()), "application/x-www-form-urlencoded", "")
		if err != nil {
			log.Warn(err.Error())
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Warn(err)
			continue
		}

		var res MsgCreate
		err = json.Unmarshal([]byte(string(body)), &res)
		if err != nil {
			log.Warn(err)
			continue
		}

		jm.jobStatus.tasks[jm.job.Tasks[i].Name] = TaskStatus{Id: res.Id, Node: jm.resources[i].ClientHost}
	}

	/* monitor job execution */
	for {
		res := jm.status()
		flag := false
		onlyPS := true
		for i := range res.Status {
			if res.Status[i].Status == "ready" {
				log.Debug(jm.job.Name, "-", i, " is ready to run")
				flag = true
			} else if res.Status[i].Status == "running" {
				log.Debug(jm.job.Name, "-", i, " is running")
				flag = true
				if !jm.job.Tasks[i].IsPS {
					onlyPS = false
				}
				InstanceJobHistoryLogger().submitTaskStatus(jm.job.Name, res.Status[i])
			} else {
				log.Info(jm.job.Name, "-", i, " ", res.Status[i].Status)
				/* save logs etc. */

				/* remove exited containers */
				//v := url.Values{}
				//v.Set("id", res.Status[i].Id)
				//
				//_, err := doRequest("POST", "http://"+res.Status[i].Node+":8000/remove", strings.NewReader(v.Encode()), "application/x-www-form-urlencoded", "")
				//if err != nil {
				//	log.Warn(err.Error())
				//	continue
				//}

				/* return resource */
				jm.scheduler.ReleaseResource(jm.job, jm.resources[i])
				log.Info("return resource ", jm.resources[i].ClientID)

				for _, t := range jm.resources[i].Status {
					jm.scheduler.Detach(t.UUID, jm.job.Name)
				}

				InstanceJobHistoryLogger().submitTaskStatus(jm.job.Name, res.Status[i])
			}
		}
		if onlyPS {
			jm.stop()
			log.Info("Only PS is running, stop", jm.job.Name)
			jm.killedFlag = false
			break
		}
		if !flag {
			break
		}
		time.Sleep(time.Second * 10)
	}

	jm.scheduler.ReleaseNetwork(network)

	if !jm.killedFlag {
		jm.scheduler.UpdateProgress(jm.job.Name, Finished)
		log.Info("finish job", jm.job.Name)
	}
}

func (jm *JobManager) logs(taskName string) MsgLog {
	spider := Spider{}
	spider.Method = "GET"
	spider.URL = "http://" + jm.jobStatus.tasks[taskName].Node + ":8000/logs?id=" + jm.jobStatus.tasks[taskName].Id

	if _, ok := jm.jobStatus.tasks[taskName]; !ok {
		return MsgLog{Code: -1, Error: "Task not exist"}
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
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		var res MsgTaskStatus
		err = json.Unmarshal([]byte(string(body)), &res)
		if err != nil {
			continue
		}
		res.Status.Node = taskStatus.Node
		tasksStatus = append(tasksStatus, res.Status)
	}

	return MsgJobStatus{Status: tasksStatus}
}

func (jm *JobManager) stop() MsgStop {
	jm.killedFlag = true
	go func() { /* kill at background */
		for _, taskStatus := range jm.jobStatus.tasks {
			v := url.Values{}
			v.Set("id", taskStatus.Id)

			_, err := doRequest("POST", "http://"+taskStatus.Node+":8000/stop", strings.NewReader(v.Encode()), "application/x-www-form-urlencoded", "")
			if err != nil {
				log.Warn(err.Error())
				continue
			}
		}
	}()

	jm.scheduler.UpdateProgress(jm.job.Name, Stopped)
	log.Info("kill job, ", jm.job.Name)
	return MsgStop{Code: 0}
}
