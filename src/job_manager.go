package main

import (
	"time"
	"net/url"
	"strings"
	"io/ioutil"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"sync"
	"strconv"
	"math/rand"
)

type JobManager struct {
	scheduler   Scheduler
	job         Job
	jobStatus   JobStatus
	resources   []NodeStatus
	resourcesMu sync.Mutex
	isRunning   bool
	killFlag    bool

	network string

	stats [][]TaskStatus
}

func (jm *JobManager) start() {
	log.Info("start job ", jm.job.Name, " at ", time.Now())
	jm.isRunning = false
	jm.killFlag = false
	jm.jobStatus = JobStatus{Name: jm.job.Name, tasks: map[string]TaskStatus{}}

	/* register in JHL */
	InstanceJobHistoryLogger().submitJob(jm.job)

	/* request for private network */
	jm.network = InstanceOfResourcePool().acquireNetwork()

	/* request for resources */
	for {
		if jm.killFlag {
			break
		}
		jm.resources = jm.scheduler.AcquireResource(jm.job)
		if len(jm.resources) > 0 {
			log.Info(jm.job.Name, " receive resource", jm.resources)
			break
		}
		/* sleep random Millisecond to avoid deadlock */
		time.Sleep(time.Millisecond * time.Duration(500+rand.Intn(500)))
	}

	if InstanceOfConfiguration().mock {
		jm.scheduler.UpdateProgress(jm.job, Running)
		jm.isRunning = false
		duration := InstanceOfMocker().GetDuration(jm.job, jm.resources)
		log.Info("mock ", jm.job.Name, ", wait ", duration)
		time.Sleep(time.Second * time.Duration(duration))
		jm.returnResource([]TaskStatus{})
		jm.scheduler.UpdateProgress(jm.job, Finished)
		log.Info("JobMaster exited ", jm.job.Name)
		return
	}

	if !jm.killFlag {
		/* switch to Running state */
		jm.scheduler.UpdateProgress(jm.job, Running)

		/* bring up containers */
		wg := sync.WaitGroup{}
		for i := range jm.job.Tasks {
			wg.Add(1)

			go func(index int) {
				defer wg.Done()
				var UUIDs []string
				for _, GPU := range jm.resources[index].Status {
					UUIDs = append(UUIDs, GPU.UUID)

					/* attach to GPUs */
					InstanceOfResourcePool().attach(GPU.UUID, jm.job.Name)
				}
				GPUs := strings.Join(UUIDs, ",")

				v := url.Values{}
				v.Set("image", jm.job.Tasks[index].Image)
				v.Set("cmd", jm.job.Tasks[index].Cmd)
				v.Set("name", jm.job.Tasks[index].Name)
				v.Set("workspace", jm.job.Workspace)
				v.Set("gpus", GPUs)
				v.Set("mem_limit", strconv.Itoa(jm.job.Tasks[index].Memory)+"m")
				v.Set("cpu_limit", strconv.Itoa(jm.job.Tasks[index].NumberCPU))
				v.Set("network", jm.network)
				v.Set("should_wait", "0")
				v.Set("output_dir", "/tmp/")
				v.Set("hdfs_address", "http://192.168.100.104:50070/")
				v.Set("hdfs_dir", "/user/yao/output/"+jm.job.Name)
				v.Set("gpu_mem", strconv.Itoa(jm.job.Tasks[index].MemoryGPU))
				v.Set("dfs_src", "/dfs/yao-jobs/"+jm.job.Name+"/task-"+strconv.Itoa(index))
				v.Set("dfs_dst", "/tmp")

				resp, err := doRequest("POST", "http://"+jm.resources[index].ClientHost+":8000/create", strings.NewReader(v.Encode()), "application/x-www-form-urlencoded", "")
				if err != nil {
					log.Warn(err.Error())
					return
				}

				body, err := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					log.Warn(err)
					return
				}

				var res MsgCreate
				err = json.Unmarshal([]byte(string(body)), &res)
				if err != nil || res.Code != 0 {
					log.Warn(res)
					return
				}
				jm.jobStatus.tasks[jm.job.Tasks[index].Name] = TaskStatus{Id: res.Id, Node: jm.resources[index].ClientHost, HostName: jm.job.Tasks[i].Name}
			}(i)
		}
		wg.Wait()
		jm.isRunning = true
	}

	/* monitor job execution */
	for {
		jm.status()
		if !jm.isRunning {
			break
		}
		time.Sleep(time.Second * 25)
	}

	/* make sure resources are released */
	var stats [][]TaskStatus
	for i, task := range jm.job.Tasks {
		if task.IsPS {
			stats = append(stats, jm.stats[i])
		}
	}
	log.Info(jm.stats)
	log.Info(stats)
	InstanceOfOptimizer().feedStats(jm.job, "PS", stats)
	stats = [][]TaskStatus{}
	for i, task := range jm.job.Tasks {
		if !task.IsPS {
			stats = append(stats, jm.stats[i])
		}
	}
	InstanceOfOptimizer().feedStats(jm.job, "Worker", stats)
	jm.returnResource(jm.status().Status)
	log.Info("JobMaster exited ", jm.job.Name)
}

/* release all resource */
func (jm *JobManager) returnResource(status []TaskStatus) {
	jm.resourcesMu.Lock()
	defer jm.resourcesMu.Unlock()
	/* return resource */
	for i := range jm.resources {
		if jm.resources[i].ClientID == "_released_" {
			continue
		}
		jm.scheduler.ReleaseResource(jm.job, jm.resources[i])
		log.Info("return resource again ", jm.resources[i].ClientID)
		jm.resources[i].ClientID = "_released_"

		for _, t := range jm.resources[i].Status {
			InstanceOfResourcePool().detach(t.UUID, jm.job)
		}

		if !InstanceOfConfiguration().mock {
			InstanceJobHistoryLogger().submitTaskStatus(jm.job.Name, status[i])
		}

		/* remove exited containers */
		//v := url.Values{}
		//v.Set("id", res.Status[i].Id)
		//
		//_, err := doRequest("POST", "http://"+res.Status[i].Node+":8000/remove", strings.NewReader(v.Encode()), "application/x-www-form-urlencoded", "")
		//if err != nil {
		//	log.Warn(err.Error())
		//	continue
		//}
	}
	if jm.network != "" {
		InstanceOfResourcePool().releaseNetwork(jm.network)
		jm.network = ""
	}
}

/* monitor all tasks */
func (jm *JobManager) checkStatus(status []TaskStatus) {
	if !jm.isRunning {
		return
	}
	flagRunning := false
	onlyPS := true
	for i := range status {
		if status[i].Status == "ready" {
			log.Debug(jm.job.Name, "-", i, " is ready to run")
			flagRunning = true
			if !jm.job.Tasks[i].IsPS {
				onlyPS = false
			}
		} else if status[i].Status == "running" {
			log.Debug(jm.job.Name, "-", i, " is running")
			flagRunning = true
			if !jm.job.Tasks[i].IsPS {
				onlyPS = false
			}
			InstanceJobHistoryLogger().submitTaskStatus(jm.job.Name, status[i])
		} else if status[i].Status == "unknown" {
			log.Warn(jm.job.Name, "-", i, " is unknown")
			flagRunning = true
			if !jm.job.Tasks[i].IsPS {
				onlyPS = false
			}
			//InstanceJobHistoryLogger().submitTaskStatus(jm.job.Name, status[i])
		} else {
			jm.resourcesMu.Lock()
			if jm.resources[i].ClientID == "_released_" {
				jm.resourcesMu.Unlock()
				continue
			}
			log.Info(jm.job.Name, "-", i, " ", status[i].Status)
			if exitCode, ok := status[i].State["ExitCode"].(float64); ok && exitCode != 0 && !jm.killFlag {
				log.Warn(jm.job.Name+"-"+jm.job.Tasks[i].Name+" exited unexpected, exitCode=", exitCode)
				jm.stop(false)
				jm.killFlag = true
				jm.scheduler.UpdateProgress(jm.job, Failed)
			} else if !jm.killFlag {
				log.Info("Some instance exited, close others")
				jm.stop(false)
				jm.killFlag = true
				jm.scheduler.UpdateProgress(jm.job, Finished)
			}

			if jm.resources[i].ClientID != "_released_" {
				jm.scheduler.ReleaseResource(jm.job, jm.resources[i])
				log.Info("return resource ", jm.resources[i].ClientID)
				jm.resources[i].ClientID = "_released_"

				for _, t := range jm.resources[i].Status {
					InstanceOfResourcePool().detach(t.UUID, jm.job)
				}
				InstanceJobHistoryLogger().submitTaskStatus(jm.job.Name, status[i])
			}
			jm.resourcesMu.Unlock()
		}
	}
	if flagRunning && onlyPS && !jm.killFlag {
		log.Info("Only PS is running, stop ", jm.job.Name)
		jm.stop(false)
		jm.killFlag = true
		jm.scheduler.UpdateProgress(jm.job, Finished)
	}

	if !flagRunning && !jm.killFlag {
		jm.scheduler.UpdateProgress(jm.job, Finished)
		log.Info("finish job ", jm.job.Name)
	}

	if !flagRunning {
		jm.isRunning = false
		jm.returnResource(status)
	}
}

/* fetch logs of task */
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
		return MsgLog{Code: 2, Error: err.Error()}
	}

	var res MsgLog
	err = json.Unmarshal([]byte(string(body)), &res)
	if err != nil {
		log.Println(err)
		return MsgLog{Code: 3, Error: "Unknown"}
	}
	return res
}

/* fetch job tasks status */
func (jm *JobManager) status() MsgJobStatus {
	var tasksStatus []TaskStatus
	for range jm.job.Tasks { //append would cause uncertain order
		tasksStatus = append(tasksStatus, TaskStatus{})
	}

	for i, task := range jm.job.Tasks {
		taskStatus := jm.jobStatus.tasks[task.Name]

		/* still in launching phase */
		if len(taskStatus.Node) == 0 {
			continue
		}

		spider := Spider{}
		spider.Method = "GET"
		spider.URL = "http://" + taskStatus.Node + ":8000/status?id=" + taskStatus.Id

		err := spider.do()
		if err != nil {
			log.Warn(err)
			tasksStatus[i] = TaskStatus{Status: "unknown", State: map[string]interface{}{"ExitCode": float64(-1)}}
			continue
		}

		resp := spider.getResponse()
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			tasksStatus[i] = TaskStatus{Status: "unknown", State: map[string]interface{}{"ExitCode": float64(-1)}}
			continue
		}

		var res MsgTaskStatus
		err = json.Unmarshal([]byte(string(body)), &res)
		if err != nil {
			log.Warn(err)
			tasksStatus[i] = TaskStatus{Status: "unknown", State: map[string]interface{}{"ExitCode": float64(-1)}}
			continue
		}
		if res.Code == 2 {
			tasksStatus[i] = TaskStatus{Status: "unknown", State: map[string]interface{}{"ExitCode": float64(-2)}}
			log.Warn(res.Error)
			continue
		}
		if res.Code != 0 {
			tasksStatus[i] = TaskStatus{Status: "notexist", State: map[string]interface{}{"ExitCode": float64(res.Code)}}
			continue
		}
		res.Status.Node = taskStatus.Node
		tasksStatus[i] = res.Status
	}

	if jm.isRunning {
		go func() {
			jm.checkStatus(tasksStatus)
		}()
		jm.stats = append(jm.stats, tasksStatus)

	}
	return MsgJobStatus{Status: tasksStatus}
}

/* force stop all containers */
func (jm *JobManager) stop(force bool) MsgStop {
	for _, taskStatus := range jm.jobStatus.tasks {
		/* stop at background */
		go func(task TaskStatus) {
			v := url.Values{}
			v.Set("id", task.Id)

			resp, err := doRequest("POST", "http://"+task.Node+":8000/stop", strings.NewReader(v.Encode()), "application/x-www-form-urlencoded", "")
			if err != nil {
				log.Warn(err.Error())
			}
			body, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				log.Warn(err)
				return
			}
			var res MsgStop
			err = json.Unmarshal([]byte(string(body)), &res)
			if err != nil || res.Code != 0 {
				log.Warn(res)
				return
			}
			if res.Code != 0 {
				log.Warn(res.Error)
			}
			log.Info(task.HostName, " is killed:", task.Id)

		}(taskStatus)
	}

	go func() {
		if force {
			jm.killFlag = true
			jm.scheduler.UpdateProgress(jm.job, Stopped)
			log.Info("kill job, ", jm.job.Name)
		}
	}()
	return MsgStop{Code: 0}
}
