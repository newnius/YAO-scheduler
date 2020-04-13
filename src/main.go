package main

import (
	"flag"
	"net/http"
	log "github.com/sirupsen/logrus"
	"encoding/json"
	"os"
	"time"
)

var addr = flag.String("addr", "0.0.0.0:8080", "http service address")
var confFile = flag.String("conf", "/etc/yao/config.json", "configuration file path")

var pool *ResourcePool

var scheduler Scheduler

func serverAPI(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("action") {
	case "resource_list":
		js, _ := json.Marshal(pool.list())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "resource_get_by_node":
		id := r.URL.Query().Get("id")
		js, _ := json.Marshal(pool.getByID(id))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "job_submit":
		var job Job
		log.Debug("job_submit")
		msgSubmit := MsgSubmit{Code: 0}
		err := json.Unmarshal([]byte(string(r.PostFormValue("job"))), &job)
		log.Info("Submit job", job.Name, time.Now())
		if err != nil {
			msgSubmit.Code = 1
			msgSubmit.Error = err.Error()
		} else {
			scheduler.Schedule(job)
		}
		js, _ := json.Marshal(msgSubmit)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "job_status":
		log.Debug("job_status")
		js, _ := json.Marshal(scheduler.QueryState(r.URL.Query().Get("id")))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "job_stop":
		log.Debug("job_stop")
		js, _ := json.Marshal(scheduler.Stop(string(r.PostFormValue("id"))))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "task_logs":
		log.Debug("task_logs")
		js, _ := json.Marshal(scheduler.QueryLogs(r.URL.Query().Get("job"), r.URL.Query().Get("task")))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "jobs":
		log.Debug("job_list")
		js, _ := json.Marshal(scheduler.ListJobs())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "summary":
		log.Debug("summary")
		js, _ := json.Marshal(scheduler.Summary())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "pool_status_history":
		log.Debug("pool_status_history")
		js, _ := json.Marshal(pool.statusHistory())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "get_counter":
		log.Debug("get_counters")
		js, _ := json.Marshal(pool.getCounter())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "get_bindings":
		log.Debug("get_bindings")
		js, _ := json.Marshal(pool.getBindings())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "group_list":
		log.Debug("group_list")
		js, _ := json.Marshal(InstanceOfGroupManager().List())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "group_add":
		log.Debug("group_add")
		var group Group
		msg := MsgGroupCreate{Code: 0}
		err := json.Unmarshal([]byte(string(r.PostFormValue("group"))), &group)
		if err != nil {
			msg.Code = 1
			msg.Error = err.Error()
		} else {
			msg = InstanceOfGroupManager().Add(group)
		}
		js, _ := json.Marshal(msg)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "group_update":
		log.Debug("group_update")
		var group Group
		msg := MsgGroupCreate{Code: 0}
		err := json.Unmarshal([]byte(string(r.PostFormValue("group"))), &group)
		if err != nil {
			msg.Code = 1
			msg.Error = err.Error()
		} else {
			msg = InstanceOfGroupManager().Update(group)
		}
		js, _ := json.Marshal(msg)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "group_remove":
		log.Debug("group_remove")
		var group Group
		msg := MsgGroupCreate{Code: 0}
		err := json.Unmarshal([]byte(string(r.PostFormValue("group"))), &group)
		if err != nil {
			msg.Code = 1
			msg.Error = err.Error()
		} else {
			msg = InstanceOfGroupManager().Remove(group)
		}
		js, _ := json.Marshal(msg)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "jhl_job_status":
		log.Debug("jhl_job_status")
		js, _ := json.Marshal(InstanceJobHistoryLogger().getTaskStatus(r.URL.Query().Get("job")))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_enable":
		log.Debug("enable schedule")
		js, _ := json.Marshal(scheduler.Enable)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_disable":
		log.Debug("disable schedule")
		js, _ := json.Marshal(scheduler.Disable)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	default:
		http.Error(w, "Not Found", http.StatusNotFound)
		break
	}
}

func main() {
	flag.Parse()
	/* read configuration */
	file, err := os.Open(*confFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	/* parse configuration */
	decoder := json.NewDecoder(file)
	config := Configuration{}
	err = decoder.Decode(&config)
	if err != nil {
		log.Fatal(err)
	}

	/* init jhl */
	InstanceJobHistoryLogger().init()

	pool = &ResourcePool{}
	pool.nodes = make(map[string]NodeStatus)
	pool.start()

	switch config.SchedulerPolicy {
	case "FCFS":
		scheduler = &SchedulerFCFS{}
		break
	case "fair":
		scheduler = &SchedulerFair{}
		break
	case "priority":
		scheduler = &SchedulerPriority{}
		break
	default:
		scheduler = &SchedulerFCFS{}
	}

	scheduler.Start()

	go func() {
		start(pool, config)
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serverAPI(w, r)
	})

	err = http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

}
