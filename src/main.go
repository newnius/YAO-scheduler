package main

import (
	"flag"
	"net/http"
	log "github.com/sirupsen/logrus"
	"encoding/json"
	"os"
	"time"
	"strconv"
)

var addr = flag.String("addr", "0.0.0.0:8080", "http service address")
var confFile = flag.String("conf", "/etc/yao/config.json", "configuration file path")

var scheduler Scheduler

func serverAPI(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("action") {
	case "resource_list":
		js, _ := json.Marshal(InstanceOfResourcePool().list())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "resource_get_by_node":
		id := r.URL.Query().Get("id")
		js, _ := json.Marshal(InstanceOfResourcePool().getByID(id))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "job_submit":
		var job Job
		log.Debug("job_submit")
		msgSubmit := MsgSubmit{Code: 0}
		err := json.Unmarshal([]byte(string(r.PostFormValue("job"))), &job)
		log.Info("Submit job ", job.Name, " at ", time.Now())
		if err != nil {
			msgSubmit.Code = 1
			msgSubmit.Error = err.Error()
		} else {
			for i := range job.Tasks {
				job.Tasks[i].ID = job.Name + ":" + job.Tasks[i].Name
			}
			scheduler.Schedule(job)
		}
		js, err := json.Marshal(msgSubmit)
		if err != nil {
			log.Warn(err)
		}
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
		js, _ := json.Marshal(InstanceOfResourcePool().statusHistory())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "get_counter":
		log.Debug("get_counters")
		js, _ := json.Marshal(InstanceOfResourcePool().getCounter())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "get_bindings":
		log.Debug("get_bindings")
		js, _ := json.Marshal(InstanceOfResourcePool().getBindings())
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
			scheduler.updateGroup(group)
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
			scheduler.updateGroup(group)
		}
		js, _ := json.Marshal(msg)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "group_remove":
		/* TODO: rearrange jobs to other queues */
		log.Debug("group_remove")
		var group Group
		msg := MsgGroupCreate{Code: 0}
		err := json.Unmarshal([]byte(string(r.PostFormValue("group"))), &group)
		if err != nil {
			msg.Code = 1
			msg.Error = err.Error()
		} else {
			msg = InstanceOfGroupManager().Remove(group)
			scheduler.updateGroup(group)
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
		js, _ := json.Marshal(scheduler.Enable())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_disable":
		log.Debug("disable schedule")
		js, _ := json.Marshal(scheduler.Disable())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_scheduler_dump":
		log.Debug("debug_scheduler_dump")
		js, _ := json.Marshal(scheduler.DebugDump())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_update_parallelism":
		log.Debug("update_parallelism")
		parallelism, _ := strconv.Atoi(r.URL.Query().Get("parallelism"))
		js, _ := json.Marshal(scheduler.UpdateParallelism(parallelism))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_update_enable_share_ratio":
		log.Debug("debug_update_enable_share_ratio")

		ratio := 0.75
		if t, err := strconv.ParseFloat(r.URL.Query().Get("ratio"), 32); err == nil {
			ratio = t
		}
		js, _ := json.Marshal(InstanceOfResourcePool().SetShareRatio(ratio))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_update_enable_pre_schedule_ratio":
		log.Debug("debug_update_enable_pre_schedule_ratio")
		ratio := 0.95
		if t, err := strconv.ParseFloat(r.URL.Query().Get("ratio"), 32); err == nil {
			ratio = t
		}
		js, _ := json.Marshal(InstanceOfResourcePool().SetPreScheduleRatio(ratio))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_get_predicts":
		log.Debug("debug_get_predicts")
		js, _ := json.Marshal(InstanceOfOptimizer().getAllPredicts())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_get_gpu_utils":
		log.Debug("debug_get_gpu_utils")
		js, _ := json.Marshal(InstanceOfOptimizer().getAllGPUUtils())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_optimizer_feed_dl":
		log.Debug("debug_optimizer_feed_dl")
		var job string
		var seq int
		var value int
		job = r.URL.Query().Get("job")
		if t, err := strconv.Atoi(r.URL.Query().Get("seq")); err == nil {
			seq = t
		}
		if t, err := strconv.Atoi(r.URL.Query().Get("value")); err == nil {
			value = t
		}
		InstanceOfOptimizer().feedData(job, seq, 0, 0, 0, value)
		js, _ := json.Marshal(OptimizerJobExecutionTime{})
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_optimizer_describe_job":
		log.Debug("debug_optimizer_describe_job")
		var job string
		job = r.URL.Query().Get("job")
		js, _ := json.Marshal(InstanceOfOptimizer().describe(job))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_optimizer_train_dl":
		log.Debug("debug_optimizer_train_dl")
		InstanceOfOptimizer().train(r.URL.Query().Get("job"))
		js, _ := json.Marshal(OptimizerJobExecutionTime{})
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_get_predict_dl":
		log.Debug("debug_get_predict_dl")
		if seq, err := strconv.Atoi(r.URL.Query().Get("seq")); err == nil {
			est, _ := InstanceOfOptimizer().predict(r.URL.Query().Get("job"), seq)
			js, _ := json.Marshal(est)
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
		} else {
			js, _ := json.Marshal(OptimizerJobExecutionTime{})
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
		}
		break

	case "allocator_update_strategy":
		log.Debug("allocator_update_strategy")
		strategy := r.URL.Query().Get("strategy")
		js, _ := json.Marshal(InstanceOfAllocator().updateStrategy(strategy))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "pool_enable_batch":
		log.Debug("pool_enable_batch")
		js, _ := json.Marshal(InstanceOfResourcePool().EnableBatch())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "pool_disable_batch":
		log.Debug("pool_disable_batch")
		js, _ := json.Marshal(InstanceOfResourcePool().DisableBatch())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "pool_set_batch_interval":
		log.Debug("pool_set_batch_interval")
		interval := str2int(r.URL.Query().Get("interval"), 1)
		js, _ := json.Marshal(InstanceOfResourcePool().SetBatchInterval(interval))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_pool_dump":
		log.Debug("debug_pool_dump")
		js, _ := json.Marshal(InstanceOfResourcePool().DebugDump())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_enable_mock":
		log.Debug("debug_enable_mock")
		js, _ := json.Marshal(InstanceOfConfiguration().EnableMock())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "debug_disable_mock":
		log.Debug("debug_disable_mock")
		js, _ := json.Marshal(InstanceOfConfiguration().DisableMock())
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

	/* init components */
	InstanceOfResourcePool().init(config)
	InstanceOfCollector().init(config)
	InstanceJobHistoryLogger().init(config)
	InstanceOfOptimizer().init(config)
	InstanceOfGroupManager().init(config)

	switch config.SchedulerPolicy {
	case "FCFS":
		scheduler = &SchedulerFCFS{}
		break
	case "priority":
		scheduler = &SchedulerPriority{}
		break
	case "capacity":
		scheduler = &SchedulerCapacity{}
		break
	case "fair":
		scheduler = &SchedulerFair{}
		break
	default:
		scheduler = &SchedulerFCFS{}
	}
	scheduler.Start()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serverAPI(w, r)
	})

	err = http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

}
