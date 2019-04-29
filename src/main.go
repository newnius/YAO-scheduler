package main

import (
	"flag"
	"net/http"
	"log"
	"encoding/json"
	"fmt"
)

var addr = flag.String("addr", ":8080", "http service address")

var pool *ResourcePool

var allocator *AllocatorFIFO

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
		fmt.Println("job_submit")
		msgSubmit := MsgSubmit{Code: 0}
		err := json.Unmarshal([]byte(string(r.PostFormValue("job"))), &job)
		if err != nil {
			msgSubmit.Code = 1
			msgSubmit.Error = err.Error()
		} else {
			allocator.schedule(job)
		}
		js, _ := json.Marshal(msgSubmit)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "job_status":
		fmt.Println("job_status")
		js, _ := json.Marshal(allocator.status(r.URL.Query().Get("id")))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "job_stop":
		fmt.Println("job_stop")
		js, _ := json.Marshal(allocator.stop(string(r.PostFormValue("id"))))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "task_logs":
		fmt.Println("task_logs")
		js, _ := json.Marshal(allocator.logs(r.URL.Query().Get("job"), r.URL.Query().Get("task")))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "jobs":
		fmt.Println("job_list")
		js, _ := json.Marshal(allocator.listJobs())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "summary":
		fmt.Println("summary")
		js, _ := json.Marshal(allocator.summary())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "pool_status_history":
		fmt.Println("pool_status_history")
		js, _ := json.Marshal(pool.statusHistory())
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	default:
		http.Error(w, "Not Found", http.StatusNotFound)
		break
	}
}

func main() {
	pool = &ResourcePool{}
	pool.nodes = make(map[string]NodeStatus)
	pool.start()

	allocator = &AllocatorFIFO{}
	allocator.start()

	go func() {
		start(pool)
	}()

	flag.Parse()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serverAPI(w, r)
	})

	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

}
