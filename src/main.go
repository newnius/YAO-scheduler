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
	var nodes []int
	for id := range pool.nodes {
		nodes = append(nodes, id)
	}

	switch r.URL.Query().Get("action") {
	case "node_gets":
		js, _ := json.Marshal(nodes)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "resource_get_by_node":
		id := str2int(r.URL.Query().Get("id"), -1)
		js, _ := json.Marshal(pool.getByID(id))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "job_submit":
		var job Job
		fmt.Println("job_submit")
		err := json.Unmarshal([]byte(string(r.PostFormValue("job"))), &job)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(err.Error()))
			return
		}
		allocator.schedule(job)
		js, _ := json.Marshal(nodes)
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "job_status":
		fmt.Println("job_status")
		js, _ := json.Marshal(allocator.status(r.URL.Query().Get("id")))
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		break

	case "task_logs":
		fmt.Println("task_logs")
		fmt.Println(r.URL.Query().Get("id"))
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
	default:
		http.Error(w, "Not Found", http.StatusNotFound)
		break
	}
}

func main() {
	pool = &ResourcePool{}
	pool.nodes = make(map[int][]NodeStatus)

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
