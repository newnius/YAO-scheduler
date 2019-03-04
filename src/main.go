package main

import (
	"flag"
	"net/http"
	"log"
	"encoding/json"
)

var addr = flag.String("addr", ":8080", "http service address")

var pool *ResourcePool

func serverAPI(w http.ResponseWriter, r *http.Request) {
	nodes := make([]int, 1)
	for id := range pool.nodes {
		nodes = append(nodes, id)
	}

	switch r.URL.Query().Get("action") {
	case "host_gets":
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
		jm := &JobManager{}
		id := str2int(r.URL.Query().Get("id"), -1)
		go func() {
			jm.start(id)
		}()
		js, _ := json.Marshal(nodes)
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
	pool.nodes = make(map[int][]Status)
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
