package main

import (
	"strconv"
	"math/rand"
	"time"
	"io"
	"net/http"
)

type Configuration struct {
	KafkaBrokers    []string `json:"kafkaBrokers"`
	KafkaTopic      string   `json:"kafkaTopic"`
	SchedulerPolicy string   `json:"schedulerPolicy"`
}

type MsgSubmit struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

type MsgPoolStatusHistory struct {
	Code  int          `json:"code"`
	Error string       `json:"error"`
	Data  []PoolStatus `json:"data"`
}

type MsgStop struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

type MsgSummary struct {
	Code         int    `json:"code"`
	Error        string `json:"error"`
	JobsFinished int    `json:"jobs_finished"`
	JobsRunning  int    `json:"jobs_running"`
	JobsPending  int    `json:"jobs_pending"`
	FreeGPU      int    `json:"gpu_free"`
	UsingGPU     int    `json:"gpu_using"`
}

type MsgResource struct {
	Code     int                   `json:"code"`
	Error    string                `json:"error"`
	Resource map[string]NodeStatus `json:"resources"`
}

type MsgJobList struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
	Jobs  []Job  `json:"jobs"`
}

type MsgLog struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
	Logs  string `json:"logs"`
}

type MsgTaskStatus struct {
	Code   int        `json:"code"`
	Error  string     `json:"error"`
	Status TaskStatus `json:"status"`
}

type MsgJobStatus struct {
	Code   int          `json:"code"`
	Error  string       `json:"error"`
	Status []TaskStatus `json:"status"`
}

type MsgCreate struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
	Id    string `json:"id"`
}

type TaskStatus struct {
	Id          string                 `json:"id"`
	HostName    string                 `json:"hostname"`
	Node        string                 `json:"node"`
	Image       string                 `json:"image"`
	ImageDigest string                 `json:"image_digest"`
	Command     string                 `json:"command"`
	CreatedAt   string                 `json:"created_at"`
	FinishedAt  string                 `json:"finished_at"`
	Status      string                 `json:"status"`
	State       map[string]interface{} `json:"state"`
}

type JobStatus struct {
	Name  string
	tasks map[string]TaskStatus
}

type GPUStatus struct {
	UUID             string `json:"uuid"`
	ProductName      string `json:"product_name"`
	PerformanceState string `json:"performance_state"`
	MemoryTotal      int    `json:"memory_total"`
	MemoryFree       int    `json:"memory_free"`
	MemoryAllocated  int    `json:"memory_allocated"`
	MemoryUsed       int    `json:"memory_used"`
	UtilizationGPU   int    `json:"utilization_gpu"`
	UtilizationMem   int    `json:"utilization_mem"`
	TemperatureGPU   int    `json:"temperature_gpu"`
	PowerDraw        int    `json:"power_draw"`
}

type NodeStatus struct {
	ClientID     string      `json:"id"`
	ClientHost   string      `json:"host"`
	Version      float64     `json:"version"`
	NumCPU       int         `json:"cpu_num"`
	UtilCPU      float64     `json:"cpu_load"`
	MemTotal     int         `json:"mem_total"`
	MemAvailable int         `json:"mem_available"`
	Status       []GPUStatus `json:"status"`
}

type Job struct {
	ID        int         `json:"id"`
	Name      string      `json:"name"`
	Tasks     []Task      `json:"tasks"`
	Workspace string      `json:"workspace"`
	Group     string      `json:"group"`
	Priority  JobPriority `json:"priority"`
	RunBefore int         `json:"run_before"`
	CreatedAt int         `json:"created_at"`
	UpdatedAt int         `json:"updated_at"`
	CreatedBy int         `json:"created_by"`
	Status    State       `json:"status"`
}

type Task struct {
	Name      string `json:"name"`
	Image     string `json:"image"`
	Cmd       string `json:"cmd"`
	NumberCPU int    `json:"cpu_number"`
	Memory    int    `json:"memory"`
	NumberGPU int    `json:"gpu_number"`
	MemoryGPU int    `json:"gpu_memory"`
	IsPS      bool   `json:"is_ps"`
	ModelGPU  string `json:"gpu_model"`
}

type Group struct {
	Name      string `json:"name"`
	Weight    int    `json:"weight"`
	Reserved  bool   `json:"reserved"`
	NumGPU    int    `json:"quota_gpu"`
	MemoryGPU int    `json:"quota_gpu_mem"`
	CPU       int    `json:"quota_cpu"`
	Memory    int    `json:"quota_mem"`
}

type MsgGroupCreate struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

type MsgGroupList struct {
	Code   int     `json:"code"`
	Error  string  `json:"error"`
	Groups []Group `json:"groups"`
}

type OptimizerJobExecutionTime struct {
	Pre     int `json:"pre"`
	Post    int `json:"post"`
	Total   int `json:"total"`
	Version int `json:"version"`
}

func str2int(str string, defaultValue int) int {
	i, err := strconv.Atoi(str)
	if err == nil {
		return i
	}
	return defaultValue
}

func getUA() string {
	rand.Seed(time.Now().Unix())
	UAs := []string{
		"Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0",
		"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:64.0) Gecko/20100101 Firefox/64.0",
		"Mozilla/5.0 (X11; Linux i586; rv:63.0) Gecko/20100101 Firefox/63.0",
		"Mozilla/5.0 (Windows NT 6.2; WOW64; rv:63.0) Gecko/20100101 Firefox/63.0",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:10.0) Gecko/20100101 Firefox/62.0",
		"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.13; ko; rv:1.9.1b2) Gecko/20081201 Firefox/60.0",
		"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/58.0",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14931",
		"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9",
	}
	return UAs[rand.Intn(len(UAs))]
}

func doRequest(method string, url string, r io.Reader, contentType string, referer string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", getUA())
	req.Header.Set("Referer", referer)

	resp, err := client.Do(req)
	return resp, err
}
