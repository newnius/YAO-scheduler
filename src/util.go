package main

import (
	"strconv"
	"math/rand"
	"time"
	"io"
	"net/http"
)

type Job struct {
	ID           int         `json:"id"`
	Name         string      `json:"name"`
	Tasks        []Task      `json:"tasks"`
	Workspace    string      `json:"workspace"`
	Group        string      `json:"group"`
	BasePriority float64     `json:"base_priority"`
	Priority     JobPriority `json:"priority"`
	RunBefore    int         `json:"run_before"`
	CreatedAt    int         `json:"created_at"`
	UpdatedAt    int         `json:"updated_at"`
	CreatedBy    int         `json:"created_by"`
	Locality     int         `json:"locality"`
	Status       State       `json:"status"`
	NumberGPU    int         `json:"number_GPU"`
}

type Task struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Job       string `json:"job_name"`
	Image     string `json:"image"`
	Cmd       string `json:"cmd"`
	NumberCPU int    `json:"cpu_number"`
	Memory    int    `json:"memory"`
	NumberGPU int    `json:"gpu_number"`
	MemoryGPU int    `json:"gpu_memory"`
	IsPS      bool   `json:"is_ps"`
	ModelGPU  string `json:"gpu_model"`
}

type UtilGPUTimeSeries struct {
	Time int `json:"time"`
	Util int `json:"util"`
}

type OptimizerJobExecutionTime struct {
	Pre     int `json:"pre"`
	Post    int `json:"post"`
	Total   int `json:"total"`
	Main    int `json:"main"`
	Version int `json:"version"`
}

type OptimizerUtilGPU struct {
	Util    int `json:"util"`
	Version int `json:"version"`
}

type ResourceCount struct {
	NumberGPU int
	MemoryGPU int
	CPU       int
	Memory    int
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
