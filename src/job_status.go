package main

type JobStatus struct {
	Name  string
	tasks map[string]TaskStatus
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
