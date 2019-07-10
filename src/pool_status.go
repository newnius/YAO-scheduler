package main

type PoolStatus struct {
	TimeStamp       string  `json:"ts"`
	UtilCPU         float64 `json:"cpu_util"`
	TotalCPU        int     `json:"cpu_total"`
	TotalMem        int     `json:"mem_total"`
	AvailableMem    int     `json:"mem_available"`
	TotalGPU        int     `json:"TotalGPU"`
	UtilGPU         int     `json:"gpu_util"`
	TotalMemGPU     int     `json:"gpu_mem_total"`
	AvailableMemGPU int     `json:"gpu_mem_available"`
}
