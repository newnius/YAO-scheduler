package main

import (
	"strconv"
)

type Status struct {
	UUID             string `json:"uuid"`
	ProductName      string `json:"product_name"`
	FanSpeed         int    `json:"fan_speed"`
	PerformanceState string `json:"performance_state"`
	MemoryTotal      int    `json:"emory_total"`
	MemoryFree       int    `json:"memory_free"`
	MemoryUsed       int    `json:"memory_used"`
	UtilizationGPU   int    `json:"utilization_gpu"`
	UtilizationMem   int    `json:"utilization_mem"`
	TemperatureGPU   int    `json:"temperature_gpu"`
	PowerDraw        int    `json:"power_draw"`
}

type MsgAgent struct {
	ClientID int      `json:"code"`
	Status   []Status `json:"status"`
}

func str2int(str string, defaultValue int) int {
	i, err := strconv.Atoi(str)
	if err == nil {
		return i
	}
	return defaultValue
}
