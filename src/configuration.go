package main

import (
	"sync"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"strconv"
)

type Configuration struct {
	KafkaBrokers           []string `json:"KafkaBrokers"`
	KafkaTopic             string   `json:"KafkaTopic"`
	SchedulerPolicy        string   `json:"SchedulerPolicy"`
	ListenAddr             string   `json:"ListenAddr"`
	HDFSAddress            string   `json:"HDFSAddress"`
	HDFSBaseDir            string   `json:"HDFSBaseDir"`
	DFSBaseDir             string   `json:"DFSBaseDir"`
	EnableShareRatio       float64  `json:"EnableShareRatio"`
	EnablePreScheduleRatio float64  `json:"EnablePreScheduleRatio"`

	mock bool
	mu   sync.Mutex
}

var configurationInstance *Configuration
var ConfigurationInstanceLock sync.Mutex

func InstanceOfConfiguration() *Configuration {
	ConfigurationInstanceLock.Lock()
	defer ConfigurationInstanceLock.Unlock()

	if configurationInstance == nil {
		/* set default values */
		configurationInstance = &Configuration{
			mock: false,
			KafkaBrokers: []string{
				"kafka-node1:9092",
				"kafka-node2:9092",
				"kafka-node3:9092",
			},
			KafkaTopic:             "yao",
			SchedulerPolicy:        "fair",
			ListenAddr:             "0.0.0.0:8080",
			HDFSAddress:            "",
			HDFSBaseDir:            "/user/root/",
			DFSBaseDir:             "",
			EnableShareRatio:       1.5,
			EnablePreScheduleRatio: 1.5,
		}

		/* override conf value from env */
		value := os.Getenv("KafkaBrokers")
		if len(value) != 0 {
			configurationInstance.KafkaBrokers = strings.Split(value, ",")
		}
		value = os.Getenv("KafkaTopic")
		if len(value) != 0 {
			configurationInstance.KafkaTopic = value
		}
		value = os.Getenv("SchedulerPolicy")
		if len(value) != 0 {
			configurationInstance.SchedulerPolicy = value
		}
		value = os.Getenv("ListenAddr")
		if len(value) != 0 {
			configurationInstance.ListenAddr = value
		}
		value = os.Getenv("HDFSAddress")
		if len(value) != 0 {
			configurationInstance.HDFSAddress = value
		}
		value = os.Getenv("HDFSBaseDir")
		if len(value) != 0 {
			configurationInstance.HDFSBaseDir = value
		}
		value = os.Getenv("DFSBaseDir")
		if len(value) != 0 {
			configurationInstance.DFSBaseDir = value
		}
		value = os.Getenv("EnableShareRatio")
		if len(value) != 0 {
			if val, err := strconv.ParseFloat(value, 32); err == nil {
				configurationInstance.EnableShareRatio = val
			}
		}
		value = os.Getenv("EnablePreScheduleRatio")
		if len(value) != 0 {
			if val, err := strconv.ParseFloat(value, 32); err == nil {
				configurationInstance.EnablePreScheduleRatio = val
			}
		}
	}
	return configurationInstance
}

func (config *Configuration) EnableMock() bool {
	config.mu.Lock()
	defer config.mu.Unlock()
	config.mock = true
	log.Info("configuration.mock = true")
	return true
}

func (config *Configuration) DisableMock() bool {
	config.mu.Lock()
	defer config.mu.Unlock()
	config.mock = false
	log.Info("configuration.mock = false")
	return true
}

func (config *Configuration) SetShareRatio(ratio float64) bool {
	config.EnableShareRatio = ratio
	log.Info("enableShareRatio is updated to ", ratio)
	return true
}

func (config *Configuration) SetPreScheduleRatio(ratio float64) bool {
	config.EnablePreScheduleRatio = ratio
	log.Info("enablePreScheduleRatio is updated to ", ratio)
	return true
}

func (config *Configuration) Dump() map[string]interface{} {
	res := map[string]interface{}{}
	res["KafkaBrokers"] = config.KafkaBrokers
	res["KafkaTopic"] = config.KafkaTopic
	res["SchedulerPolicy"] = config.SchedulerPolicy
	res["ListenAddr"] = config.ListenAddr
	res["Mock"] = config.mock
	res["HDFSAddress"] = config.HDFSAddress
	res["HDFSBaseDir"] = config.HDFSBaseDir
	res["DFSBaseDir"] = config.DFSBaseDir
	res["EnableShareRatio"] = config.EnableShareRatio
	res["EnablePreScheduleRatio"] = config.EnablePreScheduleRatio
	return res
}
