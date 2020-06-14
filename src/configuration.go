package main

import (
	"sync"
	log "github.com/sirupsen/logrus"
)

type Configuration struct {
	KafkaBrokers    []string `json:"kafkaBrokers"`
	KafkaTopic      string   `json:"kafkaTopic"`
	SchedulerPolicy string   `json:"schedulerPolicy"`
	mock            bool
	mu              sync.Mutex
}

var ConfigurationInstance *Configuration
var ConfigurationInstanceLock sync.Mutex

func InstanceOfConfiguration() *Configuration {
	ConfigurationInstanceLock.Lock()
	defer ConfigurationInstanceLock.Unlock()

	if ConfigurationInstance == nil {
		ConfigurationInstance = &Configuration{mock: false}
	}
	return ConfigurationInstance
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
