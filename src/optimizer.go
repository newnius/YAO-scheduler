package main

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

type Optimizer struct {
	scheduler  Scheduler
	killedFlag bool
}

var optimizerInstance *Optimizer
var OptimizerInstanceLock sync.Mutex

func InstanceOfOptimizer() *Optimizer {
	defer OptimizerInstanceLock.Unlock()
	OptimizerInstanceLock.Lock()

	if optimizerInstance == nil {
		optimizerInstance = &Optimizer{}
	}
	return optimizerInstance
}

func (jhl *Optimizer) feed(job string, utils []int) {
	log.Info("optimizer feed")
	log.Info(job, utils)
}
