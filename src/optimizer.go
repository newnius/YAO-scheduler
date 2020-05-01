package main

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"strings"
)

type Optimizer struct {
	scheduler  Scheduler
	killedFlag bool

	predicts map[string]*OptimizerJobExecutionTime

	jobUtilsGPU map[string]*OptimizerUtilGPU

	cache map[string]*OptimizerJobExecutionTime
}

var optimizerInstance *Optimizer
var OptimizerInstanceLock sync.Mutex

func InstanceOfOptimizer() *Optimizer {
	defer OptimizerInstanceLock.Unlock()
	OptimizerInstanceLock.Lock()

	if optimizerInstance == nil {
		optimizerInstance = &Optimizer{}
		optimizerInstance.predicts = map[string]*OptimizerJobExecutionTime{}
		optimizerInstance.jobUtilsGPU = map[string]*OptimizerUtilGPU{}
		optimizerInstance.cache = map[string]*OptimizerJobExecutionTime{}
	}
	return optimizerInstance
}

func (optimizer *Optimizer) feed(job string, utils []UtilGPUTimeSeries) {
	log.Info("optimizer feed")
	log.Info(job, utils)

	if len(utils) == 0 {
		return
	}

	go func() {
		str := strings.Split(job, "-")
		if len(str) == 2 {
			jobName := str[0]

			sum := 0
			for i := 0; i < len(utils); i++ {
				sum += utils[i].Util
			}
			sum /= len(utils)
			if _, ok := optimizer.jobUtilsGPU[jobName]; !ok {
				optimizer.jobUtilsGPU[jobName] = &OptimizerUtilGPU{}
			}
			t := optimizer.jobUtilsGPU[jobName]
			t.Util = (t.Version*t.Util + sum) / (t.Version + 1)
			t.Version++

			preTime := 0
			for i := 0; i < len(utils); i++ {
				if utils[i].Util > 15 {
					preTime = utils[i].Time - utils[0].Time
					break
				}
			}

			postTime := 0
			for i := len(utils) - 1; i >= 0; i-- {
				if utils[i].Util > 15 {
					postTime = utils[len(utils)-1].Time - utils[i].Time
					break
				}
			}

			if _, ok := optimizer.predicts[jobName]; !ok {
				optimizer.predicts[jobName] = &OptimizerJobExecutionTime{}
			}
			totalTime := utils[len(utils)-1].Time - utils[0].Time

			predict := optimizer.predicts[jobName]
			if predict.Version == 0 {
				predict.Pre = preTime
				predict.Post = postTime
				predict.Total = totalTime
				predict.Main = predict.Total - predict.Pre - predict.Post
				if predict.Main < 0 {
					predict.Main = 0
				}
			}
			predict.Pre = (predict.Pre*95 + preTime*5) / 100
			predict.Post = (predict.Post*95 + postTime*5) / 100
			predict.Total = (predict.Total*95 + totalTime*5) / 100
			predict.Main = predict.Total - predict.Pre - predict.Post
			if predict.Main < 0 {
				predict.Main = 0
			}
			predict.Version++
		}
	}()
}

func (optimizer *Optimizer) predictUtilGPU(job string) (int, bool) {
	str := strings.Split(job, "-")
	if len(str) == 2 {
		jobName := str[0]
		if _, ok := optimizer.jobUtilsGPU[jobName]; ok {
			return optimizer.jobUtilsGPU[jobName].Util, optimizer.jobUtilsGPU[jobName].Version >= 5
		}
	}
	return 100, false
}

func (optimizer *Optimizer) predictTime(job string) (*OptimizerJobExecutionTime, bool) {
	str := strings.Split(job, "-")
	if len(str) == 2 {
		jobName := str[0]
		if _, ok := optimizer.predicts[jobName]; ok {
			return optimizer.predicts[jobName], optimizer.predicts[jobName].Version >= 5
		}
	}
	return &OptimizerJobExecutionTime{}, false
}

func (optimizer *Optimizer) getAllPredicts() map[string]*OptimizerJobExecutionTime {
	return optimizer.predicts
}

func (optimizer *Optimizer) getAllGPUUtils() map[string]*OptimizerUtilGPU {
	return optimizer.jobUtilsGPU
}
