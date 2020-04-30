package main

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"strings"
)

type Optimizer struct {
	scheduler  Scheduler
	killedFlag bool

	predicts map[string]OptimizerJobExecutionTime

	jobUtilsGPU map[string]int
	versions    map[string]int
}

var optimizerInstance *Optimizer
var OptimizerInstanceLock sync.Mutex

func InstanceOfOptimizer() *Optimizer {
	defer OptimizerInstanceLock.Unlock()
	OptimizerInstanceLock.Lock()

	if optimizerInstance == nil {
		optimizerInstance = &Optimizer{}
		optimizerInstance.predicts = map[string]OptimizerJobExecutionTime{}
		optimizerInstance.jobUtilsGPU = map[string]int{}
		optimizerInstance.versions = map[string]int{}
	}
	return optimizerInstance
}

func (optimizer *Optimizer) feed(job string, utils []int) {
	log.Info("optimizer feed")
	log.Info(job, utils)
	log.Info(optimizer.jobUtilsGPU)
	log.Info(optimizer.predicts)

	if len(utils) == 0 {
		return
	}

	go func() {
		str := strings.Split(job, "-")
		if len(str) == 2 {
			preCnt := 0

			sum := 0
			for i := 0; i < len(utils); i++ {
				sum += utils[i]
			}
			last := 0
			version := 0
			if t, err := optimizer.jobUtilsGPU[job]; !err {
				last = t
				version = optimizer.versions[job]
			}
			optimizer.jobUtilsGPU[job] = (version*last + sum/len(utils)) / (version + 1)
			optimizer.versions[job]++

			for i := 0; i < len(utils); i++ {
				if utils[i] > 15 {
					break
				}
				preCnt++
			}

			postCnt := 0
			for i := len(utils) - 1; i >= 0; i-- {
				if utils[i] > 15 {
					break
				}
				postCnt++
			}

			if _, ok := optimizer.predicts[str[0]]; !ok {
				optimizer.predicts[str[0]] = OptimizerJobExecutionTime{}
			}
			predict := optimizer.predicts[str[0]]
			predict.Pre = ((predict.Pre * predict.Version) + preCnt) / (predict.Version + 1)
			predict.Post = ((predict.Post * predict.Version) + postCnt) / (predict.Version + 1)
			predict.Total = ((predict.Total * predict.Version) + len(utils)) / (predict.Version + 1)
			predict.Main = predict.Total - predict.Pre - predict.Post
			predict.Version++
		}
	}()
}

func (optimizer *Optimizer) predictTime(job string, utils []int) (int, bool) {
	if _, err := optimizer.jobUtilsGPU[job]; err {
		return 100, false
	}
	if optimizer.versions[job] > 5 {
		return optimizer.jobUtilsGPU[job], true
	}
	return optimizer.jobUtilsGPU[job], false
}

func (optimizer *Optimizer) predictUtilGPU(job string) (OptimizerJobExecutionTime, bool) {
	if _, err := optimizer.predicts[job]; err {
		return OptimizerJobExecutionTime{}, false
	}
	return optimizer.predicts[job], optimizer.predicts[job].Version > 5
}
