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
}

var optimizerInstance *Optimizer
var OptimizerInstanceLock sync.Mutex

func InstanceOfOptimizer() *Optimizer {
	defer OptimizerInstanceLock.Unlock()
	OptimizerInstanceLock.Lock()

	if optimizerInstance == nil {
		optimizerInstance = &Optimizer{}
		optimizerInstance.predicts = map[string]OptimizerJobExecutionTime{}
	}
	return optimizerInstance
}

func (optimizer *Optimizer) feed(job string, utils []int) {
	log.Info("optimizer feed")
	log.Info(job, utils)

	go func() {
		str := strings.Split(job, "-")
		if len(str) == 2 {
			preCnt := 0
			for i := 0; i < len(utils); i++ {
				if utils[i] > 15 {
					break
				}
				preCnt++
			}

			postCnt := 0
			for i := len(utils)-1; i >= 0; i-- {
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
			predict.Version++
		}
	}()
}
