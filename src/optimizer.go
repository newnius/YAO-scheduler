package main

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"strings"
	"io/ioutil"
	"strconv"
	"encoding/json"
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

			optimizer.feedData(jobName, predict.Version, 0, 0, 0, predict.Total)
			if predict.Version%10 == 0 && predict.Version > 30 {
				optimizer.train(jobName)
			}
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
		if est, ok := optimizer.cache[jobName]; ok {
			return est, true
		}
		if est, ok := optimizer.predicts[jobName]; ok {
			if est.Version > 40 {
				if est2, ok := optimizer.predict(jobName, est.Version); ok {
					est2.Pre = est.Pre * est2.Total / est.Total
					est2.Main = est.Main * est2.Total / est.Total
					est2.Post = est.Post * est2.Total / est.Total
					optimizer.cache[jobName] = &est2
					return &est2, true
				}
			}
			return est, est.Version >= 5
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

func (optimizer *Optimizer) feedData(job string, seq int, pre int, main int, post int, total int) {
	spider := Spider{}
	spider.Method = "GET"
	params := "job=" + job + "&seq=" + strconv.Itoa(seq) + "&value=" + strconv.Itoa(total)
	spider.URL = "http://yao-optimizer:8080/feed?" + params

	err := spider.do()
	if err != nil {
		return
	}

	resp := spider.getResponse()
	if _, err := ioutil.ReadAll(resp.Body); err != nil {
		log.Warn(err)
	}
	resp.Body.Close()
	if err != nil {
		return
	}
}

func (optimizer *Optimizer) train(job string) {
	spider := Spider{}
	spider.Method = "GET"
	params := "job=" + job
	spider.URL = "http://yao-optimizer:8080/train?" + params

	err := spider.do()
	if err != nil {
		return
	}

	resp := spider.getResponse()
	if _, err := ioutil.ReadAll(resp.Body); err != nil {
		log.Warn(err)
	}
	resp.Body.Close()
	if err != nil {
		return
	}
}

func (optimizer *Optimizer) predict(job string, seq int) (OptimizerJobExecutionTime, bool) {
	spider := Spider{}
	spider.Method = "GET"
	params := "job=" + job + "&seq=" + strconv.Itoa(seq)
	spider.URL = "http://yao-optimizer:8080/predict?" + params

	err := spider.do()
	if err != nil {
		return OptimizerJobExecutionTime{}, false
	}

	resp := spider.getResponse()
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Warn(err)
		return OptimizerJobExecutionTime{}, false
	}

	var res MsgOptimizerPredict
	err = json.Unmarshal([]byte(string(body)), &res)
	log.Println(res)
	if err == nil {
		return OptimizerJobExecutionTime{Total: res.Total, Pre: res.Pre, Main: res.Main, Post: res.Post}, true
	}
	return OptimizerJobExecutionTime{}, false
}
