package main

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"strings"
	"io/ioutil"
	"strconv"
	"encoding/json"
	"time"
	"math"
	"hash/fnv"
)

type Optimizer struct {
	scheduler  Scheduler
	killedFlag bool

	predicts map[string]*OptimizerJobExecutionTime

	jobUtilsGPU map[string]*OptimizerUtilGPU

	cache map[string]*OptimizerJobExecutionTime

	stats map[string]map[string]float64

	versions map[string]int
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
		optimizerInstance.stats = map[string]map[string]float64{}
		optimizerInstance.versions = map[string]int{}
	}
	return optimizerInstance
}

func (optimizer *Optimizer) init(conf Configuration) {
	log.Info("optimizer started")
}

func (optimizer *Optimizer) feedStats(job Job, role string, stats [][]TaskStatus) {
	if len(stats) == 0 {
		return
	}
	str := strings.Split(job.Name, "-")
	if len(str) == 1 {
		return
	}
	jobName := str[0]
	go func() {

		var UtilsCPU []float64
		var Mems []float64
		var BwRxs []float64
		var BwTxs []float64
		var UtilGPUs []float64
		var MemGPUs []float64
		for _, stat := range stats {
			for _, task := range stat {
				UtilsCPU = append(UtilsCPU, task.UtilCPU)
				Mems = append(Mems, task.Mem)
				BwRxs = append(BwRxs, task.BwRX)
				BwTxs = append(BwTxs, task.BWTx)
				UtilGPUs = append(UtilGPUs, float64(task.UtilGPU))
				MemGPUs = append(MemGPUs, float64(task.MemGPU))
			}
		}
		tmp := map[string]float64{
			"cpu":          optimizer.max(UtilsCPU),
			"cpu_std":      optimizer.std(UtilsCPU),
			"cpu_mean":     optimizer.mean(UtilsCPU),
			"mem":          optimizer.max(Mems),
			"bw_rx":        optimizer.mean(BwRxs),
			"bw_tx":        optimizer.mean(BwTxs),
			"gpu_util":     optimizer.mean(UtilGPUs),
			"gpu_util_std": optimizer.std(UtilGPUs),
			"gpu_mem":      optimizer.max(MemGPUs),
		}
		labels, _ := json.Marshal(tmp)

		cmd := job.Tasks[0].Cmd
		params := map[string]int{}

		psNumber := 0
		workerNumber := 0
		for _, task := range job.Tasks {
			if (role == "PS" && task.IsPS) || (role == "Worker" && !task.IsPS) {
				params["num_gpus"] = task.NumberGPU
				cmd = task.Cmd
			}
			if task.IsPS {
				psNumber++
			} else {
				workerNumber++
			}
		}
		params["ps_number"] = psNumber
		params["worker_number"] = workerNumber
		if role == "PS" {
			params["role"] = 1
		} else {
			params["role"] = 0
		}

		exceptions := map[string]bool{}
		exceptions["train_dir"] = true
		exceptions["variable_update"] = true
		exceptions["ps_hosts"] = true
		exceptions["worker_hosts"] = true
		exceptions["task_index"] = true

		pairs := strings.Split(cmd, " ")
		for _, pair := range pairs {
			v := strings.Split(pair, "=")
			if len(v) == 2 && v[0][:2] == "--" {
				var param string
				var value int
				param = v[0][2:]

				if val, err := strconv.Atoi(v[1]); err == nil {
					value = val
				} else {
					h := fnv.New32a()
					h.Write([]byte(v[1]))
					value = int(h.Sum32())
				}
				if _, ok := exceptions[param]; !ok {
					params[param] = value
				}
			}
		}
		//log.Info(job.Name, params)

		features, _ := json.Marshal(params)

		spider := Spider{}
		spider.Method = "GET"
		spider.URL = "http://yao-optimizer:8080/feed?job=" + jobName + "&features=" + string(features) + "&labels=" + string(labels)

		err := spider.do()
		if err != nil {
			log.Warn(err)
			return
		}

		resp := spider.getResponse()
		if _, err := ioutil.ReadAll(resp.Body); err != nil {
			log.Warn(err)
		}
		resp.Body.Close()
		if err != nil {
			log.Warn(err)
			return
		}

		optimizer.versions[jobName]++
		if optimizer.versions[jobName]%5 == 0 {
			optimizer.train(jobName)
		}
	}()
}

func (optimizer *Optimizer) max(values []float64) float64 {
	value := 0.0
	for _, v := range values {
		if v > value {
			value = v
		}
	}
	return value
}

func (optimizer *Optimizer) mean(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func (optimizer *Optimizer) std(values []float64) float64 {
	mean := optimizer.mean(values)
	std := 0.0
	for j := 0; j < len(values); j++ {
		// The use of Pow math function func Pow(x, y float64) float64
		std += math.Pow(values[j]-mean, 2)
	}
	// The use of Sqrt math function func Sqrt(x float64) float64
	std = math.Sqrt(std / float64(len(values)))
	return std
}

func (optimizer *Optimizer) describe(job string) map[string]float64 {
	if stat, ok := optimizer.stats[job]; ok {
		return stat
	}
	return map[string]float64{}
}

func (optimizer *Optimizer) feed3(job string, utils []UtilGPUTimeSeries) {
	log.Info("optimizer feed ", job)
	//log.Info(job, utils)

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
		if est, ok := optimizer.cache[jobName]; ok && est.Version > (int)(time.Now().Unix())-300 {
			return est, true
		}
		if est, ok := optimizer.predicts[jobName]; ok {
			if est.Version > 40 {
				if est2, ok := optimizer.predict(jobName, est.Version); ok {
					est2.Pre = est.Pre * est2.Total / est.Total
					est2.Main = est.Main * est2.Total / est.Total
					est2.Post = est.Post * est2.Total / est.Total
					est2.Version = (int)(time.Now().Unix())
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
		log.Warn(err)
		return
	}

	resp := spider.getResponse()
	if _, err := ioutil.ReadAll(resp.Body); err != nil {
		log.Warn(err)
	}
	resp.Body.Close()
	if err != nil {
		log.Warn(err)
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
	if err == nil {
		return OptimizerJobExecutionTime{Total: res.Total, Pre: res.Pre, Main: res.Main, Post: res.Post}, true
	}
	return OptimizerJobExecutionTime{}, false
}

func (optimizer *Optimizer) PredictReq(job Job, role string) MsgJobReq {
	res := MsgJobReq{CPU: 4, Mem: 4096, UtilGPU: 100, MemGPU: 8192, BW: 0}

	var jobName string
	str := strings.Split(job.Name, "-")
	if len(str) == 2 {
		jobName = str[0]
	}
	cmd := ""
	params := map[string]int{}

	psNumber := 0
	workerNumber := 0
	flag := false
	for _, task := range job.Tasks {
		if (role == "PS" && task.IsPS) || (role == "Worker" && !task.IsPS) {
			params["num_gpus"] = task.NumberGPU
			cmd = task.Cmd
			flag = true
		}
		if task.IsPS {
			psNumber++
		} else {
			workerNumber++
		}
	}
	params["ps_number"] = psNumber
	params["worker_number"] = workerNumber
	if role == "PS" {
		params["role"] = 1
	} else {
		params["role"] = 0
	}
	if !flag {
		return res
	}

	exceptions := map[string]bool{}
	exceptions["train_dir"] = true
	exceptions["variable_update"] = true
	exceptions["ps_hosts"] = true
	exceptions["worker_hosts"] = true
	exceptions["task_index"] = true

	pairs := strings.Split(cmd, " ")
	for _, pair := range pairs {
		v := strings.Split(pair, "=")
		if len(v) == 2 && v[0][:2] == "--" {
			var param string
			var value int
			param = v[0][2:]

			if val, err := strconv.Atoi(v[1]); err == nil {
				value = val
			} else {
				h := fnv.New32a()
				h.Write([]byte(v[1]))
				value = int(h.Sum32())
			}
			if _, ok := exceptions[param]; !ok {
				params[param] = value
			}
		}
	}
	//log.Info(job.Name, params)

	features, _ := json.Marshal(params)

	spider := Spider{}
	spider.Method = "GET"
	spider.URL = "http://yao-optimizer:8080/predict?job=" + jobName + "&features=" + string(features)

	err := spider.do()
	if err != nil {
		return MsgJobReq{Code: 2, Error: err.Error()}
	}

	resp := spider.getResponse()
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Warn(err)
		return MsgJobReq{Code: 3, Error: err.Error()}
	}

	var msg MsgJobReqPredict
	err = json.Unmarshal([]byte(string(body)), &msg)
	if err == nil && msg.Code == 0 {
		tmp := msg.Labels
		if v, ok := tmp["cpu"]; ok {
			res.CPU = int(math.Ceil(v / 100))
		}
		if v, ok := tmp["mem"]; ok {
			res.Mem = int(math.Ceil(v/1024)) * 1024
		}
		if v, ok := tmp["gpu_util"]; ok {
			res.UtilGPU = int(math.Ceil(v)/10) * 10
		}
		if v, ok := tmp["gpu_mem"]; ok {
			res.MemGPU = int(math.Ceil(v/1024)) * 1024
		}
		if v, ok := tmp["bw"]; ok {
			res.BW = int(math.Ceil(v/10)) * 10
		}
	}
	return res
}
