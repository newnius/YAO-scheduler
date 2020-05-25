package main

import (
	"strconv"
	"math/rand"
	"time"
	"log"
	"github.com/MaxHalford/eaopt"
	"math"
	"testing"
)

func TestGA(t *testing.T) {
	numTask := 20

	nodesMap = map[string]NodeStatus{}
	tasksMap = map[string]Task{}

	for i := 0; i < numTask*3; i++ {
		node := NodeStatus{ClientID: strconv.Itoa(i), Rack: strconv.Itoa(i % 40), Domain: strconv.Itoa(i % 4)}
		node.NumCPU = 24
		node.MemTotal = 188
		node.TotalBW = 100
		cnt := rand.Intn(3) + 1
		for i := 0; i < cnt; i++ {
			node.Status = append(node.Status, GPUStatus{MemoryTotal: 11439, MemoryAllocated: 0, UUID: node.ClientID + strconv.Itoa(i)})
		}
		nodesMap[strconv.Itoa(i)] = node
	}
	for i := 0; i < numTask; i++ {
		isPS := false
		if i >= 3 {
			isPS = true
		}
		task := Task{Name: strconv.Itoa(i), IsPS: isPS}
		task.Memory = 4
		task.NumberCPU = 2
		task.NumberGPU = 1
		tasksMap[strconv.Itoa(i)] = task
	}

	var nodes []NodeStatus
	var tasks []Task

	for _, node := range nodesMap {
		nodes = append(nodes, node)
	}
	for _, task := range tasksMap {
		tasks = append(tasks, task)
	}
	s := time.Now()
	allocation := fastBestFit(nodes, tasks)
	log.Println(time.Since(s))

	// Instantiate a GA with a GAConfig
	var ga, err = eaopt.NewDefaultGAConfig().NewGA()
	if err != nil {
		log.Println(err)
		return
	}

	// Set the number of generations to run for
	ga.NGenerations = math.MaxInt32
	ga.NPops = 1
	ga.PopSize = 30 + uint(numTask/2)

	// Add a custom print function to track progress
	ga.Callback = func(ga *eaopt.GA) {
		log.Printf("Best fitness at generation %d: %f\n", ga.Generations, ga.HallOfFame[0].Fitness)
	}

	bestFitness := math.MaxFloat64
	count := 0

	ts := time.Now()

	ga.EarlyStop = func(ga *eaopt.GA) bool {
		gap := math.Abs(ga.HallOfFame[0].Fitness - bestFitness)
		if gap <= 0.000001 || ga.HallOfFame[0].Fitness >= bestFitness {
			if count >= 30 || time.Since(ts) > time.Second*30 {
				log.Println("Early Stop")
				return true
			} else {
				count++
			}
		} else {
			bestFitness = ga.HallOfFame[0].Fitness
			count = 1
		}
		return false
	}

	// Find the minimum
	err = ga.Minimize(VectorFactory)
	log.Println(time.Since(ts))
	log.Println(ga.HallOfFame[0].Genome.(Allocation).TasksOnNode)
	//fmt.Println(ga.HallOfFame[0].Genome.(Allocation).Nodes)
	if err != nil {
		log.Println(err)
		return
	}

	log.Println(allocation)
}
