package main

import (
	"strconv"
	"math/rand"
	"time"
	log "github.com/sirupsen/logrus"
	"github.com/MaxHalford/eaopt"
	"math"
	"testing"
)

func TgenerateCase() ([]NodeStatus, []Task) {
	numTask := 6

	var nodes []NodeStatus
	var tasks []Task

	for i := 0; i < numTask*3; i++ {
		node := NodeStatus{ClientID: strconv.Itoa(i), Rack: "Rack-" + strconv.Itoa(i%40), Domain: "Domain-" + strconv.Itoa(i%4)}
		node.NumCPU = 24
		node.UtilCPU = 2.0
		node.MemTotal = 188
		node.MemAvailable = 20
		node.TotalBW = 100
		//cnt := 4
		cnt := rand.Intn(3) + 1
		for i := 0; i < cnt; i++ {
			node.Status = append(node.Status, GPUStatus{MemoryTotal: 11439, MemoryAllocated: 0, UUID: node.ClientID + "-" + strconv.Itoa(i)})
		}
		nodes = append(nodes, node)
	}
	for i := 0; i < numTask; i++ {
		isPS := false
		if i%4 == 0 {
			isPS = true
		}
		task := Task{Name: "task-" + strconv.Itoa(i), IsPS: isPS}
		task.Memory = 4
		task.NumberCPU = 2
		task.NumberGPU = 1
		task.MemoryGPU = 4096
		tasks = append(tasks, task)
	}
	return nodes, tasks
}

func TestBestFit(t *testing.T) {
	nodes, tasks := TgenerateCase()
	for _, node := range nodes {
		log.Info(node)
	}
	s := time.Now()
	allocation := fastBestFit(nodes, tasks)
	log.Println(time.Since(s))
	log.Println(allocation)
}

func TestGA(t *testing.T) {
	return
	nodes, tasks := TgenerateCase()

	// Instantiate a GA with a GAConfig
	var ga, err = eaopt.NewDefaultGAConfig().NewGA()
	if err != nil {
		log.Println(err)
		return
	}

	// Set the number of generations to run for
	ga.NGenerations = math.MaxInt32
	ga.NPops = 1
	ga.PopSize = 30 + uint(len(tasks)/2)

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

	var f = func(rng *rand.Rand) eaopt.Genome {
		allocation := Allocation{TasksOnNode: map[string][]Task{}, Nodes: map[string]NodeStatus{}, Flags: map[string]bool{"valid": true}}

		//log.Println(nodes)
		var nodesT []NodeStatus
		for _, node := range nodes {
			nodesT = append(nodesT, node.Copy())
		}

		//nodesT[0].Status[0].MemoryAllocated = 100
		//log.Println(nodes[0].Status[0].MemoryAllocated)

		//log.Println(&nodesT[0])
		//log.Println(&nodes[0])

		for _, node := range nodesT {
			allocation.Nodes[node.ClientID] = node
		}
		for _, task := range tasks {
			allocation.Tasks = append(allocation.Tasks, task)
		}

		/* shuffle */
		for n := len(tasks); n > 0; n-- {
			randIndex := rng.Intn(n)
			allocation.Tasks[n-1], allocation.Tasks[randIndex] = allocation.Tasks[randIndex], allocation.Tasks[n-1]
		}

		/* pick nodes */
		for _, node := range nodesT {
			allocation.Nodes[node.ClientID] = node
			allocation.NodeIDs = append(allocation.NodeIDs, node.ClientID)
		}

		t := rng.Int() % 10
		if t == 0 {
			/* best-fit */
			ts := time.Now()
			allocation.TasksOnNode = fastBestFit(nodesT, tasks).TasksOnNode
			log.Println(time.Since(ts))
			//fmt.Println("Best Fit")
		} else if t%2 == 0 {
			/* first-fit */
			for _, task := range tasks {
				if nodeID, ok := randomFit(allocation, task); ok {
					allocation.TasksOnNode[nodeID] = append(allocation.TasksOnNode[nodeID], task)
					for i := range allocation.Nodes[nodeID].Status {
						if allocation.Nodes[nodeID].Status[i].MemoryAllocated == 0 {
							allocation.Nodes[nodeID].Status[i].MemoryAllocated += task.MemoryGPU
							break
						}
					}
				} else {
					allocation.Flags["valid"] = false
				}
			}
		} else {
			/* random-fit */
			for _, task := range tasks {
				if nodeID, ok := randomFit(allocation, task); ok {
					allocation.TasksOnNode[nodeID] = append(allocation.TasksOnNode[nodeID], task)
					for i := range allocation.Nodes[nodeID].Status {
						if allocation.Nodes[nodeID].Status[i].MemoryAllocated == 0 {
							allocation.Nodes[nodeID].Status[i].MemoryAllocated += task.MemoryGPU
							break
						}
					}
				} else {
					allocation.Flags["valid"] = false
				}
			}
		}
		//fmt.Println(evaluatue(allocation))
		//fmt.Println(allocation)
		return allocation

	}

	// Find the minimum
	err = ga.Minimize(f)
	log.Println(time.Since(ts))
	log.Println(ga.HallOfFame[0].Genome.(Allocation).TasksOnNode)
	//log.Println(ga.HallOfFame[0].Genome.(Allocation).Flags)
	//log.Println(ga.HallOfFame[0].Genome.(Allocation).Nodes)
	if err != nil {
		log.Println(err)
		return
	}
}
