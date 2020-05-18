package main

import (
	"fmt"
	"math/rand"
	"github.com/MaxHalford/eaopt"
	"time"
	"strconv"
	"math"
)

type Evaluator struct {
	domains     map[string]map[string]int
	racks       map[string]map[string]int
	nodes       map[string]map[string]int
	upstreams   map[string]string
	cost        float64
	totalPS     int
	totalWorker int

	costNetwork float64

	factorNode   float64
	factorRack   float64
	factorDomain float64
}

func (eva *Evaluator) init(nodes []Node, tasks []Task) {
	eva.domains = map[string]map[string]int{}
	eva.racks = map[string]map[string]int{}
	eva.nodes = map[string]map[string]int{}
	eva.upstreams = map[string]string{}
	eva.totalPS = 0
	eva.totalWorker = 0
	eva.factorNode = 1.0
	eva.factorRack = 4.0
	eva.factorDomain = 40.0
	eva.cost = 0.0
	eva.costNetwork = 0.0
}

func (eva *Evaluator) add(node Node, task Task) {
	/* update node load cost */

	/* update network cost */
	if _, ok := eva.nodes[node.ClientID]; !ok {
		eva.nodes[node.ClientID] = map[string]int{"PS": 0, "Worker": 0}
	}
	if _, ok := eva.racks[node.Rack]; !ok {
		eva.racks[node.Rack] = map[string]int{"PS": 0, "Worker": 0}
	}
	if _, ok := eva.domains[node.Domain]; !ok {
		eva.domains[node.Domain] = map[string]int{"PS": 0, "Worker": 0}
	}
	if task.IsPS {
		eva.costNetwork += eva.factorNode * float64(eva.racks[node.Rack]["Worker"]-eva.nodes[node.ClientID]["Worker"])
		eva.costNetwork += eva.factorRack * float64(eva.domains[node.Domain]["Worker"]-eva.racks[node.Rack]["Worker"])
		eva.costNetwork += eva.factorDomain * float64(eva.totalWorker-eva.domains[node.Domain]["Worker"])

		eva.nodes[node.ClientID]["PS"]++
		eva.racks[node.Rack]["PS"]++
		eva.domains[node.Domain]["PS"]++
		eva.totalPS++
	} else {
		eva.costNetwork += eva.factorNode * float64(eva.racks[node.Rack]["PS"]-eva.nodes[node.ClientID]["PS"])
		eva.costNetwork += eva.factorRack * float64(eva.domains[node.Domain]["PS"]-eva.racks[node.Rack]["PS"])
		eva.costNetwork += eva.factorDomain * float64(eva.totalPS-eva.domains[node.Domain]["PS"])

		eva.nodes[node.ClientID]["Worker"]++
		eva.racks[node.Rack]["Worker"]++
		eva.domains[node.Domain]["Worker"]++
		eva.totalWorker++
	}
	eva.cost = eva.costNetwork
}

func (eva *Evaluator) remove(node Node, task Task) {
	if task.IsPS {
		eva.costNetwork -= eva.factorNode * float64(eva.racks[node.Rack]["Worker"]-eva.nodes[node.ClientID]["Worker"])
		eva.costNetwork -= eva.factorRack * float64(eva.domains[node.Domain]["Worker"]-eva.racks[node.Rack]["Worker"])
		eva.costNetwork -= eva.factorDomain * float64(eva.totalWorker-eva.domains[node.Domain]["Worker"])

		eva.nodes[node.ClientID]["PS"]--
		eva.racks[node.Rack]["PS"]--
		eva.domains[node.Domain]["PS"]--
		eva.totalPS--
	} else {
		eva.costNetwork -= eva.factorNode * float64(eva.racks[node.Rack]["PS"]-eva.nodes[node.ClientID]["PS"])
		eva.costNetwork -= eva.factorRack * float64(eva.domains[node.Domain]["PS"]-eva.racks[node.Rack]["PS"])
		eva.costNetwork -= eva.factorDomain * float64(eva.totalPS-eva.domains[node.Domain]["PS"])

		//fmt.Println(eva.totalWorker, eva.domains[node.Domain])

		eva.nodes[node.ClientID]["Worker"]--
		eva.racks[node.Rack]["Worker"]--
		eva.domains[node.Domain]["Worker"]--
		eva.totalWorker--
	}
	eva.cost = eva.costNetwork
}

func (eva *Evaluator) calculate() float64 {
	return eva.cost
}

var nodesMap map[string]Node
var tasksMap map[string]Task

type Node struct {
	ClientID     string  `json:"id"`
	Domain       string  `json:"domain"`
	Rack         string  `json:"rack"`
	Version      float64 `json:"version"`
	NumCPU       int     `json:"cpu_num"`
	UtilCPU      float64 `json:"cpu_load"`
	MemTotal     int     `json:"mem_total"`
	MemAvailable int     `json:"mem_available"`
	UsingBW      float64 `json:"bw_using"`
	TotalBW      float64 `json:"bw_total"`
	numberGPU    int
	//Status     []GPUStatus `json:"status"`
}

type Task struct {
	Name      string `json:"name"`
	Image     string `json:"image"`
	Cmd       string `json:"cmd"`
	NumberCPU int    `json:"cpu_number"`
	Memory    int    `json:"memory"`
	NumberGPU int    `json:"gpu_number"`
	MemoryGPU int    `json:"gpu_memory"`
	IsPS      bool   `json:"is_ps"`
	ModelGPU  string `json:"gpu_model"`
}

// An valid allocation
type Allocation struct {
	TasksOnNode map[string][]Task // tasks on nodes[id]
	Nodes       map[string]Node
	NodeIDs     []string
	Flags       map[string]bool
	Evaluator   Evaluator
}

func randomFit(allocation Allocation, task Task) (string, bool) {
	flag := false
	nodeID := ""
	for nodeID = range allocation.Nodes {
		if node, ok := allocation.Nodes[nodeID]; ok && len(allocation.TasksOnNode[nodeID]) < node.numberGPU {
			flag = true
			break
		}
	}
	return nodeID, flag
}

func firstFit(allocation Allocation, task Task) (string, bool) {
	flag := false
	nodeID := ""
	for _, nodeID = range allocation.NodeIDs {
		if node, ok := allocation.Nodes[nodeID]; ok && len(allocation.TasksOnNode[nodeID]) < node.numberGPU {
			flag = true
			break
		}
	}
	return nodeID, flag
}

func fastBestFit(nodes []Node, tasks []Task) Allocation {
	eva := Evaluator{}
	eva.init(nodes, tasks)

	allocation := Allocation{Flags: map[string]bool{"valid": true}}
	allocation.TasksOnNode = map[string][]Task{}
	for _, task := range tasks {
		minCost := math.MaxFloat64
		nodeID := ""
		for _, node := range nodes {
			if _, ok := allocation.TasksOnNode[node.ClientID]; !ok {
				allocation.TasksOnNode[node.ClientID] = []Task{}
			}
			if len(allocation.TasksOnNode[node.ClientID]) >= node.numberGPU {
				continue
			}
			eva.add(node, task)
			cost := eva.calculate()
			eva.remove(node, task)
			if cost < minCost || nodeID == "" {
				minCost = cost
				nodeID = node.ClientID
			}
			fmt.Println(cost)
		}
		if nodeID == "" {
			allocation.Flags["valid"] = false
			break
		} else {
			fmt.Println(task, nodeID, allocation.TasksOnNode, minCost)
			allocation.TasksOnNode[nodeID] = append(allocation.TasksOnNode[nodeID], task)
			eva.add(nodesMap[nodeID], task)
		}
	}
	fmt.Println(eva.calculate())
	return allocation
}

func bestFit(allocation Allocation, task Task) (string, bool) {
	flag := false
	nodeID := ""
	minCost := math.MaxFloat64
	for _, id := range allocation.NodeIDs {
		if node, ok := allocation.Nodes[id]; ok && len(allocation.TasksOnNode[id]) < node.numberGPU {
			/* add */
			allocation.TasksOnNode[id] = append(allocation.TasksOnNode[id], task)

			/* evaluate */
			cost := evaluatue(allocation)

			/* revert */
			idx := -1
			for i, task2 := range allocation.TasksOnNode[id] {
				if task2.Name == task.Name {
					idx = i
				}
			}
			copy(allocation.TasksOnNode[id][idx:], allocation.TasksOnNode[id][idx+1:])
			allocation.TasksOnNode[id] = allocation.TasksOnNode[id][:len(allocation.TasksOnNode[id])-1]

			if cost < minCost || !flag {
				nodeID = id
				minCost = cost
			}
			flag = true
		}
	}
	return nodeID, flag
}

func evaluatue(allocation Allocation) float64 {
	/* Calculate cost for network */
	costNetwork := 0.0
	domains := map[string]map[string]int{}
	racks := map[string]map[string]int{}
	upstreams := map[string]string{}
	totalPS := 0
	totalWorker := 0

	taskToNode := map[string]string{}
	for nodeID, tasks := range allocation.TasksOnNode {
		numPS := 0
		numWorker := 0
		node := allocation.Nodes[nodeID]
		for _, task := range tasks {
			taskToNode[task.Name] = nodeID

			if _, ok := domains[node.Domain]; !ok {
				domains[node.Domain] = map[string]int{"PS": 0, "Worker": 0}
			}
			if _, ok := racks[node.Rack]; !ok {
				racks[node.Rack] = map[string]int{"PS": 0, "Worker": 0}
			}

			if task.IsPS {
				domains[node.Domain]["PS"]++
				racks[node.Rack]["PS"]++
				numPS++
				totalPS++
			} else {
				domains[node.Domain]["Worker"]++
				racks[node.Rack]["Worker"]++
				numWorker++
				totalWorker++
			}
			upstreams[node.Rack] = node.Domain
		}
		costNetwork -= float64(numPS * numWorker)
	}

	/* in the same domain */
	for rackID, pair := range racks {
		// in the same rack
		costNetwork += float64(pair["PS"]*pair["Worker"]) * 1.0
		// cross rack, but in the same domain
		costNetwork += float64(pair["PS"]*(domains[upstreams[rackID]]["Worker"]-pair["Worker"])) * 4.0
	}

	/* across domain */
	for _, pair := range domains {
		costNetwork += float64(pair["PS"]*(totalWorker-pair["Worker"])) * 40.0
	}

	/* calculate cost for node fitness */
	//cpu, memory, bw
	costLB := 0.0
	for nodeID, tasks := range allocation.TasksOnNode {
		costCPU := 0.0
		costMem := 0.0
		costBW := 0.0
		costGPU := 0.0
		requestCPU := 0
		requestMem := 0
		requestBW := 0.0
		requestGPU := 0
		numberPS := 0
		numberWorker := 0
		for _, task := range tasks {
			requestCPU += task.NumberCPU
			requestMem += task.Memory
			requestGPU += task.NumberGPU
			if task.IsPS {
				numberPS++
			} else {
				numberWorker++
			}
		}
		requestBW = float64(numberPS*(totalWorker-numberWorker) + numberWorker*(totalPS-numberPS))
		node := allocation.Nodes[nodeID]
		costCPU += (float64(requestCPU) + node.UtilCPU) / float64(node.NumCPU) * 1.0
		costMem += (float64(requestMem + (node.MemTotal - node.MemAvailable))) / float64(node.MemTotal) * 1.0
		costBW += (float64(requestBW) + (node.TotalBW - node.UsingBW)) / node.TotalBW * 2.0
		costGPU += (float64(requestGPU + node.numberGPU)) / float64(node.numberGPU) * 3.0
		costLB += (costCPU + costMem + costBW + costGPU) / (1.0 + 1.0 + 2.0 + 3.0)
	}
	costLB /= float64(len(allocation.TasksOnNode))
	costLB *= 100
	//fmt.Println(costLB)

	cost := 0.0*costLB + 1.0*costNetwork
	return cost
}

/* Evaluate the allocation */
func (X Allocation) Evaluate() (float64, error) {
	if !X.Flags["valid"] {
		//fmt.Println("Invalid allocation")
		return math.MaxFloat64, nil
	}

	costNetwork := evaluatue(X)

	cost := costNetwork
	//fmt.Println(taskToNode, cost, len(X.Nodes))
	return float64(cost), nil
}

// Mutate a Vector by resampling each element from a normal distribution with
// probability 0.8.
func (X Allocation) Mutate(rng *rand.Rand) {
	/* remove a node randomly */
	// make sure rng.Intn != 0 && cnt >0
	cnt := rng.Intn(1+len(X.Nodes)/100)%50 + 1
	for i := 0; i < cnt; i++ {
		if !X.Flags["valid"] {
			//fmt.Println("Invalid allocation")
			return
		}
		//fmt.Println("Mutate")
		//fmt.Println("Before", X)

		var nodeIDs []string
		for nodeID := range X.Nodes {
			nodeIDs = append(nodeIDs, nodeID)
		}
		randIndex := rng.Intn(len(X.Nodes))
		nodeID := nodeIDs[randIndex]

		/* reschedule tasks on tgt node */
		var tasks []Task
		if _, ok := X.TasksOnNode[nodeID]; ok {
			for _, task := range X.TasksOnNode[nodeID] {
				tasks = append(tasks, task)
			}
			delete(X.TasksOnNode, nodeID)
		}
		delete(X.Nodes, nodeID)

		//fmt.Println(tasks)

		/* first-fit */
		for _, task := range tasks {
			if nodeID, ok := firstFit(X, task); ok {
				X.TasksOnNode[nodeID] = append(X.TasksOnNode[nodeID], task)
			} else {
				X.Flags["valid"] = false
			}
		}
	}
	//fmt.Println("After", X)

	/* exchange tasks */
	if !X.Flags["valid"] {
		//fmt.Println("Invalid allocation")
		return
	}
	var nodeIDs []string
	for nodeID := range X.Nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	randIndex1 := rng.Intn(len(nodeIDs))
	randIndex2 := rng.Intn(len(nodeIDs))
	nodeID1 := nodeIDs[randIndex1]
	nodeID2 := nodeIDs[randIndex2]
	X.TasksOnNode[nodeID1], X.TasksOnNode[nodeID2] = X.TasksOnNode[nodeID2], X.TasksOnNode[nodeID1]
}

// Crossover a Vector with another Vector by applying uniform crossover.
func (X Allocation) Crossover(Y eaopt.Genome, rng *rand.Rand) {
	// make sure rng.Intn != 0 && cnt >0
	cnt := rng.Intn(1+len(X.Nodes)/100)%10 + 1
	for i := 0; i < cnt; i++ {
		if !Y.(Allocation).Flags["valid"] || !X.Flags["valid"] {
			return
		}
		//fmt.Println("Crossover")
		taskToNode := map[string]string{}
		for nodeID, tasks := range X.TasksOnNode {
			for _, task := range tasks {
				taskToNode[task.Name] = nodeID
			}
		}

		var nodeIDs []string
		for nodeID := range Y.(Allocation).Nodes {
			nodeIDs = append(nodeIDs, nodeID)
		}

		//fmt.Println(nodeIDs, Y.(Allocation))
		randIndex := rng.Intn(len(nodeIDs))
		nodeID := nodeIDs[randIndex]

		for _, task := range Y.(Allocation).TasksOnNode[nodeID] {
			//fmt.Println(Y.(Allocation).TasksOnNode[nodeID])
			idx := -1
			nodeID2, ok := taskToNode[task.Name]
			if !ok {
				fmt.Println("Error", taskToNode, X.TasksOnNode, task.Name)
			}
			for i, task2 := range X.TasksOnNode[nodeID2] {
				if task2.Name == task.Name {
					idx = i
				}
			}
			if idx == -1 {
				fmt.Println("Error 2", taskToNode, X.TasksOnNode, task.Name)
			}
			//fmt.Println(X.TasksOnNode)
			copy(X.TasksOnNode[nodeID2][idx:], X.TasksOnNode[nodeID2][idx+1:])
			X.TasksOnNode[nodeID2] = X.TasksOnNode[nodeID2][:len(X.TasksOnNode[nodeID2])-1]
			//fmt.Println(X.TasksOnNode)
		}
		/* reschedule tasks on tgt node */
		var tasks []Task
		if _, ok := X.TasksOnNode[nodeID]; ok {
			for _, task := range X.TasksOnNode[nodeID] {
				tasks = append(tasks, task)
			}
			delete(X.TasksOnNode, nodeID)
		}

		if _, ok := X.Nodes[nodeID]; ok {
			delete(X.Nodes, nodeID)
		}
		X.Nodes[nodeID] = Y.(Allocation).Nodes[nodeID]

		var newTasksOnNode []Task
		for _, task := range Y.(Allocation).TasksOnNode[nodeID] {
			newTasksOnNode = append(newTasksOnNode, task)
		}
		X.TasksOnNode[nodeID] = newTasksOnNode

		/* first-fit */
		for _, task := range tasks {
			if nodeID, ok := firstFit(X, task); ok {
				X.TasksOnNode[nodeID] = append(X.TasksOnNode[nodeID], task)
			} else {
				X.Flags["valid"] = false
			}
		}
	}
	//fmt.Println()
	//fmt.Println("crossover", X.TasksOnNode)
}

// Clone a Vector to produce a new one that points to a different slice.
func (X Allocation) Clone() eaopt.Genome {
	if !X.Flags["valid"] {
		//fmt.Println(X.Valid)
	}
	Y := Allocation{TasksOnNode: map[string][]Task{}, Nodes: map[string]Node{}, Flags: map[string]bool{"valid": X.Flags["valid"]}}
	for id, node := range X.Nodes {
		Y.Nodes[id] = node
		Y.NodeIDs = append(Y.NodeIDs, node.ClientID)
	}
	for id, tasks := range X.TasksOnNode {
		var t []Task
		for _, task := range tasks {
			t = append(t, task)
		}
		Y.TasksOnNode[id] = t
	}
	return Y
}

func VectorFactory(rng *rand.Rand) eaopt.Genome {
	allocation := Allocation{TasksOnNode: map[string][]Task{}, Nodes: map[string]Node{}, Flags: map[string]bool{"valid": true}}

	var nodes []Node
	var tasks []Task

	for _, node := range nodesMap {
		nodes = append(nodes, node)
	}
	for _, task := range tasksMap {
		tasks = append(tasks, task)
	}

	/* shuffle */
	for n := len(nodes); n > 0; n-- {
		randIndex := rng.Intn(n)
		nodes[n-1], nodes[randIndex] = nodes[randIndex], nodes[n-1]
	}
	for n := len(tasks); n > 0; n-- {
		randIndex := rng.Intn(n)
		tasks[n-1], tasks[randIndex] = tasks[randIndex], tasks[n-1]
	}

	/* pick nodes */
	for _, node := range nodesMap {
		allocation.Nodes[node.ClientID] = node
		allocation.NodeIDs = append(allocation.NodeIDs, node.ClientID)
	}

	t := rng.Int() % 10
	if t == 0 {
		/* best-fit */
		ts := time.Now()

		/*
		for _, task := range tasks {
			if nodeID, ok := bestFit(allocation, task); ok {
				allocation.TasksOnNode[nodeID] = append(allocation.TasksOnNode[nodeID], task)
			} else {
				allocation.Flags["valid"] = false
			}
		}
		*/

		allocation.TasksOnNode = fastBestFit(nodes, tasks).TasksOnNode
		fmt.Println(time.Since(ts))
		//fmt.Println("Best Fit")
	} else if t%2 == 0 {
		/* first-fit */
		for _, task := range tasks {
			if nodeID, ok := randomFit(allocation, task); ok {
				allocation.TasksOnNode[nodeID] = append(allocation.TasksOnNode[nodeID], task)
			} else {
				allocation.Flags["valid"] = false
			}
		}
	} else {
		/* random-fit */
		for _, task := range tasks {
			if nodeID, ok := randomFit(allocation, task); ok {
				allocation.TasksOnNode[nodeID] = append(allocation.TasksOnNode[nodeID], task)
			} else {
				allocation.Flags["valid"] = false
			}
		}
	}
	//fmt.Println(evaluatue(allocation))
	//fmt.Println(allocation)
	return allocation
}

func main() {
	numTask := 5

	nodesMap = map[string]Node{}
	tasksMap = map[string]Task{}

	for i := 0; i < numTask*3; i++ {
		node := Node{ClientID: strconv.Itoa(i), Rack: strconv.Itoa(i % 40), Domain: strconv.Itoa(i % 4)}
		node.NumCPU = 24
		node.MemTotal = 188
		node.TotalBW = 100
		node.numberGPU = rand.Intn(8) + 1
		nodesMap[strconv.Itoa(i)] = node
	}
	for i := 0; i < numTask; i++ {
		isPS := false
		if i%5 == 0 {
			isPS = true
		}
		task := Task{Name: strconv.Itoa(i), IsPS: isPS}
		task.Memory = 4
		task.NumberCPU = 2
		task.NumberGPU = 1
		tasksMap[strconv.Itoa(i)] = task
	}

	var nodes []Node
	var tasks []Task

	for _, node := range nodesMap {
		nodes = append(nodes, node)
	}
	for _, task := range tasksMap {
		tasks = append(tasks, task)
	}
	s := time.Now()
	fmt.Println(fastBestFit(nodes, tasks))
	fmt.Println(time.Since(s))

	// Instantiate a GA with a GAConfig
	var ga, err = eaopt.NewDefaultGAConfig().NewGA()
	if err != nil {
		fmt.Println(err)
		return
	}

	// Set the number of generations to run for
	ga.NGenerations = math.MaxInt32
	ga.NPops = 1
	ga.PopSize = 30 + uint(numTask/2)

	// Add a custom print function to track progress
	ga.Callback = func(ga *eaopt.GA) {
		fmt.Printf("Best fitness at generation %d: %f\n", ga.Generations, ga.HallOfFame[0].Fitness)
	}

	bestFitness := math.MaxFloat64
	count := 0

	ts := time.Now()

	ga.EarlyStop = func(ga *eaopt.GA) bool {
		gap := math.Abs(ga.HallOfFame[0].Fitness - bestFitness)
		if gap <= 0.000001 {
			if count >= 30 || time.Since(ts) > time.Second*30 {
				fmt.Println("Early Stop")
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
	fmt.Println(time.Since(ts))
	//fmt.Println(ga.HallOfFame[0].Genome.(Allocation).TasksOnNode)
	//fmt.Println(ga.HallOfFame[0].Genome.(Allocation).Nodes)
	if err != nil {
		fmt.Println(err)
		return
	}
}
