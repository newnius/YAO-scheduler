package main

import (
	"math/rand"
	"github.com/MaxHalford/eaopt"
	"time"
	"strconv"
	"math"
	log "github.com/sirupsen/logrus"
)

var nodesMap map[string]NodeStatus
var tasksMap map[string]Task

// A resource allocation
type Allocation struct {
	TasksOnNode map[string][]Task // tasks on nodes[id]
	Nodes       map[string]NodeStatus
	NodeIDs     []string
	Flags       map[string]bool
	Evaluator   Evaluator
}

func randomFit(allocation Allocation, task Task) (string, bool) {
	flag := false
	nodeID := ""
	for nodeID = range allocation.Nodes {
		numberGPU := 0
		for _, gpu := range allocation.Nodes[nodeID].Status {
			if gpu.MemoryAllocated == 0 {
				numberGPU += 0
			}
		}
		if _, ok := allocation.Nodes[nodeID]; ok && len(allocation.TasksOnNode[nodeID]) < numberGPU {
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
		numberGPU := 0
		for _, gpu := range allocation.Nodes[nodeID].Status {
			if gpu.MemoryAllocated == 0 {
				numberGPU += 0
			}
		}
		if _, ok := allocation.Nodes[nodeID]; ok && len(allocation.TasksOnNode[nodeID]) < numberGPU {
			flag = true
			break
		}
	}
	return nodeID, flag
}

func fastBestFit(nodes []NodeStatus, tasks []Task) Allocation {
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
			numberGPU := 0
			for _, gpu := range allocation.Nodes[nodeID].Status {
				if gpu.MemoryAllocated == 0 {
					numberGPU += 0
				}
			}
			if len(allocation.TasksOnNode[node.ClientID]) >= numberGPU {
				continue
			}
			eva.add(node, task)
			cost := eva.calculate()
			eva.remove(node, task)
			if cost < minCost || nodeID == "" {
				minCost = cost
				nodeID = node.ClientID
			}
			//fmt.Println(cost)
		}
		if nodeID == "" {
			allocation.Flags["valid"] = false
			break
		} else {
			//fmt.Println(task, nodeID, allocation.TasksOnNode, minCost)
			allocation.TasksOnNode[nodeID] = append(allocation.TasksOnNode[nodeID], task)
			eva.add(nodesMap[nodeID], task)
		}
	}
	log.Println(eva.calculate())
	return allocation
}

func bestFit(allocation Allocation, task Task) (string, bool) {
	flag := false
	nodeID := ""
	minCost := math.MaxFloat64
	for _, id := range allocation.NodeIDs {
		numberGPU := 0
		for _, gpu := range allocation.Nodes[id].Status {
			if gpu.MemoryAllocated == 0 {
				numberGPU += 0
			}
		}
		if _, ok := allocation.Nodes[id]; ok && len(allocation.TasksOnNode[id]) < numberGPU {
			/* add */
			allocation.TasksOnNode[id] = append(allocation.TasksOnNode[id], task)

			/* evaluate */
			cost := evaluate(allocation)

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

/* Evaluate the allocation */
func (X Allocation) Evaluate() (float64, error) {
	if !X.Flags["valid"] {
		//fmt.Println("Invalid allocation")
		return math.MaxFloat64, nil
	}

	costNetwork := evaluate(X)

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

	/* move tasks */
	if !X.Flags["valid"] {
		//fmt.Println("Invalid allocation")
		return
	}
	var nodeIDs []string
	for nodeID := range X.Nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	randIndex1 := rng.Intn(len(nodeIDs))
	nodeID1 := nodeIDs[randIndex1]
	if tasks, ok := X.TasksOnNode[nodeID1]; ok && len(tasks) > 0 {
		idx := rng.Intn(len(tasks))
		task := tasks[idx]
		copy(X.TasksOnNode[nodeID1][idx:], X.TasksOnNode[nodeID1][idx+1:])
		X.TasksOnNode[nodeID1] = X.TasksOnNode[nodeID1][:len(X.TasksOnNode[nodeID1])-1]

		if nodeID, ok := firstFit(X, task); ok {
			X.TasksOnNode[nodeID] = append(X.TasksOnNode[nodeID], task)
		} else {
			X.Flags["valid"] = false
		}
	}

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
				log.Println("Error", taskToNode, X.TasksOnNode, task.Name)
			}
			for i, task2 := range X.TasksOnNode[nodeID2] {
				if task2.Name == task.Name {
					idx = i
				}
			}
			if idx == -1 {
				log.Println("Error 2", taskToNode, X.TasksOnNode, task.Name)
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
	Y := Allocation{TasksOnNode: map[string][]Task{}, Nodes: map[string]NodeStatus{}, Flags: map[string]bool{"valid": X.Flags["valid"]}}
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
	allocation := Allocation{TasksOnNode: map[string][]Task{}, Nodes: map[string]NodeStatus{}, Flags: map[string]bool{"valid": true}}

	var nodes []NodeStatus
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
	if t == -1 {
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
		log.Println(time.Since(ts))
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

func testGA() {
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
