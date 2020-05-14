package main

import (
	"fmt"
	"math/rand"
	"github.com/MaxHalford/eaopt"
	"time"
	"strconv"
	"math"
)

var nodesMap map[string]Node
var tasksMap map[string]Task

type Node struct {
	ClientID string `json:"id"`
	Domain   int    `json:"domain"`
	Rack     int    `json:"rack"`
}

type Task struct {
	Name string `json:"name"`
	IsPS bool   `json:"is_ps"`
}

// An valid allocation
type Allocation struct {
	TasksOnNode map[string][]Task // tasks on nodes[id]
	Nodes       map[string]Node
	Valid       map[string]bool
}

// Evaluate a Vector with the Drop-Wave function which takes two variables as
// input and reaches a minimum of -1 in (0, 0). The function is simple so there
// isn't any error handling to do.
func (X Allocation) Evaluate() (float64, error) {
	if !X.Valid["flag"] {
		fmt.Println("Invalid allocation")
		return math.MaxFloat64, nil
	}
	cost := 0
	taskToNode := map[string]string{}
	for id, tasks := range X.TasksOnNode {
		for _, task := range tasks {
			taskToNode[task.Name] = id
		}
	}

	for taskI, nodeI := range taskToNode {
		for taskJ, nodeJ := range taskToNode {
			if taskI == taskJ {
				continue
			}
			if tasksMap[taskI].IsPS == tasksMap[taskJ].IsPS {
				continue
			}
			if X.Nodes[nodeI].Domain != X.Nodes[nodeJ].Domain {
				cost += 4
			} else if X.Nodes[nodeI].Rack != X.Nodes[nodeJ].Rack {
				cost += 2
			} else if nodeI != nodeJ {
				cost += 1
			} else {
				cost += 0
			}
		}
	}

	//fmt.Println(taskToNode, cost/2, len(X.Nodes))
	return float64(cost / 2), nil
}

// Mutate a Vector by resampling each element from a normal distribution with
// probability 0.8.
func (X Allocation) Mutate(rng *rand.Rand) {
	if !X.Valid["flag"] {
		fmt.Println("Invalid allocation")
		return
	}
	//fmt.Println("Mutate")
	fmt.Println("Before", X)

	/* decrease node */
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

	fmt.Println(tasks)

	/* first-fit */
	for _, task := range tasks {
		flag := false
		for nodeID3 := range X.Nodes {
			if len(X.TasksOnNode[nodeID3]) < 3 {
				X.TasksOnNode[nodeID3] = append(X.TasksOnNode[nodeID3], task)
				flag = true
				break
			}
		}
		if !flag {
			X.Valid["flag"] = false
		}
	}
	fmt.Println("After", X)

	/* exchange tasks */
	//randIndexM := rng.Intn(len(X))
	//randIndexN := rng.Intn(len(X))
	//X[randIndexM].Tasks, X[randIndexN].Tasks = X[randIndexN].Tasks, X[randIndexM].Tasks
}

// Crossover a Vector with another Vector by applying uniform crossover.
func (X Allocation) Crossover(Y eaopt.Genome, rng *rand.Rand) {
	//fmt.Println("Crossover")
	if !Y.(Allocation).Valid["flag"] || !X.Valid["flag"] {
		return
	}
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
		flag := false
		for nodeID3 := range X.Nodes {
			if len(X.TasksOnNode[nodeID3]) < 3 {
				X.TasksOnNode[nodeID3] = append(X.TasksOnNode[nodeID3], task)
				flag = true
				break
			}
		}
		if !flag {
			X.Valid["flag"] = false
		}
	}
	//fmt.Println()
	//fmt.Println("crossover", X.TasksOnNode)
}

// Clone a Vector to produce a new one that points to a different slice.
func (X Allocation) Clone() eaopt.Genome {
	if !X.Valid["flag"] {
		//fmt.Println(X.Valid)
	}
	Y := Allocation{TasksOnNode: map[string][]Task{}, Nodes: map[string]Node{}, Valid: map[string]bool{"flag": X.Valid["flag"]}}
	for id, node := range X.Nodes {
		Y.Nodes[id] = node
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

// VectorFactory returns a random vector by generating 2 values uniformally
// distributed between -10 and 10.
func VectorFactory(rng *rand.Rand) eaopt.Genome {
	allocation := Allocation{TasksOnNode: map[string][]Task{}, Nodes: map[string]Node{}, Valid: map[string]bool{"flag": true}}

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
	}

	/* first-fit */
	for _, task := range tasks {
		flag := false
		for id, _ := range allocation.Nodes {
			if len(allocation.TasksOnNode[id]) < 3 {
				allocation.TasksOnNode[id] = append(allocation.TasksOnNode[id], task)
				flag = true
				break
			}
		}
		if !flag {
			allocation.Valid["flag"] = false
		}
	}
	//fmt.Println(allocation)
	return allocation
}

func main() {
	numTask := 100

	nodesMap = map[string]Node{}
	tasksMap = map[string]Task{}

	for i := 0; i < numTask*3; i++ {
		nodesMap[strconv.Itoa(i)] = Node{ClientID: strconv.Itoa(i), Rack: i % 2, Domain: i % 1}
	}
	for i := 0; i < numTask; i++ {
		isPS := false
		if i%5 == 0 {
			isPS = true
		}
		tasksMap[strconv.Itoa(i)] = Task{Name: strconv.Itoa(i), IsPS: isPS}
	}

	// Instantiate a GA with a GAConfig
	var ga, err = eaopt.NewDefaultGAConfig().NewGA()
	if err != nil {
		fmt.Println(err)
		return
	}

	// Set the number of generations to run for
	ga.NGenerations = 4000
	ga.NPops = 1
	ga.PopSize = 20

	// Add a custom print function to track progress
	ga.Callback = func(ga *eaopt.GA) {
		fmt.Printf("Best fitness at generation %d: %f\n", ga.Generations, ga.HallOfFame[0].Fitness)
	}

	bestFitness := -1.0
	count := 0

	ga.EarlyStop = func(ga *eaopt.GA) bool {
		gap := math.Abs(ga.HallOfFame[0].Fitness - bestFitness)
		if gap <= 0.000001 {
			if count >= 20 {
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
	ts := time.Now()
	err = ga.Minimize(VectorFactory)
	fmt.Println(time.Since(ts))
	if err != nil {
		fmt.Println(err)
		return
	}
}
