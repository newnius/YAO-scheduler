package main

import (
	"math/rand"
	"github.com/MaxHalford/eaopt"
	"math"
	log "github.com/sirupsen/logrus"
)

// A resource allocation
type Allocation struct {
	TasksOnNode map[string][]Task // tasks on nodes[id]
	Nodes       map[string]NodeStatus
	NodeIDs     []string
	Flags       map[string]bool
	Evaluator   Evaluator
	Tasks       []Task
}

func randomFit(allocation Allocation, task Task) (string, bool) {
	flag := false
	nodeID := ""
	for nodeID = range allocation.Nodes {
		numberGPU := 0
		for _, gpu := range allocation.Nodes[nodeID].Status {
			if gpu.MemoryAllocated == 0 {
				numberGPU += 1
			}
		}
		if task.NumberGPU <= numberGPU {
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
		if _, ok := allocation.Nodes[nodeID]; !ok {
			continue
		}
		numberGPU := 0
		for _, gpu := range allocation.Nodes[nodeID].Status {
			if gpu.MemoryAllocated == 0 {
				numberGPU += 1
			}
		}
		if task.NumberGPU <= numberGPU {
			flag = true
			break
		}
	}
	return nodeID, flag
}

func fastBestFit(nodes []NodeStatus, tasks []Task) Allocation {
	log.Info(nodes)
	log.Info(tasks)
	eva := Evaluator{}
	eva.init(nodes, tasks)

	allocation := Allocation{Flags: map[string]bool{"valid": true}}
	allocation.TasksOnNode = map[string][]Task{}
	for _, task := range tasks {
		minCost := math.MaxFloat64
		var best *NodeStatus
		for i, node := range nodes {
			if _, ok := allocation.TasksOnNode[node.ClientID]; !ok {
				allocation.TasksOnNode[node.ClientID] = []Task{}
			}
			numberGPU := 0
			for _, gpu := range node.Status {
				if gpu.MemoryAllocated == 0 {
					numberGPU += 1
				}
			}
			if task.NumberGPU > numberGPU {
				continue
			}
			eva.add(node, task)
			cost := eva.calculate()
			eva.remove(node, task)
			//log.Info(node, cost)
			if cost < minCost || best == nil {
				minCost = cost
				best = &nodes[i]
			}
		}
		if best == nil {
			allocation.Flags["valid"] = false
			break
		} else {
			log.Info(task, " choose ", best.ClientID)
			//fmt.Println(task, nodeID, allocation.TasksOnNode, minCost)
			allocation.TasksOnNode[best.ClientID] = append(allocation.TasksOnNode[best.ClientID], task)
			eva.add(*best, task)
			for i := range best.Status {
				//allocate more than 1
				if best.Status[i].MemoryAllocated == 0 {
					best.Status[i].MemoryAllocated += task.MemoryGPU
					break
				}
			}
		}
	}
	//log.Info(allocation.TasksOnNode)
	log.Println("BestFit Cost:", eva.calculate())
	return allocation
}

/* Evaluate the allocation */
func (X Allocation) Evaluate() (float64, error) {
	//log.Info(X)
	if !X.Flags["valid"] {
		//fmt.Println("Invalid allocation")
		return math.MaxFloat64, nil
	}

	//costNetwork := evaluate(X)

	var nodes []NodeStatus
	for _, node := range X.Nodes {
		nodes = append(nodes, node)
	}

	eva := Evaluator{}
	eva.init(nodes, X.Tasks)
	for node, tasks := range X.TasksOnNode {
		for _, task := range tasks {
			eva.add(X.Nodes[node], task)
		}
	}

	cost := eva.calculate()
	//log.Info(cost)
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
				for i := range X.Nodes[nodeID].Status {
					if X.Nodes[nodeID].Status[i].MemoryAllocated == 0 {
						X.Nodes[nodeID].Status[i].MemoryAllocated += task.MemoryGPU
						break
					}
				}
			} else {
				X.Flags["valid"] = false
			}
		}
	}
	//fmt.Println("After", X)

	return
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
			for i := range X.Nodes[nodeID].Status {
				if X.Nodes[nodeID].Status[i].MemoryAllocated == 0 {
					X.Nodes[nodeID].Status[i].MemoryAllocated -= task.MemoryGPU
					break
				}
			}
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
				for i := range X.Nodes[nodeID].Status {
					if X.Nodes[nodeID].Status[i].MemoryAllocated == 0 {
						X.Nodes[nodeID].Status[i].MemoryAllocated += task.MemoryGPU
						break
					}
				}
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
