package main

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

	costLoad float64
}

func (eva *Evaluator) init(nodes []NodeStatus, tasks []Task) {
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
	eva.costLoad = 0.0
}

func (eva *Evaluator) add(node NodeStatus, task Task) {
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

	if task.IsPS {
		//eva.costLoad += 1
	} else {
		//eva.costLoad += 0.5
	}
	numberGPU := 1
	for _, gpu := range node.Status {
		if gpu.MemoryAllocated != 0 {
			numberGPU += 1
		}
	}
	eva.costLoad += float64(numberGPU) / float64(len(node.Status))

}

func (eva *Evaluator) remove(node NodeStatus, task Task) {
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

	if task.IsPS {
		//eva.costLoad -= 1
	} else {
		//eva.costLoad -= 0.5
	}
	numberGPU := 1
	for _, gpu := range node.Status {
		if gpu.MemoryAllocated != 0 {
			numberGPU += 1
		}
	}
	eva.costLoad -= float64(numberGPU) / float64(len(node.Status))
}

func (eva *Evaluator) calculate() float64 {
	return eva.cost + eva.costLoad/float64(eva.totalPS+eva.totalWorker)
}

func evaluate(allocation Allocation) float64 {
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
		numberGPU := 0
		for _, gpu := range node.Status {
			if gpu.MemoryAllocated == 0 {
				numberGPU += 0
			}
		}
		costGPU += (float64(requestGPU + numberGPU)) / float64(len(node.Status)) * 3.0
		costLB += (costCPU + costMem + costBW + costGPU) / (1.0 + 1.0 + 2.0 + 3.0)
	}
	costLB /= float64(len(allocation.TasksOnNode))
	costLB *= 100
	//fmt.Println(costLB)

	cost := costNetwork
	//cost := 0.0*costLB + 1.0*costNetwork
	return cost
}
