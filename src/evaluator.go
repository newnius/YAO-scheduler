package main

type Evaluator struct {
	domains     map[string]map[string]int
	racks       map[string]map[string]int
	nodes       map[string]map[string]int
	upstreams   map[string]string
	totalPS     int
	totalWorker int

	costNetwork float64
	costLoad    float64

	factorNode   float64
	factorRack   float64
	factorDomain float64

	factorSpread float64
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
	eva.costNetwork = 0.0
	eva.costLoad = 0.0
	eva.factorSpread = -1.0
}

func (eva *Evaluator) add(node NodeStatus, task Task) {
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

	/* update node load cost */
	numberGPU := 1
	for _, gpu := range node.Status {
		if gpu.MemoryAllocated != 0 {
			numberGPU += 1
		}
	}
	eva.costLoad += float64(numberGPU+task.NumberGPU) / float64(len(node.Status))

}

func (eva *Evaluator) remove(node NodeStatus, task Task) {
	/* update network cost */
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

		eva.nodes[node.ClientID]["Worker"]--
		eva.racks[node.Rack]["Worker"]--
		eva.domains[node.Domain]["Worker"]--
		eva.totalWorker--
	}

	/* update node load cost */
	numberGPU := 1
	for _, gpu := range node.Status {
		if gpu.MemoryAllocated != 0 {
			numberGPU += 1
		}
	}
	eva.costLoad -= float64(numberGPU+task.NumberGPU) / float64(len(node.Status))
}

func (eva *Evaluator) calculate() float64 {
	usingNodes := 0.0
	for _, pair := range eva.nodes {
		if v, ok := pair["PS"]; ok && v > 0 {
			usingNodes += 1.0
		} else if v, ok := pair["Worker"]; ok && v > 0 {
			usingNodes += 1.0
		}
	}
	usingNodes /= float64(eva.totalWorker + eva.totalPS)
	return eva.costNetwork + eva.factorSpread*eva.costLoad/float64(eva.totalPS+eva.totalWorker) + usingNodes
}
