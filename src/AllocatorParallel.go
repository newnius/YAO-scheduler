package main

type AllocatorParallel struct {
}

func (allocator *AllocatorParallel) requestResource(task Task) MsgAgent {
	res := MsgAgent{}
	for id, node := range pool.nodes {
		var available []NodeStatus
		for _, status := range node {
			if status.MemoryAllocated == 0 {
				available = append(available, status)
			}
		}
		if len(available) >= task.NumberGPU {
			res.ClientID = id
			res.Status = available[0:task.NumberGPU]

			for i := range res.Status {
				for j := range node {
					if res.Status[i].UUID == node[j].UUID {
						node[j].MemoryAllocated = task.MemoryGPU
					}
				}
			}
		}
	}
	return res
}

func (allocator *AllocatorParallel) returnResource(agent MsgAgent) {
	nodes := pool.nodes[agent.ClientID]
	for i, gpu := range agent.Status {
		if gpu.UUID == nodes[i].UUID {
			nodes[i].MemoryAllocated = 0
		}
	}
}
