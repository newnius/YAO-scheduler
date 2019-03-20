package main

import (
	"sync"
	"github.com/Shopify/sarama"
	"encoding/json"
	"log"
)

var (
	wg sync.WaitGroup
)

func start(pool *ResourcePool) {
	consumer, err := sarama.NewConsumer([]string{"kafka_node1:9091", "kafka_node2:9092", "kafka_node3:9093"}, nil)
	if err != nil {
		panic(err)
	}

	partitionList, err := consumer.Partitions("yao")
	if err != nil {
		panic(err)
	}

	for partition := range partitionList {
		pc, err := consumer.ConsumePartition("yao", int32(partition), sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		defer pc.AsyncClose()

		wg.Add(1)

		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				var msgAgent MsgAgent
				err = json.Unmarshal([]byte(string(msg.Value)), &msgAgent)
				if err != nil {
					log.Println(err)
					continue
				}
				pool.update(msgAgent)
			}

		}(pc)
	}
	wg.Wait()
	consumer.Close()
}
