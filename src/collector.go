package main

import (
	"sync"
	"github.com/Shopify/sarama"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"time"
)

var (
	wg sync.WaitGroup
)

func start(pool *ResourcePool) {
	consumer, err := sarama.NewConsumer([]string{"kafka-nod21:9092", "kafka-node2:9092", "kafka-node3:9092"}, nil)
	for {
		if err == nil {
			break
		}
		log.Warn(err)
		time.Sleep(time.Second * 5)
		consumer, err = sarama.NewConsumer([]string{"kafka-nod21:9092", "kafka-node2:9092", "kafka-node3:9092"}, nil)
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
				var nodeStatus NodeStatus
				err = json.Unmarshal([]byte(string(msg.Value)), &nodeStatus)
				if err != nil {
					log.Println(err)
					continue
				}
				pool.update(nodeStatus)
			}

		}(pc)
	}
	wg.Wait()
	consumer.Close()
}
