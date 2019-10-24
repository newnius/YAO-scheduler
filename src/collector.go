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

func start(pool *ResourcePool, config Configuration) {
	consumer, err := sarama.NewConsumer(config.KafkaBrokers, nil)
	for {
		if err == nil {
			break
		}
		log.Warn(err)
		time.Sleep(time.Second * 5)
		consumer, err = sarama.NewConsumer(config.KafkaBrokers, nil)
	}

	partitionList, err := consumer.Partitions(config.KafkaTopic)
	if err != nil {
		panic(err)
	}

	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(config.KafkaTopic, int32(partition), sarama.OffsetNewest)
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
					log.Warn(err)
					continue
				}
				pool.update(nodeStatus)
			}

		}(pc)
	}
	wg.Wait()
	consumer.Close()
}
