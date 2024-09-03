package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func Producer() {
	// kafka.NewAdminClient()
	p, err := kafka.NewProducer(KafkaConfig)
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to topic: %v , partition: %d, value: %s\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, string(ev.Value))
				}
			}
		}
	}()

	// newgenmsg := []string{"welcome", "to", "the", "world", "of", "RE"}
	defaultmsg := []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"}

	// Produce messages to topic (asynchronously)
	topic := "myTopic"
	for _, word := range defaultmsg {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 3},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
func RandPrint(val string) {
	fmt.Printf("hello :%v\n", val)
}
