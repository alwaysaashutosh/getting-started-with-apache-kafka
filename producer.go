package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type kafkaProducerClient struct {
	Key       string
	Topic     string
	Partition int32
	*kafka.Producer
}

func newProducerClient() *kafkaProducerClient {
	client, err := kafka.NewProducer(KafkaConfig)
	if err != nil {
		panic(err)
	}
	return &kafkaProducerClient{
		Topic:     "myTopic",
		Partition: 3,
		Producer:  client, // Assign the client to the Producer field
	}
}

func (p *kafkaProducerClient) ProduceMsg(msg []string) {
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

	for _, word := range msg {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.Topic, Partition: p.Partition},
			Value:          []byte(word),
			Key:            []byte(p.Key),
			Timestamp:      time.Now(),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
