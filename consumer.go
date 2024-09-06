package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type kafkaConsumerClient struct {
	Topic []string
	*kafka.Consumer
}

func newConsumerClient() *kafkaConsumerClient {
	client, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "mynewGroup",
		"auto.offset.reset": "earliest",
	})

	// changes in consumer
	if err != nil {
		panic(err)
	}
	return &kafkaConsumerClient{
		Topic:    []string{"myTopic"},
		Consumer: client, // Assign the client to the Producer field
	}
}
func (c *kafkaConsumerClient) ConsumeMsg() {

	err := c.SubscribeTopics(c.Topic, nil)

	if err != nil {
		panic(err)
	}
	defer c.Close()
	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on topic: [%s], partition: [%d],  key: [%s], value: [%s]\n", *msg.TopicPartition.Topic, msg.TopicPartition.Partition, string(msg.Key), string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

}
