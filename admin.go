package main

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type kafkaAdminClient struct {
	*kafka.AdminClient
}

func newAdminClient() *kafkaAdminClient {
	client, err := kafka.NewAdminClient(KafkaConfig)
	if err != nil {
		fmt.Printf("error while initializing kafka admin client: %v", err)
		return nil

	}
	return &kafkaAdminClient{client}
}
func (c *kafkaAdminClient) describeConsumerGroup(consumergroups []string) {
	groups, err := c.DescribeConsumerGroups(context.Background(), consumergroups)
	if err != nil {
		log.Fatalf("Failed to describe consumer group: %v", err)
	}

	// Print the consumer group details
	fmt.Printf("Consumer Group: %v \n", groups)

}

func (c *kafkaAdminClient) createPartion(topicName string, maxcount int) {
	partSpecs := []kafka.PartitionsSpecification{
		{
			Topic:      topicName, // Replace with your topic name
			IncreaseTo: maxcount,
		},
	}

	result, err := c.CreatePartitions(context.Background(), partSpecs)
	if err != nil {
		fmt.Printf("error while creating partition : %v", err)
		return
	}

	fmt.Printf("result for topic ::> %v", result)
}
func (c *kafkaAdminClient) listTopics() {
	// Get list of topics
	topics, err := c.GetMetadata(nil, true, 30*1000)
	if err != nil {
		log.Fatalf("Failed to get metadata: %v", err)
	}

	// Print the list of topics
	fmt.Println("Available topics:")
	for _, topic := range topics.Topics {
		fmt.Println(topic.Topic)
	}
}

func (c *kafkaAdminClient) ListAllConsumerGroups() {

	g, err := c.ListConsumerGroups(context.Background())
	if err != nil {
		log.Fatalf("Failed to get consumers: %v", err)
	}
	fmt.Printf("value of g : %v\n", g)
}
