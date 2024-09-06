package main

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

var KafkaConfig = &kafka.ConfigMap{"bootstrap.servers": "localhost"}

func main() {
	newProducerClient().ProduceMsg([]string{"welcome", "to", "the", "world", "of", "kafka"})
	// newConsumerClient().ConsumeMsg()

	// Consumer()
	// newAdminClient().describeConsumerGroup([]string{"myGroup"})
	// newAdminClient().listTopics()

}
