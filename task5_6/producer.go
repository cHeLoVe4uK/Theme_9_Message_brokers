package task5_6

import (
	"fmt"
	"os"

	"github.com/IBM/sarama"
)

func CreateSyncProducerAndSend3Messages() error {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{os.Getenv("ADDR")}, config)
	if err != nil {
		return fmt.Errorf("Can't create a producer for Kafka: %s", err)
	}
	defer producer.Close()

	topic := os.Getenv("TOPIC")

	messages := []*sarama.ProducerMessage{
		{Topic: topic, Partition: 0, Value: sarama.StringEncoder("It's message for 0 partition")},
		{Topic: topic, Partition: 1, Value: sarama.StringEncoder("It's message for 1 partition")},
		{Topic: topic, Partition: 2, Value: sarama.StringEncoder("It's message for 2 partition")},
	}

	for _, v := range messages {
		partition, offset, err := producer.SendMessage(v)
		if err != nil {
			return fmt.Errorf("Error producing message: %s", err)
		}

		fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
	}

	return nil
}
