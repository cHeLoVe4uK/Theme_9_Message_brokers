package task7_8

import (
	"fmt"
	"os"

	"github.com/IBM/sarama"
)

func CreateSyncProducerAndSend3Messages() error {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{os.Getenv("ADDR")}, config)
	if err != nil {
		return fmt.Errorf("Can't create a producer for Kafka: %s", err)
	}
	defer producer.Close()

	topic := os.Getenv("TOPIC")

	messages := []*sarama.ProducerMessage{
		{Topic: topic, Key: sarama.StringEncoder("Key_1"), Value: sarama.StringEncoder("It's message number 1")},
		{Topic: topic, Key: sarama.StringEncoder("This_is_key"), Value: sarama.StringEncoder("It's message number 2")},
		{Topic: topic, Key: sarama.StringEncoder("This_will_be_key_3"), Value: sarama.StringEncoder("It's message number 3")},
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
