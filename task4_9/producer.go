package task4_9

import (
	"fmt"
	"os"

	"github.com/IBM/sarama"
)

func CreateSyncProducerAndSend10Messages() error {
	producer, err := sarama.NewSyncProducer([]string{os.Getenv("ADDR")}, nil)
	if err != nil {
		return fmt.Errorf("Can't create a producer for Kafka: %s", err)
	}
	defer producer.Close()

	topic := os.Getenv("TOPIC")
	num := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}

	for i := 0; i < 10; i++ {
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("It's %s message", num[i])),
		}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			return fmt.Errorf("Error producing message: %s", err)
		}

		fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
	}

	return nil
}
