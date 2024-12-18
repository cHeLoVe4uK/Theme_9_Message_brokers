package task4_9

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

func CreateConsumerGroup(ctx context.Context, wg *sync.WaitGroup) error {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup([]string{os.Getenv("ADDR")}, "1", config)
	if err != nil {
		return fmt.Errorf("Can't create a consumer group for Kafka: %s", err)
	}

	consumer := &consumer{}

	go func() {
		defer wg.Done()
		defer consumerGroup.Close()
		for {
			err = consumerGroup.Consume(ctx, []string{os.Getenv("TOPIC")}, consumer)
			if err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					fmt.Println("tried to use a consumer group that was closed")
					return
				}
				fmt.Printf("Error from consumer: %v\n", err)
				return
			}
			if ctx.Err() != nil {
				fmt.Println("Context was cancelled")
				return
			}
		}
	}()

	return nil
}

type consumer struct{}

func (c *consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Printf("Message claimed: value = %s, timestamp = %v, topic = %s\n", message.Value, message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}
	return nil
}
