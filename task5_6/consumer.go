package task5_6

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

func CreateConsumerAndRead3Messages(wg *sync.WaitGroup, signal chan struct{}) error {
	consumer, err := sarama.NewConsumer([]string{os.Getenv("ADDR")}, nil)
	if err != nil {
		return fmt.Errorf("Can't create a consumer for Kafka: %s", err)
	}

	ptConsumer1, err := consumer.ConsumePartition(os.Getenv("TOPIC"), 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("Can't create partition consumer: %s", err)
	}
	ptConsumer2, err := consumer.ConsumePartition(os.Getenv("TOPIC"), 1, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("Can't create partition consumer: %s", err)
	}
	ptConsumer3, err := consumer.ConsumePartition(os.Getenv("TOPIC"), 2, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("Can't create partition consumer: %s", err)
	}

	go func() {
		defer wg.Done()
		defer consumer.Close()
		defer ptConsumer1.Close()
		defer ptConsumer2.Close()
		defer ptConsumer3.Close()
		messages1 := ptConsumer1.Messages()
		messages2 := ptConsumer2.Messages()
		messages3 := ptConsumer3.Messages()
		for {
			select {
			case v := <-messages1:
				log.Printf("From partition %d recieved message: %s\n", v.Partition, v.Value)
			case v := <-messages2:
				log.Printf("From partition %d recieved message: %s\n", v.Partition, v.Value)
			case v := <-messages3:
				log.Printf("From partition %d recieved message: %s\n", v.Partition, v.Value)
			case <-signal:
				return
			}
		}
	}()

	return nil
}
