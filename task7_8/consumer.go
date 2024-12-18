package task7_8

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

func CreateConsumerAndRead3Messages(wg *sync.WaitGroup, signal chan struct{}) error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

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

	shutdown := make(chan struct{})

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
				log.Printf("From partition %d recieved message: %s, with key: %s", v.Partition, v.Value, v.Key)
			case v := <-messages2:
				log.Printf("From partition %d recieved message: %s, with key: %s", v.Partition, v.Value, v.Key)
			case v := <-messages3:
				log.Printf("From partition %d recieved message: %s, with key: %s", v.Partition, v.Value, v.Key)
			case <-shutdown:
				return
			}
		}
	}()

	go func() {
		error1 := ptConsumer1.Errors()
		error2 := ptConsumer2.Errors()
		error3 := ptConsumer3.Errors()
		for {
			select {
			case v := <-error1:
				log.Printf("Error from topic: %s, partition: %d, err: %s\n", v.Topic, v.Partition, v.Err)
			case v := <-error2:
				log.Printf("Error from topic: %s, partition: %d, err: %s\n", v.Topic, v.Partition, v.Err)
			case v := <-error3:
				log.Printf("Error from topic: %s, partition: %d, err: %s\n", v.Topic, v.Partition, v.Err)
			case <-signal:
				shutdown <- struct{}{}
				return
			}
		}
	}()

	return nil
}
