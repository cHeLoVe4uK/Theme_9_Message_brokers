package task3

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

func CreateConsumerAndRead10Messages(wg *sync.WaitGroup, headSignal, signal chan struct{}) error {
	consumer, err := sarama.NewConsumer([]string{os.Getenv("ADDR")}, nil)
	if err != nil {
		return fmt.Errorf("Can't create a consumer for Kafka: %s", err)
	}

	ptConsumer, err := consumer.ConsumePartition(os.Getenv("TOPIC"), 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("Can't create partition consumer: %s", err)
	}

	go func() {
		defer wg.Done()
		defer consumer.Close()
		defer ptConsumer.Close()
		messages := ptConsumer.Messages()
		for i := 0; i < 5; i++ {
			go func() {
				defer wg.Done()
				for {
					select {
					case v := <-messages:
						log.Printf("Recieved message: %s\n", string(v.Value))
					case <-signal:
						return
					}
				}
			}()
		}
		<-headSignal
		return
	}()

	return nil
}
