package task2

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

func CreateAsyncProducerAndSend10Messages(wg *sync.WaitGroup, signal chan struct{}) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer([]string{os.Getenv("ADDR")}, config)
	if err != nil {
		return fmt.Errorf("Can't create async producer for Kafka: %s", err)
	}

	topic := os.Getenv("TOPIC")
	num := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}

	for i := 0; i < 10; i++ {
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("It's %s message", num[i])),
		}

		producer.Input() <- message
		log.Printf("Message %s send", num[i])
	}

	go func() {
		defer wg.Done()
		defer producer.AsyncClose()
		errors := producer.Errors()
		messages := producer.Successes()
		for {
			select {
			case err := <-errors:
				log.Printf("Error occured while sending message %s: %s\n", err.Msg.Value, err)
			case msg := <-messages:
				log.Printf("%s successfully sended\n", msg.Value)
			case <-signal:
				return
			}
		}
	}()

	return nil
}
