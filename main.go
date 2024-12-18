package main

import (
	"errors"
	"log"
	"os"
)

// Просто расскоментируй код под заголовками "Task number" (только делай это по одному, я для удобства кучей вот таких знаков разделил ////////)
// Для каждой таски свой код

func main() {
	// Подготовка для приложения
	err := setEnvForApp()
	if err != nil {
		log.Fatal(err)
	}
	defer os.Unsetenv("TOPIC")
	defer os.Unsetenv("ADDR")

	// Task 1
	// admin, err := sarama.NewClusterAdmin([]string{os.Getenv("ADDR")}, nil)
	// if err != nil {
	// 	log.Fatalf("Error occured while creating admin for Kafka: %s", err)
	// }
	// defer admin.Close()
	// defer admin.DeleteTopic(os.Getenv("TOPIC"))

	// err = admin.CreateTopic(os.Getenv("TOPIC"), &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: -1}, false)
	// if err != nil {
	// 	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
	// 		fmt.Printf("Topic %s already exists.\n", os.Getenv("TOPIC"))
	// 	} else {
	// 		log.Fatalf("Error creating topic: %s", err)
	// 	}
	// }

	// wg := sync.WaitGroup{}
	// wg.Add(1)
	// signal := make(chan struct{})
	// defer close(signal)

	// err = task1.CreateConsumerAndRead10Messages(&wg, signal)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(time.Second)

	// err = task1.CreateSyncProducerAndSend10Messages()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(2 * time.Second)
	// signal <- struct{}{}
	// wg.Wait()

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// Task 2
	// admin, err := sarama.NewClusterAdmin([]string{os.Getenv("ADDR")}, nil)
	// if err != nil {
	// 	log.Fatalf("Error occured while creating admin for Kafka: %s", err)
	// }
	// defer admin.Close()
	// defer admin.DeleteTopic(os.Getenv("TOPIC"))

	// err = admin.CreateTopic(os.Getenv("TOPIC"), &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: -1}, false)
	// if err != nil {
	// 	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
	// 		fmt.Printf("Topic %s already exists.\n", os.Getenv("TOPIC"))
	// 	} else {
	// 		log.Fatalf("Error creating topic: %s", err)
	// 	}
	// }

	// wg := sync.WaitGroup{}
	// wg.Add(2)
	// signal := make(chan struct{}, 2)
	// defer close(signal)

	// err = task2.CreateConsumerAndRead10Messages(&wg, signal)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(time.Second)

	// err = task2.CreateAsyncProducerAndSend10Messages(&wg, signal)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(4 * time.Second)

	// signal <- struct{}{}
	// signal <- struct{}{}

	// wg.Wait()

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// Task 3
	// admin, err := sarama.NewClusterAdmin([]string{os.Getenv("ADDR")}, nil)
	// if err != nil {
	// 	log.Fatalf("Error occured while creating admin for Kafka: %s", err)
	// }
	// defer admin.Close()
	// defer admin.DeleteTopic(os.Getenv("TOPIC"))

	// err = admin.CreateTopic(os.Getenv("TOPIC"), &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: -1}, false)
	// if err != nil {
	// 	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
	// 		fmt.Printf("Topic %s already exists.\n", os.Getenv("TOPIC"))
	// 	} else {
	// 		log.Fatalf("Error creating topic: %s", err)
	// 	}
	// }

	// wg := sync.WaitGroup{}
	// wg.Add(6)
	// headSignal := make(chan struct{})
	// signal := make(chan struct{}, 5)
	// defer close(headSignal)
	// defer close(signal)

	// err = task3.CreateConsumerAndRead10Messages(&wg, headSignal, signal)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(time.Second)

	// err = task3.CreateSyncProducerAndSend10Messages()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(4 * time.Second)

	// for i := 0; i < 5; i++ {
	// 	signal <- struct{}{}
	// }
	// headSignal <- struct{}{}

	// wg.Wait()

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// Task 4 & 9
	// admin, err := sarama.NewClusterAdmin([]string{os.Getenv("ADDR")}, nil)
	// if err != nil {
	// 	log.Fatalf("Error occured while creating admin for Kafka: %s", err)
	// }
	// defer admin.Close()
	// defer admin.DeleteTopic(os.Getenv("TOPIC"))

	// err = admin.CreateTopic(os.Getenv("TOPIC"), &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: -1}, false)
	// if err != nil {
	// 	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
	// 		fmt.Printf("Topic %s already exists.\n", os.Getenv("TOPIC"))
	// 	} else {
	// 		log.Fatalf("Error creating topic: %s", err)
	// 	}
	// }
	// wg := sync.WaitGroup{}
	// wg.Add(1)
	// ctx, cancel := context.WithCancel(context.Background())

	// err = task4_9.CreateConsumerGroup(ctx, &wg)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(time.Second)

	// err = task4_9.CreateSyncProducerAndSend10Messages()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(4 * time.Second)

	// cancel()
	// wg.Wait()

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// Task 5 & 6
	// admin, err := sarama.NewClusterAdmin([]string{os.Getenv("ADDR")}, nil)
	// if err != nil {
	// 	log.Fatalf("Error occured while creating admin for Kafka: %s", err)
	// }
	// defer admin.Close()
	// defer admin.DeleteTopic(os.Getenv("TOPIC"))

	// err = admin.CreateTopic(os.Getenv("TOPIC"), &sarama.TopicDetail{NumPartitions: 3, ReplicationFactor: -1}, false)
	// if err != nil {
	// 	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
	// 		fmt.Printf("Topic %s already exists.\n", os.Getenv("TOPIC"))
	// 	} else {
	// 		log.Fatalf("Error creating topic: %s", err)
	// 	}
	// }

	// wg := sync.WaitGroup{}
	// wg.Add(1)
	// signal := make(chan struct{})
	// defer close(signal)

	// err = task5_6.CreateConsumerAndRead3Messages(&wg, signal)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(time.Second)

	// err = task5_6.CreateSyncProducerAndSend3Messages()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(2 * time.Second)
	// signal <- struct{}{}
	// wg.Wait()

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// Task 7 & 8
	// admin, err := sarama.NewClusterAdmin([]string{os.Getenv("ADDR")}, nil)
	// if err != nil {
	// 	log.Fatalf("Error occured while creating admin for Kafka: %s", err)
	// }
	// defer admin.Close()
	// defer admin.DeleteTopic(os.Getenv("TOPIC"))

	// err = admin.CreateTopic(os.Getenv("TOPIC"), &sarama.TopicDetail{NumPartitions: 3, ReplicationFactor: -1}, false)
	// if err != nil {
	// 	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
	// 		fmt.Printf("Topic %s already exists.\n", os.Getenv("TOPIC"))
	// 	} else {
	// 		log.Fatalf("Error creating topic: %s", err)
	// 	}
	// }

	// wg := sync.WaitGroup{}
	// wg.Add(1)
	// signal := make(chan struct{})
	// defer close(signal)

	// err = task7_8.CreateConsumerAndRead3Messages(&wg, signal)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(time.Second)

	// err = task7_8.CreateSyncProducerAndSend3Messages()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// time.Sleep(2 * time.Second)
	// signal <- struct{}{}
	// wg.Wait()
}

func setEnvForApp() error {
	err := os.Setenv("TOPIC", "test-topic")
	if err != nil {
		return errors.New("Can't create environment \"TOPIC\"")
	}
	err = os.Setenv("ADDR", "localhost:9092")
	if err != nil {
		return errors.New("Can't create environment \"ADDR\"")
	}
	return nil
}
