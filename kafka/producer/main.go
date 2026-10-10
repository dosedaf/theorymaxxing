package main

import (
	"fmt"
	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Transaction.ID = "1"
	config.Producer.Idempotent = true 
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.MaxOpenRequests = 1

	prod, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		fmt.Println(err)
	}

	err = prod.BeginTxn()
	if err != nil {
		fmt.Println(err)
	}

	msg := sarama.ProducerMessage{
		Topic: "bitch",
		Key: sarama.StringEncoder("Alice"),
		Value: sarama.StringEncoder("In Borderland"),
	}

	fmt.Println("test")
	p, o, err := prod.SendMessage(&msg)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(p, o)
}
