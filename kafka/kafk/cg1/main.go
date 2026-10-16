package main

import (
	"fmt"
	"os"
	"context"
	"log"

	"github.com/IBM/sarama"
)

type Consumer struct{}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			fmt.Printf("Message claimed: value = %s, timestamp = %v, topic = %s\n", 
				string(message.Value), message.Timestamp, message.Topic)

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func main() {
	// broker := sarama.NewBroker("localhost:9092")
	// err := broker.Open(nil)
	// if err != nil {
	// 	panic(err)
	// }
	//
	// request := sarama.MetadataRequest{Topics: []string{"nis", "cer"}, AllowAutoTopicCreation: false}
	// response, err := broker.GetMetadata(&request)
	// if err != nil {
	// 	_ = broker.Close()
	// 	panic(err)
	// }
	//
	// fmt.Println("There are", len(response.Topics), "topics active in the cluster.")
	//
	// if err = broker.Close(); err != nil {
	// 	panic(err)
	// }
	
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V4_3_1_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true

	cg, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "kafk", cfg)
	if err != nil {
		panic(err)
	}

	// ctx, cancel := context.WithCancel(context.Background())
	ctx := context.Background()
	consumer := Consumer{}

	for {
		if err := cg.Consume(ctx, []string{"events"}, &consumer); err != nil {
			log.Println("consume:", err)
		}

		if ctx.Err() != nil {
			return
		}
	}
}
