package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V4_3_1_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, cfg)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// Drain successes/errors so the pipeline never stalls.
	go func() {
		for {
			select {
			case <-producer.Successes():
			case err, ok := <-producer.Errors():
				if !ok {
					return
				}
				log.Println("produce error:", err.Err)
			}
		}
	}()

	ticker := time.NewTicker(10 * time.Millisecond) // ~100 log lines/sec
	defer ticker.Stop()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	var n uint64
	for {
		select {
		case <-ticker.C:
			n++
			producer.Input() <- &sarama.ProducerMessage{
				Topic: "events",
				Value: sarama.StringEncoder(fmt.Sprintf("log line %d", n)),
			}
		case <-signals:
			log.Printf("sent %d log lines, shutting down\n", n)
			return
		}
	}
}
