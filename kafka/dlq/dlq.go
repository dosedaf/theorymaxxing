package main

import (
	"os/signal"
	"log"
	"sync"
	"errors"
	"math/rand/v2"
	"time"
	"syscall"
	"context"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topic = "dlq-topic"
	group = "dlq-group"
	dlqTopic = "dlqTopic"
	maxRetry = 1
)

type kafka struct {
	producer *kgo.Client
	consumer *kgo.Client
}

type message struct {
	Topic string
	Value string
}

func (k *kafka) init() error {
	var err error
	k.producer, err = kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.AllowAutoTopicCreation(),
		)
	if err != nil {
		return err
	}

	k.consumer, err = kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		)
	if err != nil {
		return err
	}

	return nil
}

func (k *kafka) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
		default:
			fetches := k.consumer.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}
			if fetches.Empty() {
				continue
			}
			fetches.EachError(func(_ string, _ int32, err error) {
				panic(err)
			})

			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				for _, rec := range p.Records {
				if err := k.process(rec); err != nil {
					log.Println(err)
					if err := k.retry(rec); err != nil {
						log.Println(err)
						// pindah ke dlq
					}
				}
				}
			})
		}
	}
}

func (k *kafka) process(rec *kgo.Record) error {
	time.Sleep(time.Duration(1 * time.Second))
	msg := &message{
		Topic: rec.Topic,
		Value: string(rec.Value),
	}

	if rand.IntN(100) % 2 == 0 {
		return errors.New("error processing simulated")
	}

	log.Printf("%v", msg)
	return nil
}

func (k *kafka) retry(rec *kgo.Record) error {
	var err error
	for i := 1; i <= maxRetry; i++ {
		log.Printf("retrying: %d", i)

		time.Sleep(time.Duration(i) * time.Second)

		if err = k.process(rec); err != nil {
			log.Printf("retrying failed")
			continue
		}

		return nil
	}

	return err
}

func (k *kafka) close() {
	k.producer.Close()
	k.consumer.Close()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	k := &kafka{}
	if err := k.init(); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		k.producer.Produce(ctx, &kgo.Record{
			Topic: topic,
			Key: []byte("key" + strconv.Itoa(i)),
			Value: []byte("value" + strconv.Itoa(i)),
			Timestamp: time.Now(),
		}, nil)
	}

	wg.Add(1)
	go k.run(ctx, &wg)

	<-ctx.Done()
	k.close()
	wg.Wait()
}
