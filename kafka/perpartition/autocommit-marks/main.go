package main

import (
	"context"
	"flag"
	"math/rand/v2"
	"time"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	brokers = flag.String("b", "", "brokers")
	group   = flag.String("g", "", "group")
	topic   = flag.String("t", "", "topic")
)

type splitConsume struct {
	wg        sync.WaitGroup
	consumers map[string]map[int32]pconsumer
}

type pconsumer struct {
	cl   *kgo.Client
	name string

	done chan struct{}
	quit chan struct{}
	recs chan []*kgo.Record
}

func main() {
	flag.Parse()

	s := &splitConsume{
		consumers: make(map[string]map[int32]pconsumer),
	}

	if len(*group) == 0 {
		fmt.Println("missing required group")
		return
	}
	if len(*topic) == 0 {
		fmt.Println("missing required topic")
		return
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.OnPartitionsAssigned(s.assigned),
		kgo.OnPartitionsRevoked(s.revoked),
		kgo.OnPartitionsLost(s.lost),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		fmt.Println("received interrupt signal; closing client")
		cl.Close()
		<-sigs
		fmt.Println("received second interrupt; exiting")
		os.Exit(1)
	}()

	s.poll(cl)
}

func (s *splitConsume) assigned(_ context.Context, cl *kgo.Client, assigned map[string][]int32) {
	for topic, ps := range assigned {
		if s.consumers[topic] == nil {
			s.consumers[topic] = make(map[int32]pconsumer)
		}
		for _, p := range ps {
			pc := pconsumer{
				name: fmt.Sprintf("worker %d", p),
				quit: make(chan struct{}),
				recs: make(chan []*kgo.Record, 10),
				cl:   cl,
			}

			s.consumers[topic][p] = pc
			s.wg.Add(1)
			go pc.consume(topic, p)
		}
	}
}

func (s *splitConsume) revoked(_ context.Context, cl *kgo.Client, revoked map[string][]int32) {
	s.killConsumers(revoked)

	if err := cl.CommitMarkedOffsets(context.Background()); err != nil {
		fmt.Println("error comitting marked offsets: ", err)
		return
	}
}

func (s *splitConsume) lost(_ context.Context, cl *kgo.Client, lost map[string][]int32) {
	s.killConsumers(lost)
}

func (s *splitConsume) killConsumers(lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			pc := s.consumers[topic][partition]
			delete(s.consumers[topic], partition)
			delete(s.consumers, topic)
			close(pc.quit)
			fmt.Printf("waiting for work to finish t %s p %d\n", topic, partition)
			wg.Add(1)
			go func() { <-pc.done; wg.Done() }()
		}
	}
}

func (s *splitConsume) poll(cl *kgo.Client) {
	for {
		fetches := cl.PollRecords(context.Background(), 10000)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			panic(err)
		})
		fetches.EachTopic(func(t kgo.FetchTopic) {
			tconsumers := s.consumers[t.Topic]
			if tconsumers == nil {
				return
			}
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				pc, ok := tconsumers[p.Partition]
				if !ok {
					fmt.Println("ga ok")
					return
				}

				select {
				case pc.recs <- p.Records:
				case <-pc.quit:
				}
			})
		})

		cl.AllowRebalance()
	}
}

func (pc *pconsumer) consume(topic string, partition int32) {
	defer close(pc.done)

	for {
		select {
		case <-pc.quit:
		case recs := <-pc.recs:
			time.Sleep(time.Duration(rand.IntN(150)+100) * time.Millisecond)
			pc.cl.MarkCommitRecords(recs...)

			fmt.Printf("%s committed %d events\n", pc.name, len(recs))
		}
	}
}
