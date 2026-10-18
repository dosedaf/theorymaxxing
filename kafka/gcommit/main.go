package main

import (
	"context"
	"flag"
	"log"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "", "topic to consume from")
	style       = flag.String("commit-style", "autocommit", "commit style; autocommit|records|uncommitted|marks|kadm")
	group       = flag.String("group", "", "group to consume within")
	logger      = flag.Bool("logger", false, "if true, enable an info level logger")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	styleNum := 0
	switch {
	case strings.HasPrefix("autocommit", *style):
	case strings.HasPrefix("records", *style):
		styleNum = 1
	case strings.HasPrefix("uncommitted", *style):
		styleNum = 2
	case strings.HasPrefix("mark", *style):
		styleNum = 3
	case strings.HasPrefix("kadm", *style):
		styleNum = 4
	default: 
		die("ga sesuai bro")
	}

	seeds := kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...) 

	if styleNum == 4 {
		consumeKadm(seeds)
	}

	opts := []kgo.Opt{
		seeds,
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
	}

	switch styleNum {
	case 1, 2:
		opts = append(opts, kgo.DisableAutoCommit())
	case 3:
		opts = append(opts, 
			kgo.AutoCommitMarks(),
			kgo.BlockRebalanceOnPoll(),
			kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32){
				if err := cl.CommitMarkedOffsets(ctx); err != nil {
					fmt.Printf("revoke commit failed: %v\n", err)
				}
			}),
			)
	}

	if *logger {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)))
	}

	cl, err := kgo.NewClient(opts...) 
	if err != nil {
		panic(err)
	}

	go consume(cl, styleNum)

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt)

	<-sigs
	fmt.Println("received interrupt signal; closing client")

	done := make(chan struct{})
	go func() {
		defer close(done)
		cl.Close()
	}()

	select {
	case <-sigs:
		fmt.Println("received second interrupt signal; quitting without waiting for graceful close")
	case <-done:
	}
}

func consume(cl *kgo.Client, style int) {
	for {
		fetches := cl.PollFetches(context.Background())
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			die("fetch error: %s topic %v partition %d", err, t, p)
		})

		switch style {
		case 0:
			var seen int
			fetches.EachRecord(func (rec *kgo.Record) {
				seen++
			})
			log.Printf("seen %v", seen)

		case 1:
			var rs []*kgo.Record

			fetches.EachRecord(func(rec *kgo.Record){
				rs = append(rs, rec)
			})
			if err :=  cl.CommitRecords(context.Background(), rs...); err != nil {
				die("error committing records: %v", err) 
			}

			log.Printf("commited records: %v", rs)
		case 2:
			var seen int

			fetches.EachRecord(func(rec *kgo.Record){
				seen++
			})
			if err :=  cl.CommitUncommittedOffsets(context.Background()); err != nil {
				die("error committing uncommitted: %v", err) 
			}

			log.Printf("commited uncommitted: %v", seen)
		case 3:
			marks := make(map[string]map[int32]kgo.EpochOffset)
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				if len(p) == 0 {
					return
				}

				last := p.Records[-1]
				marks[p.Topic][p.Partition] = kgo.EpochOffset{
					Epoch: last.LeaderEpoch,
					Offset: last.Offset + 1,
				}
			})
			if err := cl.CommitMarkedOffsets(context.Background()); err != nil {
				die("error committing marked offsets: %v", err) 
			}
			cl.AllowRebalance()
		}
	}
}

func consumeKadm(seeds kgo.Opt) {
}
