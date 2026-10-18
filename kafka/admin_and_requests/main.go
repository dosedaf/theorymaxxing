package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

var (
	seeds = flag.String("brokers", "localhost:9092", "-")
	topicName = flag.String("topic", "franz", "-")
	mode = flag.String("mode", "kadm", "-")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seeds,",")...),
		kgo.MaxVersions(kversion.V2_4_0()),
		)

	if err != nil {
		die("unable to create client: %v\n", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15 * time.Second)
	defer cancel()

	switch *mode{
	case "kadm":
		demoKadm(ctx, cl)
	case "kmsg":
		demoKmsg(ctx, cl)
	}
}

func demoKadm(ctx context.Context, cl *kgo.Client) {
	adm := kadm.NewClient(cl)
	defer adm.Close()

	topics, err := adm.ListTopics(ctx)
	if err != nil {
		die("error retrieving topic list: %v\n", err)
	}
	if topics.Has(*topicName) {
		die("topic already exists")
	}

	resp, err := adm.CreateTopic(ctx, -1, -1 , nil, *topicName)
	if err != nil{
		die("error creating topic: %v\n", err)
	}
	fmt.Printf("created topic: %v\n", resp.Topic)

	topics, err = adm.ListTopics(ctx)
	if err != nil {
		die("error retrieving topic list: %v\n", err)
	}

	fmt.Printf("topics list %v\n", topics)
}

func demoKmsg(ctx context.Context, cl *kgo.Client) {
	{
		req := kmsg.NewPtrCreateTopicsRequest()
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic = *topicName
		t.NumPartitions = 1
		t.ReplicationFactor = 1
		req.Topics = append(req.Topics, t)

		res, err := req.RequestWith(ctx, cl)
		if err != nil {
			die("create topic request failed: %v\n", err)
		}
		if len(res.Topics) != 1 {
			die("expected 1 topic in response, got %d\n", len(res.Topics))
		}
		if err := kerr.ErrorForCode(res.Topics[0].ErrorCode); err != nil {
			fmt.Fprintf(os.Stderr, "topic creation error: %v\n", err)
		} else {
			fmt.Printf("created topic %q via kmsg\n", res.Topics[0].Topic)
		}
	}

	{
		req := kmsg.NewPtrMetadataRequest()
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = kmsg.StringPtr(*topicName)
		req.Topics = append(req.Topics, t)

		res, err := req.RequestWith(ctx, cl)
		if err != nil {
			die("metadata request failed: %v\n", err)
		}
		for _, topic := range res.Topics {
			if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
				fmt.Printf("topic %s: error %v\n", *topic.Topic, err)
				continue
			}
			fmt.Printf("topic %s: %d partitions\n", *topic.Topic, len(topic.Partitions))
		}
		fmt.Printf("cluster has %d brokers\n", len(res.Brokers))
	}
}
