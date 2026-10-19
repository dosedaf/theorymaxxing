package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	total := flag.Uint64("total", 5_000_000, "number of acknowledged log lines to produce before exiting")
	rate := flag.Uint("rate", 50_000, "target log lines per second (0 = as fast as possible)")
	flag.Parse()

	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V4_3_1_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.Flush.Messages = 1000
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond
	cfg.ChannelBufferSize = 4096

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, cfg)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	var (
		sent, acked uint64
		feeding     int32 // 1 while enqueueing; failed messages are retried only then
		start       = time.Now()
	)

	rateLabel := fmt.Sprintf("%d/sec", *rate)
	if *rate == 0 {
		rateLabel = "unlimited"
	}
	log.Printf("producing %d log lines to topic events at %s\n", *total, rateLabel)

	// Ack pump: count successes, retry failures, log progress.
	stop := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		var lastErrLog time.Time
		for {
			select {
			case <-stop:
				return
			default:
			}
			select {
			case <-producer.Successes():
				n := atomic.AddUint64(&acked, 1)
				if n%250_000 == 0 {
					log.Printf("acked %d/%d (%.0f/sec)\n", n, *total, float64(n)/time.Since(start).Seconds())
				}
			case err, ok := <-producer.Errors():
				if !ok {
					return
				}
				if time.Since(lastErrLog) >= time.Second {
					log.Println("produce error, retrying:", err.Err)
					lastErrLog = time.Now()
				}
				if atomic.LoadInt32(&feeding) == 1 {
					producer.Input() <- err.Msg
				}
			}
		}
	}()

	enqueue := func() {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: "events",
			Value: sarama.StringEncoder(fmt.Sprintf("log line %d", sent+1)),
		}
		sent++
	}

	const tick = 100 * time.Millisecond
	var ticker *time.Ticker
	var perTick int
	if *rate > 0 {
		ticker = time.NewTicker(tick)
		defer ticker.Stop()
		perTick = int(float64(*rate) * tick.Seconds())
		if perTick < 1 {
			perTick = 1
		}
	}

	interrupted := false
feed:
	for sent < *total {
		if perTick == 0 {
			// As fast as possible, in chunks so signals still get noticed.
			for i := 0; i < 4096 && sent < *total; i++ {
				select {
				case <-signals:
					interrupted = true
					break feed
				default:
				}
				enqueue()
			}
			continue
		}
		select {
		case <-ticker.C:
			for i := 0; i < perTick && sent < *total; i++ {
				enqueue()
			}
		case <-signals:
			interrupted = true
			break feed
		}
	}
	atomic.StoreInt32(&feeding, 0)

	if interrupted {
		log.Printf("interrupted, flushing ~%d in-flight messages...\n", sent-atomic.LoadUint64(&acked))
	} else {
		log.Printf("all %d queued, waiting for acks...\n", sent)
	}

	// Wait for acks to catch up with what was sent.
	var lastAcked uint64
	stalled := 0
wait:
	for atomic.LoadUint64(&acked) < sent {
		select {
		case <-signals:
			interrupted = true
			break wait
		case <-time.After(5 * time.Second):
			a := atomic.LoadUint64(&acked)
			log.Printf("waiting for acks: %d/%d\n", a, sent)
			if a == lastAcked {
				stalled++
				if stalled >= 12 {
					log.Println("no ack progress for ~1m, giving up")
					break wait
				}
			} else {
				stalled = 0
			}
			lastAcked = a
		}
	}

	if err := producer.Close(); err != nil {
		log.Println("close:", err)
	}
	close(stop)
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
	}

	final := atomic.LoadUint64(&acked)
	elapsed := time.Since(start)
	suffix := ""
	if interrupted && final < *total {
		suffix = " (interrupted)"
	}
	log.Printf("acked %d of %d log lines in %s (%.0f/sec avg)%s\n",
		final, *total, elapsed.Round(time.Millisecond), float64(final)/elapsed.Seconds(), suffix)
	if final < *total {
		os.Exit(1)
	}
}
