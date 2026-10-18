// Command refseed is the reference-data faucet: it seeds a user profile
// snapshot and a product catalog into compacted topics, then keeps mutating
// them (plan changes, price moves, deletes) to simulate a CDC feed, so an
// enrichment service has a reference table that never stops changing.
//
// Run it with the SAME --seed/--users/--products as the generator, otherwise
// the event stream and the reference table cover different universes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"clickstream/internal/codec"
	"clickstream/internal/kprod"
	"clickstream/internal/sim"
	"clickstream/internal/topics"
)

type stats struct {
	mu          *sync.Mutex // pointer so summary snapshots can copy the value
	snapUsers   int
	snapProduct int
	updated     int
	tombstones  int
	failed      int
}

type seeder struct {
	cfg   config
	rng   *rand.Rand
	uni   *sim.Universe
	kcl   *kgo.Client
	users *codec.Ref
	prods *codec.Ref

	usersTopic    string
	productsTopic string

	st stats
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("refseed: ")

	var (
		brokers    = flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "comma-separated kafka bootstrap servers")
		srURL      = flag.String("sr-url", envOr("SCHEMA_REGISTRY_URL", "http://localhost:8081"), "schema registry URL")
		usersTopic = flag.String("users-topic", "ref.users", "compacted topic for user profiles")
		prodsTopic = flag.String("products-topic", "ref.products", "compacted topic for the product catalog")
		users      = flag.Int("users", 500, "virtual user population (match generator)")
		products   = flag.Int("products", 60, "product catalog size (match generator)")
		seed       = flag.Int64("seed", 42, "rng seed (match generator)")
		partitions = flag.Int("partitions", 6, "partitions when provisioning ref topics")
		rf         = flag.Int("rf", 1, "replication factor when provisioning")
		noProv     = flag.Bool("no-provision", false, "do not create the topics")
		mutateRate = flag.Float64("mutate-rate", 5, "mutations per second after the snapshot")
		snapOnly   = flag.Bool("snapshot-only", false, "seed the snapshot and exit")
		duration   = flag.Duration("duration", 0, "stop mutating after this duration (0 = until interrupted)")
		linger     = flag.Duration("linger", 10*time.Millisecond, "producer linger")
		compress   = flag.String("compression", "snappy", "producer compression: none|gzip|snappy|lz4|zstd")
	)
	flag.Parse()

	cfg := config{
		brokers:       strings.Split(*brokers, ","),
		srURL:         *srURL,
		usersTopic:    *usersTopic,
		productsTopic: *prodsTopic,
		users:         *users,
		products:      *products,
		seed:          *seed,
		partitions:    *partitions,
		rf:            *rf,
		noProvision:   *noProv,
		mutateRate:    *mutateRate,
		snapshotOnly:  *snapOnly,
		duration:      *duration,
		linger:        *linger,
		compression:   *compress,
	}
	if cfg.mutateRate <= 0 {
		log.Fatal("--mutate-rate must be > 0")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

type config struct {
	brokers       []string
	srURL         string
	usersTopic    string
	productsTopic string
	users         int
	products      int
	seed          int64
	partitions    int
	rf            int
	noProvision   bool
	mutateRate    float64
	snapshotOnly  bool
	duration      time.Duration
	linger        time.Duration
	compression   string
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run(ctx context.Context, cfg config) error {
	// Same draw order as the generator: universe first, from a fresh seeded
	// rng, so the population matches when seed/counts match.
	rng := rand.New(rand.NewSource(cfg.seed))
	uni := sim.NewUniverse(rng, cfg.users, cfg.products)

	kcl, err := kprod.New(kprod.Config{
		Brokers:     cfg.brokers,
		Linger:      cfg.linger,
		Compression: cfg.compression,
	})
	if err != nil {
		return err
	}
	defer kcl.Close()

	if !cfg.noProvision {
		adm := kadm.NewClient(kcl)
		err := topics.Ensure(ctx, adm, []topics.Spec{
			topics.Compacted(cfg.usersTopic, int32(cfg.partitions), int16(cfg.rf)),
			topics.Compacted(cfg.productsTopic, int32(cfg.partitions), int16(cfg.rf)),
		})
		if err != nil {
			return err
		}
	}

	srcl, err := sr.NewClient(sr.URLs(cfg.srURL))
	if err != nil {
		return fmt.Errorf("schema registry client: %w", err)
	}
	usersRef, err := codec.RegisterUserSchema(ctx, srcl, cfg.usersTopic+"-value")
	if err != nil {
		return err
	}
	prodsRef, err := codec.RegisterProductSchema(ctx, srcl, cfg.productsTopic+"-value")
	if err != nil {
		return err
	}
	log.Printf("schemas ready: %s (id=%d), %s (id=%d)", usersRef.Subject(), usersRef.SchemaID(), prodsRef.Subject(), prodsRef.SchemaID())

	s := &seeder{
		cfg:           cfg,
		rng:           rng,
		uni:           uni,
		kcl:           kcl,
		users:         usersRef,
		prods:         prodsRef,
		usersTopic:    cfg.usersTopic,
		productsTopic: cfg.productsTopic,
		st:            stats{mu: new(sync.Mutex)},
	}

	if err := s.snapshot(ctx); err != nil {
		return err
	}
	if cfg.snapshotOnly {
		s.summarize()
		return nil
	}
	return s.mutateLoop(ctx)
}

// snapshot writes every user and product once. Kafka's default partitioner
// hashes the ID keys, so enrichment services rebuilding state per partition
// see a full view per partition.
func (s *seeder) snapshot(ctx context.Context) error {
	started := time.Now()
	now := time.Now().UnixMilli()
	for _, u := range s.uni.Users {
		payload, err := s.users.EncodeUser(u, now)
		if err != nil {
			return fmt.Errorf("encode user %s: %w", u.ID, err)
		}
		s.produce(ctx, s.usersTopic, []byte(u.ID), payload, func() {
			s.st.mu.Lock()
			s.st.snapUsers++
			s.st.mu.Unlock()
		})
	}
	for _, p := range s.uni.Products {
		payload, err := s.prods.EncodeProduct(p, now)
		if err != nil {
			return fmt.Errorf("encode product %s: %w", p.ID, err)
		}
		s.produce(ctx, s.productsTopic, []byte(p.ID), payload, func() {
			s.st.mu.Lock()
			s.st.snapProduct++
			s.st.mu.Unlock()
		})
	}
	flushCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	s.kcl.Flush(flushCtx)
	s.st.mu.Lock()
	st := s.st
	s.st.mu.Unlock()
	log.Printf("snapshot complete: %d users -> %s, %d products -> %s in %s (%d produce failures)",
		st.snapUsers, s.usersTopic, st.snapProduct, s.productsTopic, time.Since(started).Round(time.Millisecond), st.failed)
	if st.failed > 0 {
		return fmt.Errorf("snapshot had %d produce failures", st.failed)
	}
	return nil
}

func (s *seeder) mutateLoop(ctx context.Context) error {
	log.Printf("mutating at %.1f ops/s (updates, plan/price changes, deletes); ctrl-c to stop", s.cfg.mutateRate)
	interval := time.Duration(float64(time.Second) / s.cfg.mutateRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	started := time.Now()
	mutations := 0
	for {
		select {
		case <-ctx.Done():
			log.Printf("interrupted; shutting down cleanly")
			s.finish()
			return nil
		case <-ticker.C:
			s.mutateOne()
			mutations++
			if mutations%100 == 0 {
				s.st.mu.Lock()
				st := s.st
				s.st.mu.Unlock()
				log.Printf("mutations=%d (updated=%d tombstoned=%d failed=%d)", mutations, st.updated, st.tombstones, st.failed)
			}
			if s.cfg.duration > 0 && time.Since(started) >= s.cfg.duration {
				log.Printf("duration %s reached; shutting down cleanly", s.cfg.duration)
				s.finish()
				return nil
			}
		}
	}
}

// mutateOne applies one reference-data change, simulating a CDC stream.
func (s *seeder) mutateOne() {
	now := time.Now().UnixMilli()
	if s.rng.Float64() < 0.55 {
		u := s.uni.Users[s.rng.Intn(len(s.uni.Users))]
		mutateUser(s.rng, &u)
		payload, err := s.users.EncodeUser(u, now)
		if err != nil {
			log.Printf("BUG: encode user: %v", err)
			return
		}
		s.produce(context.Background(), s.usersTopic, []byte(u.ID), payload, nil)
		s.bump(1, 0)
		return
	}

	p := s.uni.Products[s.rng.Intn(len(s.uni.Products))]
	if s.rng.Float64() < 0.10 {
		// product deleted: tombstone (nil value) -- compaction removes it,
		// enrichment services must handle the delete.
		s.produce(context.Background(), s.productsTopic, []byte(p.ID), nil, nil)
		s.bump(0, 1)
		return
	}
	mutateProduct(s.rng, &p)
	payload, err := s.prods.EncodeProduct(p, now)
	if err != nil {
		log.Printf("BUG: encode product: %v", err)
		return
	}
	s.produce(context.Background(), s.productsTopic, []byte(p.ID), payload, nil)
	s.bump(1, 0)
}

func mutateUser(rng *rand.Rand, u *sim.User) {
	switch r := rng.Float64(); {
	case r < 0.40: // plan change
		idx := 0
		for i, p := range sim.Plans {
			if p == u.Plan {
				idx = i
				break
			}
		}
		next := (idx + 1 + rng.Intn(len(sim.Plans)-1)) % len(sim.Plans)
		u.Plan = sim.Plans[next]
	case r < 0.70: // loyalty points drift
		delta := int32(100 + rng.Intn(900))
		if rng.Float64() < 0.4 {
			u.LoyaltyPoints -= delta
		} else {
			u.LoyaltyPoints += delta
		}
		if u.LoyaltyPoints < 0 {
			u.LoyaltyPoints = 0
		}
	case r < 0.90: // active flag toggle
		u.Active = !u.Active
	default: // relocation
		cities := []struct{ Country, City string }{
			{"US", "Denver"}, {"DE", "Hamburg"}, {"JP", "Osaka"}, {"BR", "Recife"}, {"GB", "Manchester"},
		}
		cc := cities[rng.Intn(len(cities))]
		u.Country, u.City = cc.Country, cc.City
	}
}

func mutateProduct(rng *rand.Rand, p *sim.Product) {
	switch r := rng.Float64(); {
	case r < 0.60: // price move
		p.Price = math.Round(p.Price*(0.85+rng.Float64()*0.30)*100) / 100
		if p.Price < 0.99 {
			p.Price = 0.99
		}
	case r < 0.80: // stock drift
		delta := int32(1 + rng.Intn(50))
		if rng.Float64() < 0.5 {
			p.Stock -= delta
		} else {
			p.Stock += delta
		}
		if p.Stock < 0 {
			p.Stock = 0
		}
	case r < 0.90: // delist/relist
		p.Active = !p.Active
	default: // rename (marketing, obviously)
		names := []string{"Renamed Widget", "Rebranded Gadget", "New & Improved Thing"}
		p.Name = names[rng.Intn(len(names))]
	}
}

// produce emits one record; onOK runs from the delivery callback on success.
func (s *seeder) produce(ctx context.Context, topic string, key, val []byte, onOK func()) {
	rec := &kgo.Record{Topic: topic, Key: key, Value: val}
	s.kcl.Produce(ctx, rec, func(r *kgo.Record, err error) {
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.st.mu.Lock()
				s.st.failed++
				n := s.st.failed
				s.st.mu.Unlock()
				if n == 1 || n%100 == 0 {
					log.Printf("produce failed (%d so far): %v", n, err)
				}
			}
			return
		}
		if onOK != nil {
			onOK()
		}
	})
}

func (s *seeder) bump(updated, tombstoned int) {
	s.st.mu.Lock()
	s.st.updated += updated
	s.st.tombstones += tombstoned
	s.st.mu.Unlock()
}

func (s *seeder) finish() {
	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.kcl.Flush(flushCtx)
	s.summarize()
}

func (s *seeder) summarize() {
	s.st.mu.Lock()
	st := s.st
	s.st.mu.Unlock()
	log.Printf("================ refseed summary ================")
	log.Printf("snapshot          : %d users, %d products", st.snapUsers, st.snapProduct)
	log.Printf("mutations         : %d updates, %d tombstones, %d failures", st.updated, st.tombstones, st.failed)
	log.Printf("==================================================")
}
