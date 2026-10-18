// Command generator is the raw clickstream faucet: it simulates users and
// sessions, injects deliberate noise (duplicates, malformed records,
// out-of-order timestamps, late arrivals), produces Avro to events.raw via
// Schema Registry, and journals ground truth to a JSONL file.
//
// Ground truth rows are written on delivery acknowledgement only, so the
// ledger reflects what Kafka actually has.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"clickstream/internal/codec"
	"clickstream/internal/kprod"
	"clickstream/internal/ledger"
	"clickstream/internal/sim"
	"clickstream/internal/topics"
)

// Wire-level malformed categories. Semantic violations are named
// "semantic:<violation>"; see buildPayload.
const (
	malGarbage    = "garbage_bytes"
	malBadMagic   = "bad_magic"
	malCorruptBdy = "corrupt_body"
)

type config struct {
	brokers     []string
	srURL       string
	topic       string
	partitions  int
	rf          int
	retention   time.Duration
	noProvision bool

	rate         float64
	duration     time.Duration
	users        int
	products     int
	seed         int64
	keyBy        string
	schemaVer    int
	dupRate      float64
	malformedRte float64
	lateRate     float64
	lateMax      time.Duration
	oooMax       time.Duration
	ringSize     int

	backfill         bool
	backfillStart    string
	backfillDuration time.Duration
	backfillSpeedup  float64
	backfillMaxEps   float64

	gtFile      string
	linger      time.Duration
	compression string
	maxBuffered int
	quiet       bool
}

type stats struct {
	mu         *sync.Mutex // pointer so snapshot() can copy the value
	issued     int         // new events generated (incl. malformed, incl. later-held)
	delivered  int         // acked by kafka
	failed     int
	dupIssued  int
	dupRows    int // delivered dup rows
	lateIssued int
	lateRows   int // delivered rows that were held
	malRows    int // delivered malformed rows
	held       int // holds in flight
	byType     map[string]int
	malByType  map[string]int
	partitions map[int32]int
	revenue    float64
}

func newStats() stats {
	return stats{
		mu:         new(sync.Mutex),
		byType:     make(map[string]int),
		malByType:  make(map[string]int),
		partitions: make(map[int32]int),
	}
}

func (s *stats) snapshot() stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := *s
	c.byType = copyMap(s.byType)
	c.malByType = copyMap(s.malByType)
	c.partitions = copyI32(s.partitions)
	return c
}

func copyMap(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyI32(m map[int32]int) map[int32]int {
	out := make(map[int32]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

type ringEntry struct {
	key []byte
	val []byte
	ev  sim.Event
}

type faucet struct {
	cfg    config
	rng    *rand.Rand
	engine *sim.Engine
	evc    *codec.Event
	kcl    *kgo.Client
	led    *ledger.Ledger

	ring     []ringEntry
	ringNext int

	st    stats
	holds sync.WaitGroup
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("generator: ")
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil && !strings.Contains(err.Error(), "context canceled") {
		log.Fatalf("fatal: %v", err)
	}
}

func parseFlags() config {
	var (
		brokers = flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "comma-separated kafka bootstrap servers")
		srURL   = flag.String("sr-url", envOr("SCHEMA_REGISTRY_URL", "http://localhost:8081"), "schema registry URL")
		topic   = flag.String("topic", "events.raw", "raw events topic")
		part    = flag.Int("partitions", 12, "partitions when provisioning the topic")
		rf      = flag.Int("rf", 1, "replication factor when provisioning")
		retent  = flag.Duration("retention", 24*time.Hour, "topic retention.ms when provisioning")
		noProv  = flag.Bool("no-provision", false, "do not create the topic; fail or misbehave on your own")

		rate     = flag.Float64("rate", 200, "events per second")
		duration = flag.Duration("duration", 0, "stop after this wall-clock duration (0 = until interrupted)")
		users    = flag.Int("users", 500, "virtual user population (match refseed)")
		products = flag.Int("products", 60, "product catalog size (match refseed)")
		seed     = flag.Int64("seed", 42, "rng seed; same seed + same counts => same universe")
		keyBy    = flag.String("key-by", "user", "record key: user | session")
		schemaV  = flag.Int("schema-version", 2, "avro schema version to register and produce (1 or 2)")
		dupRate  = flag.Float64("dup-rate", 0.03, "fraction of emissions that are exact replays of a recent event")
		malRate  = flag.Float64("malformed-rate", 0.02, "fraction of new events emitted malformed")
		lateRate = flag.Float64("late-rate", 0.02, "fraction of new events held back and emitted late")
		lateMax  = flag.Duration("late-max", 45*time.Second, "maximum hold time for late events")
		oooMax   = flag.Duration("ooo-max", 5*time.Second, "maximum out-of-order skew applied to every event timestamp")
		ringSize = flag.Int("ring", 4096, "size of the recent-events ring used for dup injection")

		backfill         = flag.Bool("backfill", false, "backfill mode: blast a simulated day of event-time as fast as possible")
		backfillStart    = flag.String("backfill-start", "", "backfill event-time start, RFC3339 (default: now-24h)")
		backfillDuration = flag.Duration("backfill-duration", 24*time.Hour, "simulated event-time duration to cover")
		backfillSpeed    = flag.Float64("backfill-speedup", 1000, "sim seconds per wall second for late holds")
		backfillCap      = flag.Float64("backfill-max-eps", 0, "wall-clock cap on emissions/sec in backfill (0 = unlimited)")

		gtFile      = flag.String("gt-file", "ground_truth.jsonl", "ground truth ledger output (JSONL)")
		linger      = flag.Duration("linger", 10*time.Millisecond, "producer linger")
		compression = flag.String("compression", "snappy", "producer compression: none|gzip|snappy|lz4|zstd")
		maxBuffered = flag.Int("max-buffered", 1000000, "producer max buffered records")
		quiet       = flag.Bool("quiet", false, "only log warnings and the summary")
	)
	flag.Parse()

	cfg := config{
		brokers:          strings.Split(*brokers, ","),
		srURL:            *srURL,
		topic:            *topic,
		partitions:       *part,
		rf:               *rf,
		retention:        *retent,
		noProvision:      *noProv,
		rate:             *rate,
		duration:         *duration,
		users:            *users,
		products:         *products,
		seed:             *seed,
		keyBy:            *keyBy,
		schemaVer:        *schemaV,
		dupRate:          *dupRate,
		malformedRte:     *malRate,
		lateRate:         *lateRate,
		lateMax:          *lateMax,
		oooMax:           *oooMax,
		ringSize:         *ringSize,
		backfill:         *backfill,
		backfillStart:    *backfillStart,
		backfillDuration: *backfillDuration,
		backfillSpeedup:  *backfillSpeed,
		backfillMaxEps:   *backfillCap,
		gtFile:           *gtFile,
		linger:           *linger,
		compression:      *compression,
		maxBuffered:      *maxBuffered,
		quiet:            *quiet,
	}

	for _, b := range cfg.brokers {
		b = strings.TrimSpace(b)
		if b == "" {
			log.Fatal("--brokers contains an empty entry")
		}
	}
	if cfg.rate <= 0 {
		log.Fatal("--rate must be > 0")
	}
	if cfg.users < 1 || cfg.products < 1 {
		log.Fatal("--users and --products must be >= 1")
	}
	if cfg.schemaVer != 1 && cfg.schemaVer != 2 {
		log.Fatal("--schema-version must be 1 or 2")
	}
	switch cfg.keyBy {
	case "user", "session":
	default:
		log.Fatal("--key-by must be user or session")
	}
	for name, r := range map[string]float64{"dup-rate": cfg.dupRate, "malformed-rate": cfg.malformedRte, "late-rate": cfg.lateRate} {
		if r < 0 || r >= 1 {
			log.Fatalf("--%s must be in [0,1)", name)
		}
	}
	if cfg.lateRate > 0 && cfg.lateMax <= 0 {
		log.Fatal("--late-max must be > 0 when --late-rate > 0")
	}
	if cfg.backfillSpeedup < 1 {
		log.Fatal("--backfill-speedup must be >= 1")
	}
	return cfg
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run(ctx context.Context, cfg config) error {
	started := time.Now()

	// Single seeded rng drives universe, engine, and noise in draw order --
	// determinism depends on this sequence.
	rng := rand.New(rand.NewSource(cfg.seed))
	uni := sim.NewUniverse(rng, cfg.users, cfg.products)
	engine := sim.NewEngine(uni, rng, cfg.oooMax)

	kcl, err := kprod.New(kprod.Config{
		Brokers:            cfg.brokers,
		Linger:             cfg.linger,
		Compression:        cfg.compression,
		MaxBufferedRecords: cfg.maxBuffered,
	})
	if err != nil {
		return err
	}
	defer kcl.Close()

	if !cfg.noProvision {
		adm := kadm.NewClient(kcl)
		err := topics.Ensure(ctx, adm, []topics.Spec{
			topics.RawEvent(cfg.topic, int32(cfg.partitions), int16(cfg.rf), cfg.retention),
		})
		if err != nil {
			return err
		}
	}

	srcl, err := sr.NewClient(sr.URLs(cfg.srURL))
	if err != nil {
		return fmt.Errorf("schema registry client: %w", err)
	}
	evc, err := codec.RegisterEvent(ctx, srcl, cfg.topic+"-value", cfg.schemaVer)
	if err != nil {
		return err
	}
	log.Printf("schema ready: subject=%s id=%d version=%d (compat=BACKWARD)", evc.Subject(), evc.SchemaID(), evc.Version())

	led, err := ledger.New(cfg.gtFile)
	if err != nil {
		return err
	}
	defer led.Close()

	f := &faucet{
		cfg:    cfg,
		rng:    rng,
		engine: engine,
		evc:    evc,
		kcl:    kcl,
		led:    led,
		ring:   make([]ringEntry, cfg.ringSize),
		st:     newStats(),
	}

	// Progress reporter.
	reportCtx, cancelReport := context.WithCancel(ctx)
	defer cancelReport()
	go f.reportLoop(reportCtx, started)

	if cfg.backfill {
		err = f.runBackfill(ctx)
	} else {
		err = f.runLive(ctx)
	}

	// Shutdown: wait out in-flight late holds (bounded), then flush so every
	// acked record is in the ledger before the file closes.
	f.drainHolds(ctx)
	flushCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	log.Printf("flushing %d in-flight records to kafka...", kcl.BufferedProduceRecords())
	kcl.Flush(flushCtx)
	flushErr := led.Flush()

	f.summarize(started)
	if err != nil {
		return err
	}
	return flushErr
}

// ---------------------------------------------------------------------------
// emission paths
// ---------------------------------------------------------------------------

func (f *faucet) emit(ev sim.Event, payload, key []byte, tsForRec int64, dup, late bool, hold time.Duration, malType string) {
	if payload == nil {
		return // encoding bug already counted
	}
	rec := &kgo.Record{
		Topic:     f.cfg.topic,
		Key:       key,
		Value:     payload,
		Timestamp: time.UnixMilli(tsForRec),
	}
	row := ledger.Row{
		EventID:       ev.EventID,
		EventType:     ev.EventType,
		EventTS:       ev.EventTS,
		UserID:        ev.UserID,
		SessionID:     ev.SessionID,
		Page:          ev.Page,
		ProductID:     ev.ProductID,
		Qty:           ev.Qty,
		Revenue:       ev.Revenue,
		Device:        ev.Device,
		Referrer:      ev.Referrer,
		SchemaVersion: f.evc.Version(),
		Key:           string(key),
		IsDuplicate:   dup,
		IsMalformed:   malType != "",
		MalformedType: malType,
		IsLate:        late,
		HoldMs:        hold.Milliseconds(),
		EmittedAt:     time.Now().UnixMilli(),
	}
	f.kcl.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		if err != nil {
			f.st.mu.Lock()
			f.st.failed++
			failed := f.st.failed
			f.st.mu.Unlock()
			if failed == 1 || failed%1000 == 0 {
				log.Printf("produce failed (%d so far): %v", failed, err)
			}
			return
		}
		row.Partition = r.Partition
		row.Offset = r.Offset
		f.led.Write(row)

		f.st.mu.Lock()
		f.st.delivered++
		f.st.partitions[r.Partition]++
		f.st.byType[row.EventType]++
		if row.IsDuplicate {
			f.st.dupRows++
		}
		if row.IsMalformed {
			f.st.malRows++
		}
		if row.IsLate {
			f.st.lateRows++
		}
		if !row.IsDuplicate && !row.IsMalformed && row.EventType == "purchase" {
			f.st.revenue += row.Revenue
		}
		f.st.mu.Unlock()
	})
}

// tickLive emits one unit of work for a live tick: either a duplicate of a
// recent clean event, or a fresh event (possibly malformed, possibly held).
func (f *faucet) tickLive(now time.Time) {
	// Duplicate roll first: replaces this tick's fresh event, keeping eps steady.
	if f.cfg.dupRate > 0 && f.rng.Float64() < f.cfg.dupRate {
		if entry, ok := f.pickRing(); ok {
			f.st.mu.Lock()
			f.st.dupIssued++
			f.st.mu.Unlock()
			f.emit(entry.ev, entry.val, entry.key, entry.ev.EventTS, true, false, 0, "")
			return
		}
	}

	ev := *f.engine.Next(now)
	f.st.mu.Lock()
	f.st.issued++
	f.st.mu.Unlock()

	payload, key, tsForRec, malType := f.buildPayload(ev)
	if payload == nil {
		return
	}

	if malType == "" {
		f.pushRing(ev, payload, key)
		if f.cfg.lateRate > 0 && f.rng.Float64() < f.cfg.lateRate {
			hold := time.Duration(f.rng.Int63n(int64(f.cfg.lateMax) + 1))
			f.hold(ev, payload, key, tsForRec, hold, hold)
			return
		}
	}
	f.emit(ev, payload, key, tsForRec, false, false, 0, malType)
}

// hold schedules an emission for later, making it late. simHold is the delay
// recorded in the ledger; wallHold is the real scheduling delay (they differ
// in backfill mode, where simulated time runs faster than wall time).
func (f *faucet) hold(ev sim.Event, payload, key []byte, tsForRec int64, simHold, wallHold time.Duration) {
	f.st.mu.Lock()
	f.st.lateIssued++
	f.st.held++
	f.st.mu.Unlock()
	f.holds.Add(1)
	go func() {
		defer f.holds.Done()
		time.Sleep(wallHold)
		f.emit(ev, payload, key, tsForRec, false, true, simHold, "")
		f.st.mu.Lock()
		f.st.held--
		f.st.mu.Unlock()
	}()
}

func (f *faucet) runLive(ctx context.Context) error {
	log.Printf("live mode: %.0f eps -> %s (key=%s, dup=%.0f%%, malformed=%.0f%%, late=%.0f%% up to %s, ooo<= %s)",
		f.cfg.rate, f.cfg.topic, f.cfg.keyBy, f.cfg.dupRate*100, f.cfg.malformedRte*100, f.cfg.lateRate*100, f.cfg.lateMax, f.cfg.oooMax)
	interval := time.Duration(float64(time.Second) / f.cfg.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	started := time.Now()
	for {
		select {
		case <-ctx.Done():
			log.Printf("interrupted; shutting down cleanly")
			return nil
		case <-ticker.C:
			f.tickLive(time.Now())
			if f.cfg.duration > 0 && time.Since(started) >= f.cfg.duration {
				log.Printf("duration %s reached; shutting down cleanly", f.cfg.duration)
				return nil
			}
		}
	}
}

func (f *faucet) runBackfill(ctx context.Context) error {
	wallStarted := time.Now()
	start := time.Now().Add(-f.cfg.backfillDuration).UTC()
	if f.cfg.backfillStart != "" {
		t, err := time.Parse(time.RFC3339, f.cfg.backfillStart)
		if err != nil {
			return fmt.Errorf("--backfill-start: %w", err)
		}
		start = t
	}
	end := start.Add(f.cfg.backfillDuration)
	log.Printf("backfill mode: event-time [%s .. %s) at %.0f eps simulated (late holds /%.0f)",
		start.Format(time.RFC3339), end.Format(time.RFC3339), f.cfg.rate, f.cfg.backfillSpeedup)

	simNow := start
	pace := time.Duration(0)
	if f.cfg.backfillMaxEps > 0 {
		pace = time.Duration(float64(time.Second) / f.cfg.backfillMaxEps)
	}
	for simNow.Before(end) {
		select {
		case <-ctx.Done():
			log.Printf("interrupted; shutting down cleanly")
			return nil
		default:
		}
		ev := *f.engine.Next(simNow)
		f.st.mu.Lock()
		f.st.issued++
		f.st.mu.Unlock()

		payload, key, tsForRec, malType := f.buildPayload(ev)
		if payload == nil {
			continue
		}

		if malType != "" {
			f.emit(ev, payload, key, tsForRec, false, false, 0, malType)
		} else {
			f.pushRing(ev, payload, key)
			if f.cfg.lateRate > 0 && f.rng.Float64() < f.cfg.lateRate {
				simHold := time.Duration(f.rng.Int63n(int64(f.cfg.lateMax) + 1))
				wallHold := simHold / time.Duration(f.cfg.backfillSpeedup)
				f.hold(ev, payload, key, tsForRec, simHold, wallHold)
			} else {
				f.emit(ev, payload, key, tsForRec, false, false, 0, "")
			}
		}

		// Advance the simulated clock by an exponentially distributed gap.
		simNow = simNow.Add(time.Duration(f.rng.ExpFloat64() / f.cfg.rate * float64(time.Second)))

		// Duplicates ride along after their original (ring is always populated).
		if f.cfg.dupRate > 0 && f.rng.Float64() < f.cfg.dupRate {
			if entry, ok := f.pickRing(); ok {
				f.st.mu.Lock()
				f.st.dupIssued++
				f.st.mu.Unlock()
				f.emit(entry.ev, entry.val, entry.key, entry.ev.EventTS, true, false, 0, "")
			}
		}
		if pace > 0 {
			time.Sleep(pace)
		}
	}
	log.Printf("backfill covered %s of event-time in %s wall time", f.cfg.backfillDuration, time.Since(wallStarted).Round(time.Second))
	return nil
}

// ---------------------------------------------------------------------------
// payload building and corruption
// ---------------------------------------------------------------------------

// buildPayload encodes ev and applies the malformed roll. Returns the payload,
// record key, record timestamp to use, and the malformed type ("" = clean).
func (f *faucet) buildPayload(ev sim.Event) (payload []byte, key []byte, tsForRec int64, malType string) {
	tsForRec = ev.EventTS
	keyByUser := f.cfg.keyBy == "user"
	if keyByUser {
		key = []byte(ev.UserID)
	} else {
		key = []byte(ev.SessionID)
	}

	if f.rng.Float64() >= f.cfg.malformedRte {
		payload, err := f.evc.EncodeEvent(&ev)
		if err != nil {
			// Encoding failure would be a generator bug; emit nothing and count it.
			f.st.mu.Lock()
			f.st.failed++
			f.st.mu.Unlock()
			log.Printf("BUG: encode failed: %v", err)
			return nil, key, tsForRec, ""
		}
		return payload, key, tsForRec, ""
	}

	// Malformed branch: four families, each hitting a different consumer
	// failure point. Semantic violations carry a lying payload; the ledger row
	// always records the intended (truth) values.
	switch f.rng.Intn(4) {
	case 0: // pure noise, no wire header at all
		b := make([]byte, 16+f.rng.Intn(48))
		f.rng.Read(b)
		return b, key, tsForRec, malGarbage

	case 1: // valid-looking header, wrong magic byte
		payload, _ := f.evc.EncodeEvent(&ev)
		bad := append([]byte(nil), payload...)
		bad[0] = 0x07
		return bad, key, tsForRec, malBadMagic

	case 2: // real header + current schema id, truncated body -> decode fails
		payload, _ := f.evc.EncodeEvent(&ev)
		broken := append([]byte(nil), payload[:7]...) // 5-byte header + 2 body bytes
		return broken, key, tsForRec, malCorruptBdy

	default: // valid avro, invalid semantics
		native := f.evc.Native(&ev)
		violation := f.rng.Intn(6)
		switch violation {
		case 0:
			native["revenue"] = codec.UnionDouble(-(0.99 + f.rng.Float64()*100))
			malType = "semantic:neg_revenue"
		case 1:
			native["qty"] = codec.UnionInt(0) // avro-valid int 0, semantically wrong
			malType = "semantic:zero_qty"
		case 2:
			native["user_id"] = ""
			malType = "semantic:empty_user"
		case 3:
			native["product_id"] = codec.UnionString("p-999999")
			malType = "semantic:unknown_product"
		case 4:
			native["event_ts"] = int64(0)
			tsForRec = 0
			malType = "semantic:epoch_ts"
		default:
			native["event_ts"] = time.Now().Add(30 * 365 * 24 * time.Hour).UnixMilli()
			malType = "semantic:future_ts"
		}
		payload, err := f.evc.EncodeNative(native)
		if err != nil {
			log.Printf("BUG: semantic encode failed: %v", err)
			return nil, key, tsForRec, ""
		}
		return payload, key, tsForRec, malType
	}
}

// ---------------------------------------------------------------------------
// dup ring
// ---------------------------------------------------------------------------

func (f *faucet) pushRing(ev sim.Event, payload, key []byte) {
	f.ring[f.ringNext] = ringEntry{key: append([]byte(nil), key...), val: append([]byte(nil), payload...), ev: ev}
	f.ringNext = (f.ringNext + 1) % len(f.ring)
}

func (f *faucet) pickRing() (ringEntry, bool) {
	for i := 0; i < 3; i++ {
		e := f.ring[f.rng.Intn(len(f.ring))]
		if e.val != nil {
			return e, true
		}
	}
	return ringEntry{}, false
}

// ---------------------------------------------------------------------------
// shutdown and reporting
// ---------------------------------------------------------------------------

func (f *faucet) drainHolds(ctx context.Context) {
	for {
		f.st.mu.Lock()
		held := f.st.held
		f.st.mu.Unlock()
		if held == 0 || ctx.Err() != nil {
			if held > 0 {
				log.Printf("abandoning %d in-flight late holds (their acks, if any, still land in the ledger)", held)
			}
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (f *faucet) reportLoop(ctx context.Context, started time.Time) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if f.cfg.quiet {
				continue
			}
			s := f.st.snapshot()
			elapsed := time.Since(started).Seconds()
			log.Printf("delivered=%d (%.0f eps) issued=%d dup=%d/%d malformed=%d/%d late=%d/%d failed=%d buffered=%d",
				s.delivered, float64(s.delivered)/elapsed, s.issued,
				s.dupRows, s.dupIssued, s.malRows, s.issued, s.lateRows, s.lateIssued,
				s.failed, f.kcl.BufferedProduceRecords())
		}
	}
}

func (f *faucet) summarize(started time.Time) {
	s := f.st.snapshot()
	ls := f.led.Stats()
	wall := time.Since(started)

	log.Printf("================ generator summary ================")
	log.Printf("wall time              : %s", wall.Round(time.Second))
	log.Printf("events issued          : %d (new, incl. malformed)", s.issued)
	log.Printf("duplicates issued      : %d", s.dupIssued)
	log.Printf("delivered (acked)      : %d (%.0f eps avg)", s.delivered, float64(s.delivered)/wall.Seconds())
	log.Printf("delivery failures      : %d", s.failed)
	log.Printf("ledger rows            : %d -> %s", ls.Rows, f.led.Path())
	log.Printf("  duplicates           : %d", ls.Duplicates)
	log.Printf("  malformed            : %d %v", ls.Malformed, sortedCounts(ls.MalformedBy))
	log.Printf("  late                 : %d", ls.Late)
	log.Printf("  by event type        : %v", sortedCounts(ls.ByType))
	log.Printf("  event-time span      : %s .. %s", msToUTC(ls.FirstTS), msToUTC(ls.LastTS))
	log.Printf("ground-truth revenue   : %.2f (purchases, first pass, non-malformed)", ls.Revenue)
	if len(s.partitions) > 0 {
		log.Printf("partition distribution : %v", sortedPartitions(s.partitions))
	}
	log.Printf("====================================================")
	log.Printf("reconcile hint: distinct clean events = delivered - duplicate rows = %d",
		s.delivered-s.dupRows)
}

func sortedCounts(m map[string]int) string {
	type kv struct {
		k string
		v int
	}
	var out []kv
	for k, v := range m {
		out = append(out, kv{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].k < out[j].k })
	parts := make([]string, len(out))
	for i, e := range out {
		parts[i] = fmt.Sprintf("%s=%d", e.k, e.v)
	}
	return "{" + strings.Join(parts, " ") + "}"
}

func sortedPartitions(m map[int32]int) string {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("p%d=%d", k, m[int32(k)])
	}
	return "{" + strings.Join(parts, " ") + "}"
}

func msToUTC(ms int64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}
