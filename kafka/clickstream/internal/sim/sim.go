// Package sim holds the deterministic virtual world shared by the data
// faucets: the user/product universe, the clickstream session engine, and
// clock handling for live and backfill runs.
//
// Determinism: build the Universe and the Engine (and everything else that
// draws randomness) from a single *rand.Rand seeded with the same --seed, in
// the same order, and the emitted stream is reproducible run to run. The
// universe of user/product IDs is identical between generator and refseed as
// long as both run with matching --seed/--users/--products.
package sim

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
)

// Plans is the plan ladder; index arithmetic is used for plan upgrades.
var Plans = []string{"free", "pro", "enterprise"}

// Categories is the product category set; also the Avro enum symbols in
// ref-product.avsc. Keep in sync.
var Categories = []string{"electronics", "apparel", "home", "beauty", "sports", "toys", "grocery", "books"}

var countries = []struct{ Country, City string }{
	{"US", "Seattle"}, {"US", "Austin"}, {"US", "Chicago"},
	{"DE", "Berlin"}, {"DE", "Munich"},
	{"GB", "London"}, {"FR", "Paris"}, {"ES", "Madrid"},
	{"IN", "Bangalore"}, {"BR", "Sao Paulo"}, {"JP", "Tokyo"}, {"AU", "Sydney"},
}

var firstNames = []string{"Ava", "Liam", "Noah", "Mia", "Ethan", "Zoe", "Kai", "Nina", "Omar", "Lena", "Ravi", "Sofia", "Yuki", "Marta", "Diego", "Aisha", "Tomas", "Ingrid", "Jin", "Clara"}
var lastNames = []string{"Reyes", "Kowalski", "Tanaka", "Silva", "Novak", "Haddad", "Fischer", "Moreau", "Okafor", "Lindqvist", "Carvalho", "Ivanov", "Nguyen", "Bauer", "Rossi", "Petrov", "Diaz", "Larsen", "Kim", "Weber"}

var productAdj = []string{"Ultra", "Cozy", "Swift", "Nimbus", "Ember", "Atlas", "Drift", "Halo", "Pebble", "Vantage", "Lumen", "Terra", "Quartz", "Nova", "Ridgeline"}
var productNoun = []string{"Lamp", "Hoodie", "Mug", "Speaker", "Notebook", "Blanket", "Water Bottle", "Sneakers", "Backpack", "Candle", "Headphones", "Keyboard", "Blender", "T-Shirt", "Sunglasses", "Watch", "Charger", "Plant Pot", "Yoga Mat", "Journal"}

var referrers = []string{"google.com", "news.ycombinator.com", "reddit.com", "twitter.com", "instagram.com", "newsletter", "producthunt.com"}

// User is a reference profile. The base profile is deterministic; refseed
// mutates copies of it over time.
type User struct {
	ID            string
	Email         string
	FullName      string
	Country       string
	City          string
	SignupTS      int64 // millis UTC, stable for the user's lifetime
	Plan          string
	LoyaltyPoints int32
	Active        bool
}

// Product is a reference catalog row. Price/stock are the seeded starting
// values; refseed mutates them over time.
type Product struct {
	ID       string
	Name     string
	Category string
	Price    float64
	Stock    int32
	Active   bool
}

// Universe is the deterministic population both faucets draw from.
type Universe struct {
	Users    []User
	Products []Product
}

// UserID formats a user id; generator, refseed, and ground truth all speak
// this exact format.
func UserID(i int) string { return fmt.Sprintf("u-%06d", i) }

// ProductID formats a product id.
func ProductID(i int) string { return fmt.Sprintf("p-%04d", i) }

// NewUniverse builds nUsers users and nProducts products from rng. Draw order
// matters for reproducibility: users first, then products, field by field.
func NewUniverse(rng *rand.Rand, nUsers, nProducts int) *Universe {
	u := &Universe{
		Users:    make([]User, nUsers),
		Products: make([]Product, nProducts),
	}
	base := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	span := int64(365 * 24 * 3600) // seconds of signup spread

	for i := range u.Users {
		cc := countries[rng.Intn(len(countries))]
		first := firstNames[rng.Intn(len(firstNames))]
		last := lastNames[rng.Intn(len(lastNames))]
		plan := "free"
		switch r := rng.Float64(); {
		case r < 0.10:
			plan = "enterprise"
		case r < 0.30:
			plan = "pro"
		}
		u.Users[i] = User{
			ID:            UserID(i),
			Email:         strings.ToLower(fmt.Sprintf("%s.%s%d@example.com", first, last, i)),
			FullName:      first + " " + last,
			Country:       cc.Country,
			City:          cc.City,
			SignupTS:      base + int64(rng.Intn(int(span)))*1000,
			Plan:          plan,
			LoyaltyPoints: rng.Int31n(5000),
			Active:        rng.Float64() > 0.05,
		}
	}

	for j := range u.Products {
		cat := Categories[rng.Intn(len(Categories))]
		price := math.Round((0.99+rng.Float64()*299)*100) / 100
		u.Products[j] = Product{
			ID:       ProductID(j),
			Name:     fmt.Sprintf("%s %s", productAdj[rng.Intn(len(productAdj))], productNoun[rng.Intn(len(productNoun))]),
			Category: cat,
			Price:    price,
			Stock:    rng.Int31n(500),
			Active:   true,
		}
	}
	return u
}

// ProductByID looks a product up by its ID.
func (u *Universe) ProductByID(id string) (Product, bool) {
	var n int
	if _, err := fmt.Sscanf(id, "p-%d", &n); err != nil || n < 0 || n >= len(u.Products) {
		return Product{}, false
	}
	return u.Products[n], true
}

// UserByID looks a user up by its ID.
func (u *Universe) UserByID(id string) (User, bool) {
	var n int
	if _, err := fmt.Sscanf(id, "u-%d", &n); err != nil || n < 0 || n >= len(u.Users) {
		return User{}, false
	}
	return u.Users[n], true
}

// Event is one logical clickstream event, before any wire encoding or noise
// corruption. EventType is one of "pageview", "click", "purchase".
type Event struct {
	EventID   string
	EventType string
	EventTS   int64 // millis UTC, already carries out-of-order skew
	UserID    string
	SessionID string
	Page      string
	ProductID string  // "" when not applicable
	Qty       int32   // 0 when not applicable
	Revenue   float64 // 0 when not applicable
	Device    string
	Referrer  string
}

const sessionTTL = 30 * time.Minute

type session struct {
	id         string
	userID     string
	startedAt  int64
	eventsLeft int
	willBuy    bool
	clicked    bool
	bought     bool
	productIdx int
	referrer   string
}

// Engine generates the clickstream funnel. It is not safe for concurrent use;
// drive it from a single goroutine.
type Engine struct {
	uni    *Universe
	rng    *rand.Rand
	oooMax time.Duration

	active  []*session
	byUser  map[string]*session
	sessSeq uint64
}

// NewEngine returns a funnel engine. oooMax is the maximum out-of-order skew
// applied to every event's timestamp (0 disables skew).
func NewEngine(uni *Universe, rng *rand.Rand, oooMax time.Duration) *Engine {
	return &Engine{
		uni:    uni,
		rng:    rng,
		oooMax: oooMax,
		byUser: make(map[string]*session),
	}
}

// Next emits the next event. eventTime is the "now" of the stream: wall clock
// in live mode, the simulated clock in backfill mode.
func (e *Engine) Next(eventTime time.Time) *Event {
	nowMs := eventTime.UnixMilli()
	s := e.pickSession(nowMs)
	ev := e.progress(s)
	s.eventsLeft--
	if s.eventsLeft <= 0 || s.bought {
		e.drop(s)
	}

	ev.EventID = e.uuid()
	if e.oooMax > 0 {
		// skew in millis (oooMax is a duration; don't mix units with nowMs)
		ev.EventTS = nowMs - e.rng.Int63n(e.oooMax.Milliseconds()+1)
	} else {
		ev.EventTS = nowMs
	}
	ev.Device = e.device()
	ev.Referrer = s.referrer
	return ev
}

func (e *Engine) pickSession(nowMs int64) *session {
	// 75% of traffic continues an existing session, the rest opens a new one.
	if len(e.active) > 0 && e.rng.Float64() < 0.75 {
		s := e.active[e.rng.Intn(len(e.active))]
		if nowMs-s.startedAt < int64(sessionTTL/time.Millisecond) {
			return s
		}
		e.drop(s) // expired; fall through to a fresh session
	}
	return e.newSession(nowMs)
}

func (e *Engine) newSession(nowMs int64) *session {
	user := e.uni.Users[e.rng.Intn(len(e.uni.Users))]
	if old, ok := e.byUser[user.ID]; ok {
		e.drop(old) // new session replaces the user's abandoned one
	}
	e.sessSeq++
	s := &session{
		id:         fmt.Sprintf("s-%08d", e.sessSeq),
		userID:     user.ID,
		startedAt:  nowMs,
		eventsLeft: 2 + e.rng.Intn(7), // 2..8 events per session
		willBuy:    e.rng.Float64() < 0.12,
		productIdx: e.rng.Intn(len(e.uni.Products)),
		referrer:   e.referrer(),
	}
	e.active = append(e.active, s)
	e.byUser[user.ID] = s
	return s
}

func (e *Engine) drop(s *session) {
	if cur, ok := e.byUser[s.userID]; ok && cur == s {
		delete(e.byUser, s.userID)
	}
	for i, a := range e.active {
		if a == s {
			e.active = append(e.active[:i], e.active[i+1:]...)
			break
		}
	}
}

// progress turns session state into the next event.
func (e *Engine) progress(s *session) *Event {
	ev := &Event{
		UserID:    s.userID,
		SessionID: s.id,
		ProductID: "",
	}
	p := e.uni.Products[s.productIdx]

	switch {
	case !s.clicked && s.willBuy && (s.eventsLeft <= 3 || e.rng.Float64() < 0.5):
		// buying session reaches a product page
		s.clicked = true
		ev.EventType = "click"
		ev.Page = "/products/" + p.ID
		ev.ProductID = p.ID

	case s.clicked && s.willBuy && !s.bought && (s.eventsLeft <= 1 || e.rng.Float64() < 0.45):
		// purchase lands
		s.bought = true
		qty := 1 + e.rng.Int31n(3)
		ev.EventType = "purchase"
		ev.Page = "/checkout"
		ev.ProductID = p.ID
		ev.Qty = qty
		ev.Revenue = math.Round(p.Price*float64(qty)*100) / 100

	default:
		ev.EventType = "pageview"
		ev.Page = e.browsePage(s.productIdx)
	}
	return ev
}

func (e *Engine) browsePage(productIdx int) string {
	r := e.rng.Float64()
	switch {
	case r < 0.30:
		return "/"
	case r < 0.50:
		return "/products/" + ProductID(productIdx)
	case r < 0.65:
		return "/products"
	case r < 0.80:
		return "/search?q=" + Categories[e.rng.Intn(len(Categories))]
	case r < 0.90:
		return "/cart"
	default:
		return "/account"
	}
}

func (e *Engine) device() string {
	r := e.rng.Float64()
	switch {
	case r < 0.58:
		return "mobile"
	case r < 0.92:
		return "desktop"
	default:
		return "tablet"
	}
}

func (e *Engine) referrer() string {
	if e.rng.Float64() < 0.45 {
		return "direct"
	}
	return referrers[e.rng.Intn(len(referrers))]
}

// uuid mints a random (version 4 shaped) id from the seeded rng. Deterministic
// per seed, unique enough for playground volumes.
func (e *Engine) uuid() string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], e.rng.Uint64())
	binary.LittleEndian.PutUint64(b[8:16], e.rng.Uint64())
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
