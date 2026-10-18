package sim

import (
	"math/rand"
	"testing"
	"time"
)

func newTestEngine(seed int64) (*Engine, *Universe) {
	rng := rand.New(rand.NewSource(seed))
	uni := NewUniverse(rng, 100, 20)
	eng := NewEngine(uni, rng, 5*time.Second)
	return eng, uni
}

// TestEngineDeterministic: same seed => identical event sequence.
func TestEngineDeterministic(t *testing.T) {
	run := func() []Event {
		eng, _ := newTestEngine(42)
		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		var out []Event
		for i := 0; i < 500; i++ {
			out = append(out, *eng.Next(base.Add(time.Duration(i) * time.Second)))
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("event %d diverged:\n%+v\n%+v", i, a[i], b[i])
		}
	}
}

// TestEngineDifferentSeedsDiffer guards against a constant rng accidentally
// producing identical universes.
func TestEngineDifferentSeedsDiffer(t *testing.T) {
	engA, _ := newTestEngine(1)
	engB, _ := newTestEngine(2)
	base := time.Now()
	a := engA.Next(base)
	b := engB.Next(base)
	if a.EventID == b.EventID {
		t.Fatal("different seeds produced identical first events")
	}
}

func TestEventInvariants(t *testing.T) {
	eng, uni := newTestEngine(7)
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	var sawPurchase, sawPageview, sawClick bool
	for i := 0; i < 5000 && !(sawPurchase && sawPageview && sawClick); i++ {
		now := base.Add(time.Duration(i) * 100 * time.Millisecond)
		ev := eng.Next(now)

		if ev.EventTS > now.UnixMilli() {
			t.Fatalf("event_ts %d is in the future of stream now %d", ev.EventTS, now.UnixMilli())
		}
		if ev.EventTS < now.Add(-10*time.Second).UnixMilli() {
			t.Fatalf("ooo skew too large: %d", ev.EventTS)
		}
		if ev.UserID == "" || ev.SessionID == "" || ev.EventID == "" || ev.Page == "" {
			t.Fatalf("missing identity fields: %+v", ev)
		}

		switch ev.EventType {
		case "pageview":
			sawPageview = true
			if ev.ProductID != "" || ev.Qty != 0 || ev.Revenue != 0 {
				t.Fatalf("pageview carries purchase fields: %+v", ev)
			}
		case "click":
			sawClick = true
			if ev.ProductID == "" {
				t.Fatal("click without product")
			}
			if _, ok := uni.ProductByID(ev.ProductID); !ok {
				t.Fatalf("click references unknown product %s", ev.ProductID)
			}
		case "purchase":
			sawPurchase = true
			p, ok := uni.ProductByID(ev.ProductID)
			if !ok {
				t.Fatalf("purchase references unknown product %s", ev.ProductID)
			}
			if ev.Qty < 1 || ev.Qty > 3 {
				t.Fatalf("purchase qty out of range: %d", ev.Qty)
			}
			want := p.Price * float64(ev.Qty)
			if ev.Revenue <= 0 || absDiff(ev.Revenue, want) > 0.005 {
				t.Fatalf("revenue %.2f != price %.2f * qty %d", ev.Revenue, p.Price, ev.Qty)
			}
		default:
			t.Fatalf("unknown event type %q", ev.EventType)
		}
	}
	if !sawPageview || !sawClick || !sawPurchase {
		t.Fatalf("funnel incomplete: pageview=%v click=%v purchase=%v", sawPageview, sawClick, sawPurchase)
	}
}

func TestUniverseIDFormats(t *testing.T) {
	uni := NewUniverse(rand.New(rand.NewSource(1)), 3, 2)
	if uni.Users[0].ID != "u-000000" || uni.Users[2].ID != "u-000002" {
		t.Fatalf("bad user ids: %s %s", uni.Users[0].ID, uni.Users[2].ID)
	}
	if uni.Products[0].ID != "p-0000" {
		t.Fatalf("bad product id: %s", uni.Products[0].ID)
	}
	if u, ok := uni.UserByID("u-000001"); !ok || u.ID != "u-000001" {
		t.Fatal("UserByID lookup failed")
	}
	if _, ok := uni.UserByID("u-999999"); ok {
		t.Fatal("UserByID accepted out-of-range id")
	}
}

func absDiff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}
