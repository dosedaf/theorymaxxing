package codec

import (
	"os"
	"path/filepath"
	"testing"

	"clickstream/internal/sim"
)

// topDir resolves the repo root relative to this package so the test can
// compare embedded schemas against the user-facing copies in schemas/.
func topDir(t *testing.T) string {
	t.Helper()
	abs, err := filepath.Abs("../../schemas")
	if err != nil {
		t.Fatal(err)
	}
	return abs
}

// TestEmbeddedSchemasInSync fails if internal/codec/schemas and the
// user-facing schemas/ directory drift apart.
func TestEmbeddedSchemasInSync(t *testing.T) {
	for _, name := range []string{
		"clickstream-event-v1.avsc",
		"clickstream-event-v2.avsc",
		"ref-user.avsc",
		"ref-product.avsc",
	} {
		embedded, err := SchemaText(name)
		if err != nil {
			t.Fatalf("embedded %s: %v", name, err)
		}
		onDisk, err := os.ReadFile(filepath.Join(topDir(t), name))
		if err != nil {
			t.Fatalf("top-level %s: %v", name, err)
		}
		if embedded != string(onDisk) {
			t.Errorf("%s drifted: internal/codec/schemas and schemas/ differ; copy one over the other", name)
		}
	}
}

func newTestEvent(t *testing.T, version int) *Event {
	t.Helper()
	text, err := SchemaText(eventSchemaName(version))
	if err != nil {
		t.Fatal(err)
	}
	e, err := newEvent("events.raw-value", version, text, 7)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func decodeNative(t *testing.T, e *Event, wire []byte) map[string]any {
	t.Helper()
	if wire[0] != 0 {
		t.Fatal("magic byte not 0")
	}
	native, rest, err := e.codec.NativeFromBinary(wire[5:])
	if err != nil {
		t.Fatalf("roundtrip decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatal("trailing bytes after decode")
	}
	return native.(map[string]any)
}

func TestEventV2RoundTrip(t *testing.T) {
	e := newTestEvent(t, 2)
	if e.SchemaID() != 7 || e.Version() != 2 {
		t.Fatalf("bad encoder state: %+v", e)
	}
	ev := &sim.Event{
		EventID:   "0f0e0d0c-0b0a-4948-8765-4321fedcba98",
		EventType: "purchase",
		EventTS:   1767000000123,
		UserID:    "u-000042",
		SessionID: "s-00000017",
		Page:      "/checkout",
		ProductID: "p-0003",
		Qty:       2,
		Revenue:   59.98,
		Device:    "mobile",
		Referrer:  "google.com",
	}
	native := decodeNative(t, e, mustEncode(t, e, ev))
	// Nullable union fields decode as tagged maps ({"string": ...}); unwrap
	// them before comparing.
	checks := map[string]any{
		"event_id":   ev.EventID,
		"event_type": ev.EventType,
		"event_ts":   ev.EventTS,
		"user_id":    ev.UserID,
		"session_id": ev.SessionID,
		"page":       ev.Page,
		"product_id": ev.ProductID,
		"qty":        ev.Qty,
		"revenue":    ev.Revenue,
		"device":     ev.Device,
		"referrer":   ev.Referrer,
	}
	for k, want := range checks {
		got := unwrap(native[k])
		if got != want {
			t.Errorf("field %s = %#v, want %#v", k, got, want)
		}
	}
}

// unwrap flattens goavro union tagged maps to their datum value.
func unwrap(v any) any {
	if m, ok := v.(map[string]any); ok && len(m) == 1 {
		for _, datum := range m {
			return datum
		}
	}
	return v
}

func TestEventV1OmitsV2Fields(t *testing.T) {
	e := newTestEvent(t, 1)
	ev := &sim.Event{
		EventID:   "aaaabbbb-cccc-4ddd-8eee-ffff00001111",
		EventType: "pageview",
		EventTS:   1767000000456,
		UserID:    "u-000000",
		SessionID: "s-00000001",
		Page:      "/",
		Device:    "mobile", // must be dropped for v1
		Referrer:  "direct",
	}
	native := decodeNative(t, e, mustEncode(t, e, ev))
	if _, ok := native["device"]; ok {
		t.Error("v1 payload must not carry device")
	}
	if _, ok := native["referrer"]; ok {
		t.Error("v1 payload must not carry referrer")
	}
	if native["product_id"] != nil || native["qty"] != nil || native["revenue"] != nil {
		t.Error("pageview optionals must encode as null")
	}
}

func TestRefRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		file   string
		native func() map[string]any
	}{
		{
			file: "ref-user.avsc",
			native: func() map[string]any {
				return map[string]any{
					"user_id": "u-000001", "email": "a.b@example.com", "full_name": "A B",
					"country": "US", "city": "Austin", "signup_ts": int64(1672531200000),
					"plan": "pro", "loyalty_points": int32(120), "is_active": true,
					"updated_at": int64(1767000000000),
				}
			},
		},
		{
			file: "ref-product.avsc",
			native: func() map[string]any {
				return map[string]any{
					"product_id": "p-0002", "name": "Nimbus Mug", "category": "home",
					"price": 24.99, "currency": "USD", "stock": int32(42),
					"is_active": true, "updated_at": int64(1767000000000),
				}
			},
		},
	} {
		text, err := SchemaText(tc.file)
		if err != nil {
			t.Fatal(err)
		}
		codec, err := newRefFromText(text)
		if err != nil {
			t.Fatal(err)
		}
		native := tc.native()
		body, err := codec.encode(native)
		if err != nil {
			t.Fatalf("%s encode: %v", tc.file, err)
		}
		if body[0] != 0 {
			t.Fatalf("%s: wire magic byte not 0", tc.file)
		}
		got, rest, err := codec.codec.NativeFromBinary(body[5:]) // strip wire header
		if err != nil || len(rest) != 0 {
			t.Fatalf("%s roundtrip: %v (rest=%d)", tc.file, err, len(rest))
		}
		for k, want := range native {
			if got.(map[string]any)[k] != want {
				t.Errorf("%s field %s = %#v, want %#v", tc.file, k, got.(map[string]any)[k], want)
			}
		}
	}
}

func mustEncode(t *testing.T, e *Event, ev *sim.Event) []byte {
	t.Helper()
	wire, err := e.EncodeEvent(ev)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return wire
}
