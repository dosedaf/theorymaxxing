// Package codec owns everything on the wire: the Avro schemas, Schema
// Registry registration, Confluent wire framing, and encode/decode helpers.
//
// Wire format (Confluent wire protocol v1):
//
//	byte 0      magic byte, always 0x00
//	bytes 1-4   schema ID, big-endian uint32
//	rest        Avro binary payload for that schema
//
// Subjects follow the default TopicNameStrategy: "<topic>-value".
package codec

import (
	"context"
	"embed"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/sr"

	"clickstream/internal/sim"
)

//go:embed schemas/*.avsc
var embedded embed.FS

// SchemaText returns the embedded .avsc text for the given file name
// (e.g. "clickstream-event-v1.avsc").
func SchemaText(name string) (string, error) {
	b, err := embedded.ReadFile("schemas/" + name)
	if err != nil {
		return "", fmt.Errorf("embedded schema %q: %w", name, err)
	}
	return string(b), nil
}

func eventSchemaName(version int) string {
	return fmt.Sprintf("clickstream-event-v%d.avsc", version)
}

// ---------------------------------------------------------------------------
// clickstream events
// ---------------------------------------------------------------------------

// Event encodes clickstream events against one registered schema version.
type Event struct {
	subject string
	id      int
	version int
	codec   *goavro.Codec
}

// RegisterEvent idempotently registers the clickstream event schema under
// subject (same text -> same schema ID, no duplicate subject versions) and
// returns an encoder bound to it. The subject's compatibility is pinned to
// BACKWARD so the v1 -> v2 evolution story holds.
func RegisterEvent(ctx context.Context, reg *sr.Client, subject string, version int) (*Event, error) {
	if version != 1 && version != 2 {
		return nil, fmt.Errorf("schema version %d not in {1,2}", version)
	}
	text, err := SchemaText(eventSchemaName(version))
	if err != nil {
		return nil, err
	}
	ss, err := reg.CreateSchema(ctx, subject, sr.Schema{Schema: text, Type: sr.TypeAvro})
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", subject, err)
	}
	for _, res := range reg.SetCompatibility(ctx, sr.SetCompatibility{Level: sr.CompatBackward}, subject) {
		if res.Err != nil {
			return nil, fmt.Errorf("set compatibility on %s: %w", subject, res.Err)
		}
	}
	return newEvent(subject, version, text, ss.ID)
}

func newEvent(subject string, version int, text string, id int) (*Event, error) {
	codec, err := goavro.NewCodec(text)
	if err != nil {
		return nil, fmt.Errorf("compile schema %s: %w", subject, err)
	}
	return &Event{subject: subject, id: id, version: version, codec: codec}, nil
}

// SchemaID is the registry-global ID this encoder stamps into every record.
func (e *Event) SchemaID() int { return e.id }

// Version is the schema version this encoder produces (1 or 2).
func (e *Event) Version() int { return e.version }

// Subject is the registry subject in use (e.g. "events.raw-value").
func (e *Event) Subject() string { return e.subject }

// EncodeEvent encodes ev with Confluent wire framing.
func (e *Event) EncodeEvent(ev *sim.Event) ([]byte, error) {
	return e.EncodeNative(e.native(ev))
}

// EncodeNative encodes a caller-built Avro native map with wire framing.
// Exposed so the generator can build deliberately invalid records.
func (e *Event) EncodeNative(native map[string]any) ([]byte, error) {
	body, err := e.codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("avro encode: %w", err)
	}
	return Wire(e.id, body), nil
}

// native builds the Avro native map; only fields present in the bound schema
// version are included (goavro rejects unknown fields). Nullable fields use
// the Union* wrappers below.
func (e *Event) native(ev *sim.Event) map[string]any {
	m := map[string]any{
		"event_id":   ev.EventID,
		"event_type": ev.EventType,
		"event_ts":   ev.EventTS,
		"user_id":    ev.UserID,
		"session_id": ev.SessionID,
		"page":       ev.Page,
		"product_id": UnionString(ev.ProductID),
		"qty":        UnionInt(ev.Qty),
		"revenue":    UnionDouble(ev.Revenue),
	}
	if e.version >= 2 {
		m["device"] = UnionString(ev.Device)
		m["referrer"] = UnionString(ev.Referrer)
	}
	return m
}

// Native exposes the native map for ev under this encoder's schema version.
// Mutate the returned map (e.g. to build semantic violations) and pass it to
// EncodeNative.
func (e *Event) Native(ev *sim.Event) map[string]any { return e.native(ev) }

// Union* wrappers: goavro requires non-null values of ("null", T) Avro unions
// to be tagged maps like {"string": "x"}; null is just nil.
func UnionString(s string) any {
	if s == "" {
		return nil
	}
	return map[string]any{"string": s}
}

func UnionInt(n int32) any {
	if n == 0 {
		return nil
	}
	return map[string]any{"int": n}
}

func UnionDouble(f float64) any {
	if f == 0 {
		return nil
	}
	return map[string]any{"double": f}
}

// Wire frames an Avro binary body with the Confluent header.
func Wire(schemaID int, body []byte) []byte {
	out := make([]byte, 5+len(body))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(schemaID))
	copy(out[5:], body)
	return out
}

// ---------------------------------------------------------------------------
// reference data
// ---------------------------------------------------------------------------

// Ref encodes reference-data rows (users, products) for the compacted topics.
type Ref struct {
	subject string
	id      int
	codec   *goavro.Codec
}

// RegisterUserSchema registers the ref-user schema under subject.
func RegisterUserSchema(ctx context.Context, reg *sr.Client, subject string) (*Ref, error) {
	return registerRef(ctx, reg, subject, "ref-user.avsc")
}

// RegisterProductSchema registers the ref-product schema under subject.
func RegisterProductSchema(ctx context.Context, reg *sr.Client, subject string) (*Ref, error) {
	return registerRef(ctx, reg, subject, "ref-product.avsc")
}

func registerRef(ctx context.Context, reg *sr.Client, subject, file string) (*Ref, error) {
	text, err := SchemaText(file)
	if err != nil {
		return nil, err
	}
	ss, err := reg.CreateSchema(ctx, subject, sr.Schema{Schema: text, Type: sr.TypeAvro})
	if err != nil {
		return nil, fmt.Errorf("register %s: %w", subject, err)
	}
	r, err := newRefFromText(text)
	if err != nil {
		return nil, fmt.Errorf("compile schema %s: %w", subject, err)
	}
	r.subject = subject
	r.id = ss.ID
	return r, nil
}

func newRefFromText(text string) (*Ref, error) {
	codec, err := goavro.NewCodec(text)
	if err != nil {
		return nil, err
	}
	return &Ref{codec: codec}, nil
}

// SchemaID is the registry-global ID stamped into every record.
func (r *Ref) SchemaID() int { return r.id }

// Subject is the registry subject in use.
func (r *Ref) Subject() string { return r.subject }

// EncodeUser encodes a user profile row (updatedAt in millis).
func (r *Ref) EncodeUser(u sim.User, updatedAt int64) ([]byte, error) {
	return r.encode(map[string]any{
		"user_id":        u.ID,
		"email":          u.Email,
		"full_name":      u.FullName,
		"country":        u.Country,
		"city":           u.City,
		"signup_ts":      u.SignupTS,
		"plan":           u.Plan,
		"loyalty_points": u.LoyaltyPoints,
		"is_active":      u.Active,
		"updated_at":     updatedAt,
	})
}

// EncodeProduct encodes a product row (updatedAt in millis).
func (r *Ref) EncodeProduct(p sim.Product, updatedAt int64) ([]byte, error) {
	return r.encode(map[string]any{
		"product_id": p.ID,
		"name":       p.Name,
		"category":   p.Category,
		"price":      p.Price,
		"currency":   "USD",
		"stock":      p.Stock,
		"is_active":  p.Active,
		"updated_at": updatedAt,
	})
}

func (r *Ref) encode(native map[string]any) ([]byte, error) {
	body, err := r.codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("avro encode: %w", err)
	}
	return Wire(r.id, body), nil
}

// ---------------------------------------------------------------------------
// decoding (helper for services that consume the faucet topics)
// ---------------------------------------------------------------------------

// Sentinel decode errors; consumers can branch on these when routing to a DLQ.
var (
	ErrTooShort    = errors.New("value too short for confluent wire header")
	ErrBadMagic    = errors.New("bad magic byte (not confluent wire format)")
	ErrUnknownData = errors.New("avro body has trailing garbage")
)

// Decoder decodes Confluent wire-format values, fetching codecs from the
// registry by schema ID on first use. Safe for concurrent use.
type Decoder struct {
	reg    *sr.Client
	mu     sync.Mutex
	codecs map[int]*goavro.Codec
}

// NewDecoder returns a decoder bound to a Schema Registry client.
func NewDecoder(reg *sr.Client) *Decoder {
	return &Decoder{reg: reg, codecs: make(map[int]*goavro.Codec)}
}

// Decode parses a wire-format value into a native map.
func (d *Decoder) Decode(value []byte) (map[string]any, error) {
	if len(value) < 5 {
		return nil, ErrTooShort
	}
	if value[0] != 0 {
		return nil, ErrBadMagic
	}
	id := int(binary.BigEndian.Uint32(value[1:5]))
	codec, err := d.codecFor(id)
	if err != nil {
		return nil, err
	}
	native, rest, err := codec.NativeFromBinary(value[5:])
	if err != nil {
		return nil, fmt.Errorf("avro decode (schema %d): %w", id, err)
	}
	if len(rest) != 0 {
		return nil, ErrUnknownData
	}
	m, ok := native.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("decoded value is %T, expected record", native)
	}
	return m, nil
}

func (d *Decoder) codecFor(id int) (*goavro.Codec, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.codecs[id]; ok {
		return c, nil
	}
	ss, err := d.reg.SchemaByID(context.Background(), id)
	if err != nil {
		return nil, fmt.Errorf("fetch schema %d: %w", id, err)
	}
	c, err := goavro.NewCodec(ss.Schema)
	if err != nil {
		return nil, fmt.Errorf("compile schema %d: %w", id, err)
	}
	d.codecs[id] = c
	return c, nil
}
