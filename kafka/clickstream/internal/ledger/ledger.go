// Package ledger writes the ground-truth journal: one JSON line per
// generator-emitted record, written only after Kafka acknowledged delivery.
// It is the oracle your pipeline gets reconciled against.
package ledger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Row is one ground-truth entry. See README for the reconciliation math.
type Row struct {
	EventID       string  `json:"event_id"`
	EventType     string  `json:"event_type"`
	EventTS       int64   `json:"event_ts_ms"`
	UserID        string  `json:"user_id"`
	SessionID     string  `json:"session_id"`
	Page          string  `json:"page,omitempty"`
	ProductID     string  `json:"product_id,omitempty"`
	Qty           int32   `json:"qty,omitempty"`
	Revenue       float64 `json:"revenue,omitempty"`
	Device        string  `json:"device,omitempty"`
	Referrer      string  `json:"referrer,omitempty"`
	SchemaVersion int     `json:"schema_version"`

	Key       string `json:"key"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`

	IsDuplicate   bool   `json:"is_duplicate"`
	IsMalformed   bool   `json:"is_malformed"`
	MalformedType string `json:"malformed_type,omitempty"`
	IsLate        bool   `json:"is_late"`
	HoldMs        int64  `json:"hold_ms,omitempty"`
	EmittedAt     int64  `json:"emitted_at_ms"`
}

// Stats is a snapshot of what the ledger has seen.
type Stats struct {
	Rows        int            `json:"rows"`
	Duplicates  int            `json:"duplicates"`
	Malformed   int            `json:"malformed"`
	MalformedBy map[string]int `json:"malformed_by_type"`
	Late        int            `json:"late"`
	ByType      map[string]int `json:"by_event_type"`
	Revenue     float64        `json:"revenue_first_pass"`
	FirstTS     int64          `json:"first_event_ts_ms"`
	LastTS      int64          `json:"last_event_ts_ms"`
	Partitions  map[string]int `json:"partitions"`
}

// Ledger is a mutex-guarded, buffered JSONL writer. Rows arrive from produce
// callbacks, so Write must be safe for concurrent use.
type Ledger struct {
	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	path string
	st   Stats
}

// New opens (truncating) the ground-truth file at path.
func New(path string) (*Ledger, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("open ground truth %s: %w", path, err)
	}
	return &Ledger{
		f:    f,
		w:    bufio.NewWriterSize(f, 1<<20),
		path: path,
		st: Stats{
			MalformedBy: make(map[string]int),
			ByType:      make(map[string]int),
			Partitions:  make(map[string]int),
		},
	}, nil
}

// Path is the file backing this ledger.
func (l *Ledger) Path() string { return l.path }

// Write appends one row and folds it into the running stats. Rows are written
// on delivery acknowledgement, so file order is ack order, not emit order.
func (l *Ledger) Write(r Row) {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, err := json.Marshal(r)
	if err != nil {
		// A marshaling failure is a bug; drop the row but keep the faucet live.
		return
	}
	l.w.Write(b)
	l.w.WriteByte('\n')

	st := &l.st
	st.Rows++
	if r.IsDuplicate {
		st.Duplicates++
	}
	if r.IsMalformed {
		st.Malformed++
		st.MalformedBy[r.MalformedType]++
	}
	if r.IsLate {
		st.Late++
	}
	st.ByType[r.EventType]++
	st.Partitions[fmt.Sprint(r.Partition)]++
	if !r.IsDuplicate && !r.IsMalformed && r.EventType == "purchase" {
		st.Revenue += r.Revenue
	}
	if st.FirstTS == 0 || r.EventTS < st.FirstTS {
		st.FirstTS = r.EventTS
	}
	if r.EventTS > st.LastTS {
		st.LastTS = r.EventTS
	}
}

// Flush pushes buffered rows to the OS.
func (l *Ledger) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Flush()
}

// Close flushes and closes the file.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.w.Flush(); err != nil {
		l.f.Close()
		return err
	}
	return l.f.Close()
}

// Stats returns a snapshot of the running totals.
func (l *Ledger) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.st
	c.MalformedBy = copyInts(l.st.MalformedBy)
	c.ByType = copyInts(l.st.ByType)
	c.Partitions = copyInts(l.st.Partitions)
	return c
}

func copyInts(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
