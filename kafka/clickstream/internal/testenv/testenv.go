// Package testenv spins up an in-memory Kafka (kfake) and an in-memory
// Schema Registry (srfake) so the faucets can be exercised end-to-end in
// tests without a real cluster.
package testenv

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

// Env is a throwaway kafka + schema registry pair.
type Env struct {
	Brokers []string
	SRURL   string

	Cluster *kfake.Cluster
	Reg     *srfake.Registry
}

// New boots the env and registers cleanup with t.
func New(t *testing.T) *Env {
	t.Helper()
	cl, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	reg := srfake.New()
	t.Cleanup(func() {
		reg.Close()
		cl.Close()
	})
	return &Env{
		Brokers: cl.ListenAddrs(),
		SRURL:   reg.URL(),
		Cluster: cl,
		Reg:     reg,
	}
}
