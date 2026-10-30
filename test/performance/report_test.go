package performance

import (
	"testing"
	"time"
)

func TestNoIncompleteThroughput(t *testing.T) {
	r := Report{Scenario: Scenario{Blocks: 2}, Status: "failed", Integrity: map[string]bool{"handoffs": false}}
	if r.Accept(time.Second, 2, 4) == nil || r.BlocksPerSecond != nil {
		t.Fatal("failed work counted")
	}
	r.Integrity["handoffs"] = true
	if r.Accept(time.Second, 1, 4) == nil {
		t.Fatal("partial work counted")
	}
	if r.Accept(2*time.Second, 2, 4) != nil || *r.BlocksPerSecond != 1 || *r.TransactionsPerSecond != 2 {
		t.Fatal(r)
	}
}
func TestScenarioGuards(t *testing.T) {
	s := Scenario{Dataset: "synthetic-stage11", Workers: 1, Blocks: 10, Start: 100, Directory: "x", Binaries: "b"}
	if s.Validate() != nil {
		t.Fatal("valid scenario")
	}
	s.Workers = 3
	if s.Validate() == nil {
		t.Fatal("unreviewed concurrency")
	}
	s.Workers = 1
	s.Dataset = "authentic-mainnet"
	if s.Validate() == nil {
		t.Fatal("synthetic cannot impersonate authentic")
	}
}
