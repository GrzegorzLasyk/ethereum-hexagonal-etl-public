// indexing_test.go exercises range eligibility, admission and operation-identity rules in the pure
// domain layer.

package domain

import (
	"math"
	"testing"
)

// TestEligibility checks confirmation subtraction without unsigned underflow and allows the
// explicitly configured zero-depth case.
func TestEligibility(t *testing.T) {
	for _, c := range []struct {
		head, depth, want uint64
		ok                bool
	}{{0, 0, 0, true}, {0, 1, 0, false}, {10, 10, 0, true}, {10, 11, 0, false}, {10, 2, 8, true}, {math.MaxUint64, 1, math.MaxUint64 - 1, true}} {
		n, ok := EligibleHeight(c.head, c.depth)
		if n != c.want || ok != c.ok {
			t.Fatal(c, n, ok)
		}
	}
}

// TestCapacityAndOperationIdentity checks admission edge values and stable operator-ID validation
// used by audited recovery.
func TestCapacityAndOperationIdentity(t *testing.T) {
	l := CapacityLimits{10, 100, 1000}
	if l.Validate() != nil {
		t.Fatal("valid limits")
	}
	for _, c := range []Capacity{{Events: 10}, {Bytes: 100}, {DatabaseBytes: 1000}} {
		if c.Allows(l, 0) {
			t.Fatal(c)
		}
	}
	if !(Capacity{}).Allows(l, 100) || (Capacity{Bytes: 1}).Allows(l, 100) {
		t.Fatal("payload reservation")
	}
	for _, id := range []string{"", "a/b", "contains space"} {
		if ValidateOperationID(id) == nil {
			t.Fatal(id)
		}
	}
	if ValidateOperationID("replay-2026_10.1") != nil {
		t.Fatal("valid operation")
	}
}
