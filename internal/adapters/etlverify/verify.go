// verify.go implements bounded durable pipeline completion decisions independent of readiness
// alone in the shared technical support layer, outside service domains.

// Package etlverify is read-only operator tooling, not a service dependency.
// Each query must run as the named service's runtime role in its own connection.
package etlverify

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Target pins the chain, block identity and expected record counts required by the completion
// verifier.
type Target struct {
	Number                       uint64
	Hash                         string
	Transactions, Receipts, Logs int
}

// MainnetBlock pins the approved one-block Mainnet completion target, including hash and record
// counts.
var MainnetBlock = Target{1000000, "8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e", 2, 2, 1}

// Query runs an observation against one owning runtime role; verification never requires a cross-
// service transaction.
type Query func(context.Context, string, string) ([]byte, error)

// Snapshot captures one service observation used to decide durable pipeline completion.
type Snapshot struct {
	Ready         bool              `json:"ready"`
	States        map[string]string `json:"states"`
	Stuck         bool              `json:"stuck"`
	RawDigest     string            `json:"raw_digest"`
	PayloadDigest string            `json:"payload_digest"`
	Provenance    string            `json:"provenance"`
	Members       []string          `json:"members"`
}

// Report collects service completion evidence and distinguishes incomplete, failed and stuck work.
type Report map[string]Snapshot

// ErrIncomplete signals ETL completion deadline exceeded. Callers can match this category with
// errors.Is without parsing error text.
var ErrIncomplete = errors.New("ETL completion deadline exceeded")

// ErrFailed signals ETL contains failed events. Callers can match this category with errors.Is
// without parsing error text.
var ErrFailed = errors.New("ETL contains failed events")

// ErrStuck signals ETL contains expired processing/delivery leases. Callers can match this
// category with errors.Is without parsing error text.
var ErrStuck = errors.New("ETL contains expired processing/delivery leases")

// ErrObservation signals ETL database observation unavailable. Callers can match this category
// with errors.Is without parsing error text.
var ErrObservation = errors.New("ETL database observation unavailable")

// Validate requires a pinned chain/block/hash/count target before completion observations are
// accepted.
func (t Target) Validate() error {
	b, e := hex.DecodeString(t.Hash)
	if e != nil || len(b) != 32 || strings.ToLower(t.Hash) != t.Hash || t.Transactions < 0 || t.Receipts != t.Transactions || t.Logs < 0 {
		return errors.New("invalid verification target")
	}
	return nil
}

// Observe never turns unavailable DB data into healthy zero values. It returns
// safe status categories only; query/driver errors and payloads are not exposed.
func Observe(ctx context.Context, q Query, t Target) (Report, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	result := Report{}
	for _, service := range []string{"extractor", "transformer", "loader"} {
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		data, err := q(call, service, sqlFor(service, t))
		cancel()
		var s Snapshot
		if err != nil || json.Unmarshal(data, &s) != nil || !validStates(service, s.States) {
			return result, ErrObservation
		}
		result[service] = s
	}
	return result, nil
}

// Complete requires every service observation to be ready and complete, with no failed or stuck
// target work.
func (r Report) Complete() bool {
	ex, x := r["extractor"]
	tr, y := r["transformer"]
	lo, z := r["loader"]
	if !x || !y || !z || !ex.Ready || !tr.Ready || !lo.Ready || ex.RawDigest == "" || ex.RawDigest != tr.RawDigest || tr.PayloadDigest == "" || tr.PayloadDigest != lo.PayloadDigest || ex.Provenance == "" || ex.Provenance != tr.Provenance || ex.Provenance != lo.Provenance || !reflect.DeepEqual(ex.Members, tr.Members) || !reflect.DeepEqual(ex.Members, lo.Members) {
		return false
	}
	for service, s := range r {
		if s.Stuck {
			return false
		}
		for name, state := range s.States {
			want := "completed"
			if name == "outbox" {
				want = "delivered"
			}
			if state != want {
				return false
			}
		}
		if service == "extractor" && len(s.States) != 1 || service == "transformer" && len(s.States) != 2 || service == "loader" && len(s.States) != 1 {
			return false
		}
	}
	return true
}

// Wait polls complete durable pipeline evidence until success, known failure/stall or context
// deadline; a checkpoint alone cannot pass.
func Wait(ctx context.Context, q Query, t Target, timeout, poll time.Duration) (Report, error) {
	if q == nil || timeout <= 0 || timeout > 2*time.Minute || poll <= 0 {
		return nil, errors.New("invalid verifier bounds")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last Report
	for {
		if ctx.Err() != nil {
			return last, errors.Join(ErrIncomplete, ctx.Err())
		}
		r, err := Observe(ctx, q, t)
		last = r
		// A known terminal failure remains a failure even if another service
		// cannot be observed. Partial observations can never certify success.
		for _, s := range r {
			for _, state := range s.States {
				if state == "failed" {
					return last, ErrFailed
				}
			}
			if s.Stuck {
				return last, ErrStuck
			}
		}
		if err == nil {
			if r.Complete() {
				return last, nil
			}
		} else if !errors.Is(err, ErrObservation) {
			return last, err
		}

		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, errors.Join(ErrIncomplete, ctx.Err())
		case <-timer.C:
		}
	}
}

// Summary intentionally omits source payloads, provenance values and SQL errors.
func (r Report) Summary() string {
	var parts []string
	for _, service := range []string{"extractor", "transformer", "loader"} {
		s, ok := r[service]
		if !ok {
			parts = append(parts, service+"=unavailable")
			continue
		}
		parts = append(parts, fmt.Sprintf("%s ready=%t inbox=%s outbox=%s stuck=%t", service, s.Ready, s.States["inbox"], s.States["outbox"], s.Stuck))
	}
	return strings.Join(parts, "; ")
}

// validStates checks observed queue states against the completion protocol's fixed vocabulary.
func validStates(service string, states map[string]string) bool {
	expected := 1
	if service == "transformer" {
		expected = 2
	}
	if len(states) != expected {
		return false
	}
	for name, state := range states {
		if name == "inbox" && service != "extractor" {
			switch state {
			case "missing", "pending", "processing", "completed", "failed":
				continue
			}
		}
		if name == "outbox" && service != "loader" {
			switch state {
			case "missing", "pending", "sending", "delivered", "failed":
				continue
			}
		}
		return false
	}
	return true
}
