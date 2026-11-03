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

type Target struct {
	Number                       uint64
	Hash                         string
	Transactions, Receipts, Logs int
}

var MainnetBlock = Target{1000000, "8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e", 2, 2, 1}

type Query func(context.Context, string, string) ([]byte, error)
type Snapshot struct {
	Ready         bool              `json:"ready"`
	States        map[string]string `json:"states"`
	Stuck         bool              `json:"stuck"`
	RawDigest     string            `json:"raw_digest"`
	PayloadDigest string            `json:"payload_digest"`
	Provenance    string            `json:"provenance"`
	Members       []string          `json:"members"`
}
type Report map[string]Snapshot

var ErrIncomplete = errors.New("ETL completion deadline exceeded")
var ErrFailed = errors.New("ETL contains failed events")
var ErrStuck = errors.New("ETL contains expired processing/delivery leases")
var ErrObservation = errors.New("ETL database observation unavailable")

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
