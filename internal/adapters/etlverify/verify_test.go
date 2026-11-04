// verify_test.go exercises bounded durable pipeline completion decisions independent of readiness
// alone in the shared technical support layer, outside service domains.

package etlverify

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// healthy builds complete synthetic service observations for verifier tests; it is not a database
// readiness check.
func healthy(service string) Snapshot {
	s := Snapshot{Ready: true, States: map[string]string{}, RawDigest: "raw", PayloadDigest: "payload", Provenance: "original", Members: []string{"tx1", "tx2"}}
	if service != "extractor" {
		s.States["inbox"] = "completed"
	}
	if service != "loader" {
		s.States["outbox"] = "delivered"
	}
	return s
}

// TestBoundedCompletionOffline requires all durable pipeline observations within a deadline and
// rejects partial or failed completion.
func TestBoundedCompletionOffline(t *testing.T) {
	for _, kind := range []string{"success", "eventually", "partial", "failed", "stuck", "database", "mismatch", "missing", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if kind == "cancel" {
					cancel()
				}
				cycles := 0
				q := func(ctx context.Context, service, sql string) ([]byte, error) {
					if _, ok := ctx.Deadline(); !ok {
						t.Fatal("unbounded query")
					}
					for _, other := range []string{"extractor", "transformer", "loader"} {
						if other != service && strings.Contains(sql, other+".") {
							t.Fatal("cross-schema query")
						}
					}
					if service == "extractor" {
						cycles++
					}
					s := healthy(service)
					switch kind {
					case "database":
						return nil, errors.New("private DSN/password")
					case "missing":
						return []byte(`{}`), nil
					case "partial":
						if service == "loader" {
							s.States["inbox"] = "pending"
						} else {
							s.States["outbox"] = "pending"
						}
					case "failed":
						if service == "extractor" {
							s.States["outbox"] = "failed"
						} else {
							s.States["inbox"] = "failed"
						}
					case "stuck":
						s.Stuck = true
					case "eventually":
						if cycles < 3 {
							s.Ready = false
						}
					case "mismatch":
						if service == "loader" {
							s.PayloadDigest = "other"
						}
					}
					return json.Marshal(s)
				}
				r, err := Wait(ctx, q, MainnetBlock, 3*time.Second, time.Second)
				switch kind {
				case "success", "eventually":
					if err != nil || !r.Complete() {
						t.Fatal(err)
					}
				case "failed":
					if !errors.Is(err, ErrFailed) {
						t.Fatal(err)
					}
				case "stuck":
					if !errors.Is(err, ErrStuck) {
						t.Fatal(err)
					}
				default:
					if !errors.Is(err, ErrIncomplete) || r.Complete() {
						t.Fatal("false success", err)
					}
				}
				if err != nil && strings.Contains(err.Error(), "private") {
					t.Fatal("secret disclosure")
				}
			})
		})
	}
}

// TestVerifierGuards checks incomplete configuration/observations fail closed instead of passing
// on readiness or checkpoint alone.
func TestVerifierGuards(t *testing.T) {
	if _, err := Wait(t.Context(), func(context.Context, string, string) ([]byte, error) {
		t.Fatal("query before bounds validation")
		return nil, nil
	}, MainnetBlock, 2*time.Minute+1, time.Second); err == nil {
		t.Fatal("unbounded verifier")
	}
	target := MainnetBlock
	target.Hash = "' OR true --"
	called := false
	if _, err := Observe(t.Context(), func(context.Context, string, string) ([]byte, error) { called = true; return nil, nil }, target); err == nil || called {
		t.Fatal("unsafe target")
	}
}

// TestKnownFailureSurvivesDownstreamObservationOutage requires a proven upstream terminal failure
// to remain visible when a downstream observation is unavailable.
func TestKnownFailureSurvivesDownstreamObservationOutage(t *testing.T) {
	q := func(ctx context.Context, service, sql string) ([]byte, error) {
		if service != "extractor" {
			return nil, errors.New("private connection details")
		}
		s := healthy(service)
		s.States["outbox"] = "failed"
		return json.Marshal(s)
	}
	r, err := Wait(t.Context(), q, MainnetBlock, time.Second, time.Millisecond)
	if !errors.Is(err, ErrFailed) || r.Complete() || !strings.Contains(r.Summary(), "transformer=unavailable") {
		t.Fatal("known failure was hidden", err)
	}
}
