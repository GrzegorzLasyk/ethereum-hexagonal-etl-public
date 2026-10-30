package jsonrpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
)

func TestRequestBudgetCoversBlockReceiptsAndRecoveryHeaders(t *testing.T) {
	f := serveFixtures(t, fixture(t, "block"), [2][]byte{fixture(t, "receipt_0"), fixture(t, "receipt_1")})
	cfg := testConfig(f.server.URL)
	cfg.MaxRequests = 12
	c := newClient(t, cfg)
	if _, err := c.Block(t.Context(), fixtureNumber); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Head(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Header(t.Context(), fixtureNumber); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ChainID(t.Context()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := c.Header(t.Context(), fixtureNumber); !errors.Is(err, outbound.ErrRPCBudgetExceeded) {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c.Requests() != 12 || len(f.requests) != 12 {
		t.Fatal("request count mismatch", c.Requests(), len(f.requests))
	}
}

func TestBudgetRejectsPartialSnapshot(t *testing.T) {
	f := serveFixtures(t, fixture(t, "block"), [2][]byte{fixture(t, "receipt_0"), fixture(t, "receipt_1")})
	cfg := testConfig(f.server.URL)
	cfg.MaxRequests = 3
	c := newClient(t, cfg)
	e, err := c.Block(t.Context(), fixtureNumber)
	if !errors.Is(err, outbound.ErrRPCBudgetExceeded) || len(e.RawBlock) != 0 || len(e.RawReceipts) != 0 || c.Requests() != 3 {
		t.Fatal("partial snapshot escaped", err)
	}
}

func TestBudgetIncludesRetriesAndConcurrentAttempts(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "concurrent"}[concurrent], func(t *testing.T) {
			var requests atomic.Uint64
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { requests.Add(1); w.WriteHeader(503) }))
			defer s.Close()
			cfg := testConfig(s.URL)
			cfg.MaxRequests = 2
			c := newClient(t, cfg)
			n := 1
			if concurrent {
				n = 20
			}
			var group sync.WaitGroup
			for range n {
				group.Go(func() {
					if _, err := c.ChainID(t.Context()); !errors.Is(err, outbound.ErrRPCBudgetExceeded) {
						t.Error(err)
					}
				})
			}
			group.Wait()
			if requests.Load() != 2 || c.Requests() != 2 {
				t.Fatal("budget exceeded or retry not counted", requests.Load(), c.Requests())
			}
		})
	}
}

type budgetTransport func(*http.Request) (*http.Response, error)

func (f budgetTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRunDeadlineCancelsRPCWithoutRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig("https://example.invalid/secret")
		cfg.MaxRequests = 10
		cfg.RequestTimeout = time.Hour
		var attempts int
		c, err := New(cfg, &http.Client{Transport: budgetTransport(func(r *http.Request) (*http.Response, error) {
			attempts++
			<-r.Context().Done()
			return nil, r.Context().Err()
		})})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		if _, err := c.ChainID(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if attempts != 1 || c.Requests() != 1 {
			t.Fatal("retried canceled run")
		}
		if _, err := c.ChainID(ctx); !errors.Is(err, context.DeadlineExceeded) || c.Requests() != 1 {
			t.Fatal("canceled call consumed budget", err)
		}
	})
}
