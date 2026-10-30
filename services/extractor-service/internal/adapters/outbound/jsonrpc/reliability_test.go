package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/inbound/indexer"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

type rpcCycle struct {
	client *Client
	calls  int
}

func (u *rpcCycle) Cycle(ctx context.Context) (inbound.IndexResult, error) {
	u.calls++
	_, err := u.client.ChainID(ctx)
	return inbound.IndexResult{Done: err == nil}, err
}

func rpcResponse(r *http.Request, body string, code int) *http.Response {
	var q wireRequest
	_ = json.NewDecoder(r.Body).Decode(&q)
	return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,%s}`, q.ID, body)))}
}

// Real adapter retries and real worker cycles share a deterministic virtual clock.
func TestRPCPolicyAcrossWorkerCycles(t *testing.T) {
	for _, tc := range []struct {
		name             string
		status           int
		body             string
		attempts, cycles int
		budget           uint64
		want             error
	}{
		{"401", 401, "", 1, 1, 100, outbound.ErrSourcePermanent},
		{"402", 402, "", 1, 1, 100, outbound.ErrSourcePermanent},
		{"403", 403, "", 1, 1, 100, outbound.ErrSourcePermanent},
		{"501", 501, "", 1, 1, 100, outbound.ErrSourcePermanent},
		{"429", 429, "", 6, 3, 100, nil},
		{"500", 500, "", 6, 3, 100, nil}, {"502", 502, "", 6, 3, 100, nil},
		{"503", 503, "", 6, 3, 100, nil}, {"504", 504, "", 6, 3, 100, nil},
		{"transport", 0, "", 6, 3, 100, ErrTransport},
		{"timeout", -1, "", 6, 3, 100, context.DeadlineExceeded},
		{"budget across cycles", 503, "", 3, 2, 3, outbound.ErrRPCBudgetExceeded},
		{"wrong chain", 200, `"result":"0x2"`, 1, 1, 100, domain.ErrChainMismatch},
		{"RPC quota", 200, `"error":{"code":402,"message":"private-secret"}`, 1, 1, 100, outbound.ErrSourcePermanent},
		{"recover next cycle", 503, "", 3, 2, 100, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempts := 0
				cfg := testConfig("https://example.invalid/private-secret")
				cfg.MaxAttempts = 2
				cfg.MaxRequests = tc.budget
				client, err := New(cfg, &http.Client{Transport: budgetTransport(func(r *http.Request) (*http.Response, error) {
					attempts++
					if tc.name == "recover next cycle" && attempts == 3 {
						return rpcResponse(r, `"result":"0x1"`, 200), nil
					}
					if tc.status == 0 {
						return nil, errors.New("private transport detail")
					}
					if tc.status == -1 {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					return rpcResponse(r, tc.body, tc.status), nil
				})})
				if err != nil {
					t.Fatal(err)
				}
				u := &rpcCycle{client: client}
				var logs bytes.Buffer
				worker, err := indexer.New(u, slog.New(slog.NewJSONHandler(&logs, nil)), time.Millisecond, time.Minute, 3)
				if err != nil {
					t.Fatal(err)
				}
				err = worker.Run(t.Context())
				if tc.name == "recover next cycle" {
					if err != nil {
						t.Fatal(err)
					}
				} else if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
					t.Fatal("wrong error classification", err)
				}
				if attempts != tc.attempts || u.calls != tc.cycles || client.Requests() != uint64(attempts) {
					t.Fatal("retry policy", attempts, u.calls, client.Requests())
				}
				if strings.Contains(logs.String(), "private") || err != nil && strings.Contains(err.Error(), "private") {
					t.Fatal("secret disclosed")
				}
			})
		})
	}
}

func TestRetryAfterTimingAndCycleHalt(t *testing.T) {
	for _, mode := range []string{"seconds", "date", "too long", "exhausted", "cancel backoff", "cancel pacing"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := testConfig("https://example.invalid/private-secret")
				cfg.InitialBackoff = time.Second
				cfg.MaxBackoff = 5 * time.Second
				cfg.MaxRequests = 100
				if mode == "exhausted" {
					cfg.MaxAttempts = 1
				}
				if mode == "cancel pacing" {
					cfg.MinRequestInterval = 5 * time.Second
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var times []time.Time
				var cancelTimer *time.Timer
				defer func() {
					if cancelTimer != nil {
						cancelTimer.Stop()
					}
				}()
				client, err := New(cfg, &http.Client{Transport: budgetTransport(func(r *http.Request) (*http.Response, error) {
					times = append(times, time.Now())
					if len(times) > 1 {
						return rpcResponse(r, `"result":"0x1"`, 200), nil
					}
					if strings.HasPrefix(mode, "cancel") {
						cancelTimer = time.AfterFunc(time.Second, cancel)
					}
					if mode == "cancel pacing" {
						return rpcResponse(r, `"result":"0x1"`, 200), nil
					}
					resp := rpcResponse(r, "", 429)
					after := "2"
					if mode == "date" {
						after = time.Now().Add(2 * time.Second).UTC().Format(http.TimeFormat)
					}
					if mode == "too long" {
						after = "60"
					}
					resp.Header.Set("Retry-After", after)
					return resp, nil
				})})
				if err != nil {
					t.Fatal(err)
				}
				if mode == "cancel pacing" {
					if _, err := client.ChainID(ctx); err != nil {
						t.Fatal(err)
					}
					if _, err := client.ChainID(ctx); !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				} else {
					u := &rpcCycle{client: client}
					worker, _ := indexer.New(u, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Millisecond, time.Minute, 5)
					err = worker.Run(ctx)
					if mode == "too long" || mode == "exhausted" {
						if !errors.Is(err, outbound.ErrRetryDeferred) || u.calls != 1 {
							t.Fatal("worker bypassed cooldown", err, u.calls)
						}
					} else if err != nil {
						t.Fatal(err)
					}
				}
				want := 1
				if mode == "seconds" || mode == "date" {
					want = 2
					if len(times) == 2 && times[1].Sub(times[0]) != 2*time.Second {
						t.Fatal("Retry-After not respected", times)
					}
				}
				if len(times) != want || client.Requests() != uint64(want) {
					t.Fatal("unexpected retry", len(times), client.Requests())
				}
			})
		})
	}
}

type unreadableBody struct {
	reads  int
	closed bool
}

func (b *unreadableBody) Read([]byte) (int, error) {
	b.reads++
	return 0, errors.New("private-body-failure")
}
func (b *unreadableBody) Close() error { b.closed = true; return nil }
func TestPermanentHTTPClassificationDoesNotReadErrorBody(t *testing.T) {
	for _, code := range []int{401, 402, 403} {
		body := &unreadableBody{}
		client, err := New(testConfig("https://example.invalid/private-secret"), &http.Client{Transport: budgetTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: code, Body: body, Header: http.Header{"Retry-After": []string{"120"}}}, nil
		})})
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.ChainID(t.Context())
		if !errors.Is(err, outbound.ErrSourcePermanent) || errors.Is(err, outbound.ErrRetryDeferred) || body.reads != 0 || !body.closed || client.Requests() != 1 {
			t.Fatal("permanent response was retried/read", err, body)
		}
	}
}
