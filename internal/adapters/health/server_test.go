package health

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReadinessAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var dependency error
	h := Handler(ctx, func(c context.Context) error {
		if _, ok := c.Deadline(); !ok {
			t.Error("unbounded readiness check")
		}
		return dependency
	})
	check := func(path string, want int) {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Fatalf("%s: %d, want %d", path, w.Code, want)
		}
		if w.Body.Len() != 0 && path != "/missing" {
			t.Fatal("dependency diagnostics leaked")
		}
	}
	check("/ready", 200)
	dependency = errors.New("private database connection")
	check("/ready", 503)
	check("/live", 200)
	dependency = nil
	check("/ready", 200)
	cancel()
	check("/live", 503)
	check("/ready", 503)
}

func TestMetricsDoesNotChangeHealth(t *testing.T) {
	h := Handler(t.Context(), func(context.Context) error { return errors.New("offline") }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("etl_database_snapshot_success 0\n")) }))
	for _, tc := range []struct {
		path string
		code int
	}{{"/metrics", 200}, {"/live", 200}, {"/ready", 503}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.code {
			t.Fatal(tc.path, w.Code)
		}
	}
}

func TestReadinessDeadlineAndConcurrentShutdown(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		h := Handler(t.Context(), func(ctx context.Context) error { <-ctx.Done(); return nil })
		w := httptest.NewRecorder()
		started := time.Now()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/ready", nil))
		if w.Code != 503 || w.Body.Len() != 0 || time.Since(started) > 2*time.Second {
			t.Fatal("deadline did not fail closed")
		}
	})
	t.Run("shutdown during successful check", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		entered := make(chan struct{})
		done := make(chan struct{})
		h := Handler(ctx, func(call context.Context) error { close(entered); <-call.Done(); return nil })
		w := httptest.NewRecorder()
		go func() { h.ServeHTTP(w, httptest.NewRequest("GET", "/ready", nil)); close(done) }()
		<-entered
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("shutdown did not cancel check")
		}
		if w.Code != 503 || w.Body.Len() != 0 {
			t.Fatal("shutdown reported ready")
		}
	})
}
func TestListenerRestrictionsAndClose(t *testing.T) {
	check := func(context.Context) error { return nil }
	for _, address := range []string{":0", "0.0.0.0:0", "[::]:0", "localhost:0", "192.0.2.1:0", "bad", "127.0.0.1:bad"} {
		if stop, err := Start(t.Context(), address, check); err == nil {
			stop()
			t.Fatal("untrusted/invalid listener accepted", address)
		}
	}
	if _, err := Start(t.Context(), "127.0.0.1:0", nil); err == nil {
		t.Fatal("nil check accepted")
	}
	for _, address := range []string{"", "127.0.0.1:0", "[::1]:0"} {
		stop, err := Start(t.Context(), address, check)
		if err != nil {
			t.Fatal(address, err)
		}
		stop()
		stop()
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if _, err := Start(t.Context(), listener.Addr().String(), check); err == nil {
		t.Fatal("occupied port accepted")
	}
}
