package health

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
)

func TestReadinessAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var dependency error
	h := handler(ctx, func(c context.Context) error {
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
