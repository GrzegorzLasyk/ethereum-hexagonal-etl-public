// server.go implements loopback HTTP routes and controlled local server lifecycle in the shared
// technical support layer, outside service domains.

// Package health provides optional local readiness for service composition roots.
// It has no knowledge of service domains or downstream processing completion.
package health

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// Start binds only loopback. Readiness checks the owning service's dependencies;
// a downstream outage must not make a durable receiver unready.
func Start(ctx context.Context, address string, check func(context.Context) error, metrics ...http.Handler) (func(), error) {
	if address == "" {
		return func() {}, nil
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || check == nil {
		return nil, errors.New("health address must be a loopback IP and port")
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, errors.New("cannot open health listener")
	}
	serving, stop := context.WithCancel(ctx)
	s := &http.Server{Handler: Handler(serving, check, metrics...), ReadHeaderTimeout: time.Second, WriteTimeout: 3 * time.Second}
	done := make(chan struct{})
	go func() { _ = s.Serve(l); close(done) }()
	return func() {
		stop()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if s.Shutdown(shutdown) != nil {
			_ = s.Close()
		}
		<-done
	}, nil
}

// Handler builds the routes; Start additionally enforces trusted listener binding.
func Handler(ctx context.Context, check func(context.Context) error, metrics ...http.Handler) http.Handler {
	mux := http.NewServeMux()
	if len(metrics) > 0 && metrics[0] != nil {
		mux.Handle("GET /metrics", metrics[0])
	}
	mux.HandleFunc("GET /live", func(w http.ResponseWriter, r *http.Request) {
		if ctx.Err() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		call, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		stop := context.AfterFunc(ctx, cancel)
		defer stop()
		if ctx.Err() != nil || check(call) != nil || call.Err() != nil || ctx.Err() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
