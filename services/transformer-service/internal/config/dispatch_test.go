package config

import (
	"testing"
	"time"
)

func TestDispatchConfiguration(t *testing.T) {
	c, e := loadDispatch()
	if e != nil || c.Workers != 1 || c.MaxAttempts != 20 || c.Lease <= c.Timeout || c.Timeout <= c.RPCTimeout {
		t.Fatal(c, e)
	}
	for _, tc := range []struct{ key, value string }{{"TARGET", "example.com:80"}, {"TARGET", "127.0.0.1:0"}, {"WORKERS", "9"}, {"WORKERS", "x"}, {"MAX_ATTEMPTS", "0"}, {"RPC_TIMEOUT", "30s"}, {"LEASE", "30s"}, {"POLL_INTERVAL", "0"}, {"TIMEOUT", "-1s"}, {"INITIAL_BACKOFF", "2m"}} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			t.Setenv("TRANSFORMER_DISPATCH_"+tc.key, tc.value)
			if _, e := loadDispatch(); e == nil {
				t.Fatal("invalid dispatch configuration accepted")
			}
		})
	}
	t.Setenv("TRANSFORMER_DISPATCH_TARGET", "[::1]:5555")
	t.Setenv("TRANSFORMER_DISPATCH_RPC_TIMEOUT", "2s")
	c, e = loadDispatch()
	if e != nil || c.RPCTimeout != 2*time.Second {
		t.Fatal(c, e)
	}
}
