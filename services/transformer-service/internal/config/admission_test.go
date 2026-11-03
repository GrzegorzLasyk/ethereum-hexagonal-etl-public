package config_test

import (
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/config"
	"testing"
)

func TestAdmissionLimits(t *testing.T) {
	setDefaults(t)
	for _, key := range []string{"MAX_OUTSTANDING_EVENTS", "MAX_INCOMING_REQUESTS", "MAX_INFLIGHT_BYTES", "MAX_GRPC_CONNECTIONS", "DB_MAX_CONNECTIONS"} {
		t.Setenv("TRANSFORMER_"+key, "")
	}
	got, e := config.Load()
	if e != nil {
		t.Fatal(e)
	}
	if got.AdmissionCapacity != 128 || got.Resources.Incoming != 2 || got.Resources.InFlightBytes != 2*got.Limits.MessageBytes || got.Resources.PoolConnections != 12 {
		t.Fatal("unsafe defaults")
	}
	for _, c := range []struct{ key, value string }{{"MAX_OUTSTANDING_EVENTS", "0"}, {"MAX_OUTSTANDING_EVENTS", "10001"}, {"MAX_INFLIGHT_BYTES", "1"}, {"MAX_INCOMING_REQUESTS", "9"}, {"MAX_GRPC_CONNECTIONS", "65"}, {"DB_MAX_CONNECTIONS", "33"}} {
		t.Run(c.key+c.value, func(t *testing.T) {
			t.Setenv("TRANSFORMER_"+c.key, c.value)
			if _, e := config.Load(); e == nil {
				t.Fatal("invalid resource accepted")
			}
		})
	}
}
