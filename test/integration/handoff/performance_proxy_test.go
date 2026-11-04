//go:build postgresintegration && perfbaseline

// performance_proxy_test.go exercises gRPC byte/timing observation without changing ACK semantics
// in the offline test and diagnostic infrastructure.

package handoff_test

import (
	"context"
	"sync"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/test/performance"
	"google.golang.org/protobuf/proto"
)

// meter synchronizes gRPC handoff counts, byte sizes and duration samples during a baseline.
type meter struct {
	mu    sync.Mutex
	stats performance.RPC
}

// observe records one measured gRPC call's bytes, latency and outcome under test-observer
// synchronization.
func (m *meter) observe(start time.Time, size int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats.Attempts++
	m.stats.PayloadBytes += int64(size)
	if err != nil {
		m.stats.Errors++
	}
	m.stats.Latency.Add(time.Since(start).Seconds())
}

// snapshot returns a synchronized copy of measured gRPC counts/latencies for reporting.
func (m *meter) snapshot() performance.RPC {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.stats
	v.Latency = v.Latency.Summary()
	return v
}

// measuredRaw forwards raw test handoffs while retaining timing and duplicate-payload evidence.
type measuredRaw struct {
	etlv1.UnimplementedTransformerServiceServer
	client etlv1.TransformerServiceClient
	meter  meter
	mu     sync.Mutex
	last   *etlv1.SubmitRawBlockRequest
}

// SubmitRawBlock forwards the real test-local handoff while recording payload bytes, duration and
// outcome without altering ACK semantics.
func (p *measuredRaw) SubmitRawBlock(ctx context.Context, r *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
	start := time.Now()
	ack, err := p.client.SubmitRawBlock(ctx, r)
	p.meter.observe(start, proto.Size(r), err)
	p.mu.Lock()
	p.last = r
	p.mu.Unlock()
	return ack, err
}

// measuredTransformed forwards transformed test handoffs while retaining timing and duplicate-
// payload evidence.
type measuredTransformed struct {
	etlv1.UnimplementedLoaderServiceServer
	client etlv1.LoaderServiceClient
	meter  meter
	mu     sync.Mutex
	last   *etlv1.SubmitTransformedBlockRequest
}

// SubmitTransformedBlock forwards the real test-local handoff while recording payload bytes,
// duration and outcome without altering ACK semantics.
func (p *measuredTransformed) SubmitTransformedBlock(ctx context.Context, r *etlv1.SubmitTransformedBlockRequest) (*etlv1.SubmitTransformedBlockResponse, error) {
	start := time.Now()
	ack, err := p.client.SubmitTransformedBlock(ctx, r)
	p.meter.observe(start, proto.Size(r), err)
	p.mu.Lock()
	p.last = r
	p.mu.Unlock()
	return ack, err
}
