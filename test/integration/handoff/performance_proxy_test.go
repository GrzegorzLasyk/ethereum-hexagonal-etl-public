//go:build postgresintegration && perfbaseline

package handoff_test

import (
	"context"
	"sync"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/test/performance"
	"google.golang.org/protobuf/proto"
)

type meter struct {
	mu    sync.Mutex
	stats performance.RPC
}

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
func (m *meter) snapshot() performance.RPC {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.stats
	v.Latency = v.Latency.Summary()
	return v
}

type measuredRaw struct {
	etlv1.UnimplementedTransformerServiceServer
	client etlv1.TransformerServiceClient
	meter  meter
	mu     sync.Mutex
	last   *etlv1.SubmitRawBlockRequest
}

func (p *measuredRaw) SubmitRawBlock(ctx context.Context, r *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
	start := time.Now()
	ack, err := p.client.SubmitRawBlock(ctx, r)
	p.meter.observe(start, proto.Size(r), err)
	p.mu.Lock()
	p.last = r
	p.mu.Unlock()
	return ack, err
}

type measuredTransformed struct {
	etlv1.UnimplementedLoaderServiceServer
	client etlv1.LoaderServiceClient
	meter  meter
	mu     sync.Mutex
	last   *etlv1.SubmitTransformedBlockRequest
}

func (p *measuredTransformed) SubmitTransformedBlock(ctx context.Context, r *etlv1.SubmitTransformedBlockRequest) (*etlv1.SubmitTransformedBlockResponse, error) {
	start := time.Now()
	ack, err := p.client.SubmitTransformedBlock(ctx, r)
	p.meter.observe(start, proto.Size(r), err)
	p.mu.Lock()
	p.last = r
	p.mu.Unlock()
	return ack, err
}
