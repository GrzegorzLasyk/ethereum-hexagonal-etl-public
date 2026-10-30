package grpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	transport "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type recipient struct {
	etlv1.UnimplementedTransformerServiceServer
	handle func(context.Context, *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error)
}

func (r *recipient) SubmitRawBlock(c context.Context, p *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
	return r.handle(c, p)
}
func publication() outbound.Publication {
	h, p := [32]byte{1}, [32]byte{2}
	return outbound.Publication{EventID: fmt.Sprintf("raw-block/v1/1/42/%x", h), ChainID: "1", Number: 42, Hash: h, ParentHash: p, Provider: "fixture", ExtractedAt: time.Unix(100, 123), RawBlock: []byte(" { } "), RawReceipts: [][]byte{[]byte("{}")}}
}
func testPublisher(t *testing.T, handle func(context.Context, *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error)) *Publisher {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := transport.NewServer()
	etlv1.RegisterTransformerServiceServer(server, &recipient{handle: handle})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	conn, e := transport.NewClient("passthrough:///test", transport.WithTransportCredentials(insecure.NewCredentials()), transport.WithContextDialer(func(c context.Context, _ string) (net.Conn, error) { return listener.DialContext(c) }))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { conn.Close(); server.Stop(); listener.Close(); <-done })
	return &Publisher{client: etlv1.NewTransformerServiceClient(conn), conn: conn, chain: "1", limits: grpccontract.DefaultLimits()}
}
func TestPublishVerifiedACKAndExactRetry(t *testing.T) {
	event := publication()
	var mu sync.Mutex
	var requests []*etlv1.SubmitRawBlockRequest
	p := testPublisher(t, func(_ context.Context, r *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, r)
		status := etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_DURABLY_ACCEPTED
		if len(requests) > 1 {
			status = etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED
		}
		return &etlv1.SubmitRawBlockResponse{Acceptance: &etlv1.DurableAcceptance{EventId: r.Metadata.EventId, Status: status}}, nil
	})
	for range 2 {
		c, cancel := context.WithTimeout(t.Context(), time.Second)
		e := p.Publish(c, event)
		cancel()
		if e != nil {
			t.Fatal(e)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 || !proto.Equal(requests[0], requests[1]) {
		t.Fatal("retry changed content")
	}
	r := requests[0]
	if !bytes.Equal(r.Payload.BlockJson, event.RawBlock) || !bytes.Equal(r.Payload.Receipts.Json[0], event.RawReceipts[0]) || !r.Metadata.ExtractedAt.AsTime().Equal(event.ExtractedAt) {
		t.Fatal("wire content/provenance changed")
	}
}
func TestPublishFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		ack  *etlv1.DurableAcceptance
		code codes.Code
		want error
	}{
		{name: "nil acceptance", want: outbound.ErrPublishACK},
		{name: "wrong ID", ack: &etlv1.DurableAcceptance{EventId: "other", Status: 1}, want: outbound.ErrPublishACK},
		{name: "unspecified", ack: &etlv1.DurableAcceptance{EventId: publication().EventID}, want: outbound.ErrPublishACK},
		{name: "unknown status", ack: &etlv1.DurableAcceptance{EventId: publication().EventID, Status: 99}, want: outbound.ErrPublishACK},
		{name: "conflicting duplicate", code: codes.AlreadyExists, want: outbound.ErrPublishConflict},
		{name: "budget", code: codes.ResourceExhausted, want: outbound.ErrPublishOversize},
		{name: "wrong chain", code: codes.FailedPrecondition, want: outbound.ErrPublishInvalid},
		{name: "invalid", code: codes.InvalidArgument, want: outbound.ErrPublishInvalid},
		{name: "offline", code: codes.Unavailable, want: outbound.ErrPublishUnavailable},
		{name: "not deployed", code: codes.Unimplemented, want: outbound.ErrPublishUnavailable},
		{name: "lost ACK", code: codes.DeadlineExceeded, want: outbound.ErrPublishUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testPublisher(t, func(context.Context, *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
				if tc.code != codes.OK {
					return nil, status.Error(tc.code, "private-driver-detail")
				}
				return &etlv1.SubmitRawBlockResponse{Acceptance: tc.ack}, nil
			})
			c, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if e := p.Publish(c, publication()); !errors.Is(e, tc.want) {
				t.Fatal(e)
			}
		})
	}
}

// publishClientFunc isolates caller-context handling from gRPC's independent
// client-stream and server deadline timers.
type publishClientFunc func(context.Context, *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error)

func (f publishClientFunc) SubmitRawBlock(ctx context.Context, r *etlv1.SubmitRawBlockRequest, _ ...transport.CallOption) (*etlv1.SubmitRawBlockResponse, error) {
	return f(ctx, r)
}

func TestPublishLocalBudgetsAndCancellation(t *testing.T) {
	calls := 0
	p := &Publisher{chain: "1", limits: grpccontract.DefaultLimits(), client: publishClientFunc(func(c context.Context, _ *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
		calls++
		// Wait for the caller's cancellation, not a remote deadline that can
		// expire before the caller's context timer has been scheduled.
		<-c.Done()
		return nil, status.FromContextError(c.Err()).Err()
	})}
	c, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if e := p.Publish(c, publication()); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	canceled, cancelNow := context.WithCancel(t.Context())
	cancelNow()
	if e := p.Publish(canceled, publication()); !errors.Is(e, context.Canceled) {
		t.Fatal("pre-canceled publication", e)
	}
	p.limits = grpccontract.Limits{PayloadBytes: 1, MessageBytes: 2}
	if e := p.Publish(t.Context(), publication()); !errors.Is(e, outbound.ErrPublishOversize) {
		t.Fatal(e)
	}
	p.limits = grpccontract.Limits{PayloadBytes: 64, MessageBytes: 65}
	if e := p.Publish(t.Context(), publication()); !errors.Is(e, outbound.ErrPublishOversize) {
		t.Fatal("message metadata budget", e)
	}
	if calls != 1 {
		t.Fatalf("local rejection reached RPC client: calls=%d, want 1", calls)
	}
	if _, e := New("remote:50051", "1", grpccontract.DefaultLimits()); e == nil {
		t.Fatal("unsafe target accepted")
	}
	// Lazy construction succeeds while the recipient is offline.
	conn, e := New("127.0.0.1:1", "1", grpccontract.DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
}

// Exercise cancellation over real gRPC only after the handler has started.
// No propagated deadline races with cancellation of the caller's context.
func TestPublishCancellationInFlight(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan error, 1)
	p := testPublisher(t, func(c context.Context, _ *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
		close(started)
		<-c.Done()
		stopped <- c.Err()
		return nil, c.Err()
	})
	c, cancel := context.WithCancel(t.Context())
	defer cancel()
	guard, stopGuard := context.WithTimeout(t.Context(), time.Second)
	defer stopGuard()
	result := make(chan error, 1)
	go func() { result <- p.Publish(c, publication()) }()
	select {
	case <-started:
	case err := <-result:
		t.Fatalf("publication ended before cancellation: %v", err)
	case <-guard.Done():
		t.Fatal("RPC handler did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation: %v", err)
		}
	case <-guard.Done():
		t.Fatal("publication did not stop after cancellation")
	}
	select {
	case err := <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("server cancellation: %v", err)
		}
	case <-guard.Done():
		t.Fatal("RPC handler did not observe cancellation")
	}
}

func TestPublishReceiverOverloadIsRetryable(t *testing.T) {
	p := testPublisher(t, func(context.Context, *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
		return nil, grpccontract.Overloaded()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := p.Publish(ctx, publication())
	if !errors.Is(err, outbound.ErrPublishOverloaded) || !errors.Is(err, outbound.ErrPublishUnavailable) || errors.Is(err, outbound.ErrPublishOversize) {
		t.Fatal("overload became permanent", err)
	}
}
