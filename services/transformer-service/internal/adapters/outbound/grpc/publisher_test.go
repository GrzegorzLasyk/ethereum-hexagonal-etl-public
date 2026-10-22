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
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
	transport "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type recipient struct {
	etlv1.UnimplementedLoaderServiceServer
	handle func(context.Context, *etlv1.SubmitTransformedBlockRequest) (*etlv1.SubmitTransformedBlockResponse, error)
}

func (r *recipient) SubmitTransformedBlock(c context.Context, p *etlv1.SubmitTransformedBlockRequest) (*etlv1.SubmitTransformedBlockResponse, error) {
	return r.handle(c, p)
}
func publication() outbound.Publication {
	h, p := [32]byte{1}, [32]byte{2}
	return outbound.Publication{EventID: fmt.Sprintf("transformed-block/v1/1/42/%x", h), ChainID: "1", Number: 42, Hash: h, ParentHash: p, Provider: "fixture", ExtractedAt: time.Unix(100, 123), Payload: []byte(" { } "), SourceEventID: fmt.Sprintf("raw-block/v1/1/42/%x", h), TransformedAt: time.Unix(200, 456)}
}
func testPublisher(t *testing.T, handle func(context.Context, *etlv1.SubmitTransformedBlockRequest) (*etlv1.SubmitTransformedBlockResponse, error)) *Publisher {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := transport.NewServer()
	etlv1.RegisterLoaderServiceServer(server, &recipient{handle: handle})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	conn, e := transport.NewClient("passthrough:///test", transport.WithTransportCredentials(insecure.NewCredentials()), transport.WithContextDialer(func(c context.Context, _ string) (net.Conn, error) { return listener.DialContext(c) }))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { conn.Close(); server.Stop(); listener.Close(); <-done })
	return &Publisher{client: etlv1.NewLoaderServiceClient(conn), conn: conn, chain: "1", limits: grpccontract.DefaultLimits()}
}
func TestPublishVerifiedACKAndExactRetry(t *testing.T) {
	event := publication()
	var mu sync.Mutex
	var requests []*etlv1.SubmitTransformedBlockRequest
	p := testPublisher(t, func(_ context.Context, r *etlv1.SubmitTransformedBlockRequest) (*etlv1.SubmitTransformedBlockResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, r)
		status := etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_DURABLY_ACCEPTED
		if len(requests) > 1 {
			status = etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED
		}
		return &etlv1.SubmitTransformedBlockResponse{Acceptance: &etlv1.DurableAcceptance{EventId: r.Metadata.EventId, Status: status}}, nil
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
	if !bytes.Equal(r.PayloadJson, event.Payload) || r.SourceEventId != event.SourceEventID || !r.TransformedAt.AsTime().Equal(event.TransformedAt) || !r.Metadata.ExtractedAt.AsTime().Equal(event.ExtractedAt) {
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
			p := testPublisher(t, func(context.Context, *etlv1.SubmitTransformedBlockRequest) (*etlv1.SubmitTransformedBlockResponse, error) {
				if tc.code != codes.OK {
					return nil, status.Error(tc.code, "private-driver-detail")
				}
				return &etlv1.SubmitTransformedBlockResponse{Acceptance: tc.ack}, nil
			})
			c, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if e := p.Publish(c, publication()); !errors.Is(e, tc.want) {
				t.Fatal(e)
			}
		})
	}
}
func TestPublishLocalBudgetsAndCancellation(t *testing.T) {
	p := testPublisher(t, func(c context.Context, _ *etlv1.SubmitTransformedBlockRequest) (*etlv1.SubmitTransformedBlockResponse, error) {
		<-c.Done()
		return nil, c.Err()
	})
	c, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if e := p.Publish(c, publication()); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	p.limits = grpccontract.Limits{PayloadBytes: 1, MessageBytes: 2}
	if e := p.Publish(t.Context(), publication()); !errors.Is(e, outbound.ErrPublishOversize) {
		t.Fatal(e)
	}
	p.limits = grpccontract.Limits{PayloadBytes: 64, MessageBytes: 65}
	if e := p.Publish(t.Context(), publication()); !errors.Is(e, outbound.ErrPublishOversize) {
		t.Fatal("message metadata budget", e)
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
