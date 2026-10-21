package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type acceptFunc func(context.Context, inbound.AcceptTransformedBlockRequest) (inbound.AcceptanceResult, error)

func (f acceptFunc) AcceptTransformedBlock(ctx context.Context, r inbound.AcceptTransformedBlockRequest) (inbound.AcceptanceResult, error) {
	return f(ctx, r)
}

func request() *etlv1.SubmitTransformedBlockRequest {
	block := &etlv1.BlockIdentity{ChainId: "1", Number: proto.Uint64(10), BlockHash: []byte(strings.Repeat("a", 32)), ParentHash: []byte(strings.Repeat("b", 32))}
	return &etlv1.SubmitTransformedBlockRequest{
		Metadata:    &etlv1.EventMetadata{EventId: grpccontract.EventID("transformed-block", block), EventVersion: 1, PayloadVersion: 1, Block: block, Provider: "infura-mainnet", ExtractedAt: timestamppb.New(time.Unix(100, 123))},
		PayloadJson: []byte(" { \"opaque\": 123456789012345678901234567890 } "), SourceEventId: grpccontract.EventID("raw-block", block), TransformedAt: timestamppb.New(time.Unix(101, 456)),
	}
}

func receiver(t *testing.T, a inbound.TransformedBlockAcceptor) *Receiver {
	t.Helper()
	r, err := NewReceiver("1", grpccontract.DefaultLimits(), a)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestMappingAndDurableOutcomes(t *testing.T) {
	for _, outcome := range []inbound.Acceptance{inbound.DurablyAccepted, inbound.AlreadyAccepted} {
		req := request()
		ctx, cancel := context.WithCancel(context.Background())
		var got inbound.AcceptTransformedBlockRequest
		r := receiver(t, acceptFunc(func(actual context.Context, input inbound.AcceptTransformedBlockRequest) (inbound.AcceptanceResult, error) {
			if actual != ctx {
				t.Error("context was replaced")
			}
			got = input
			// Simulate the port reporting commit, followed immediately by cancellation.
			cancel()
			return inbound.AcceptanceResult{EventID: input.EventID, Acceptance: outcome}, nil
		}))
		response, err := r.SubmitTransformedBlock(ctx, req)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		expected := inbound.AcceptTransformedBlockRequest{
			EventID: req.Metadata.EventId, EventVersion: 1, PayloadVersion: 1,
			Block:    inbound.BlockIdentity{ChainID: "1", Number: 10, Hash: [32]byte(req.Metadata.Block.BlockHash), ParentHash: [32]byte(req.Metadata.Block.ParentHash)},
			Provider: "infura-mainnet", ExtractedAt: time.Unix(100, 123).UTC(), PayloadJSON: req.PayloadJson, SourceEventID: req.SourceEventId, TransformedAt: req.TransformedAt.AsTime(),
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("mapping mismatch: got %#v; want %#v", got, expected)
		}
		if &got.PayloadJSON[0] != &req.PayloadJson[0] {
			t.Fatal("mapping copied payload buffer")
		}
		wantStatus := etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_DURABLY_ACCEPTED
		if outcome == inbound.AlreadyAccepted {
			wantStatus = etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED
		}
		if response.Acceptance.EventId != req.Metadata.EventId || response.Acceptance.Status != wantStatus {
			t.Fatalf("bad ACK: %v", response)
		}
	}
}

func TestValidationNeverDelegates(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*etlv1.SubmitTransformedBlockRequest)
		code   codes.Code
	}{
		{"missing metadata", func(r *etlv1.SubmitTransformedBlockRequest) { r.Metadata = nil }, codes.InvalidArgument},
		{"unsupported schema", func(r *etlv1.SubmitTransformedBlockRequest) { r.Metadata.PayloadVersion = 2 }, codes.FailedPrecondition},
		{"wrong chain", func(r *etlv1.SubmitTransformedBlockRequest) { r.Metadata.Block.ChainId = "2" }, codes.FailedPrecondition},
		{"event ID mismatch", func(r *etlv1.SubmitTransformedBlockRequest) { r.Metadata.EventId += "x" }, codes.InvalidArgument},
		{"message oversize", func(r *etlv1.SubmitTransformedBlockRequest) { r.Metadata.Provider = strings.Repeat("a", 2048) }, codes.ResourceExhausted},
		{"missing payload", func(r *etlv1.SubmitTransformedBlockRequest) { r.PayloadJson = nil }, codes.InvalidArgument},
		{"null payload", func(r *etlv1.SubmitTransformedBlockRequest) { r.PayloadJson = []byte("null") }, codes.InvalidArgument},
		{"array payload", func(r *etlv1.SubmitTransformedBlockRequest) { r.PayloadJson = []byte("[]") }, codes.InvalidArgument},
		{"invalid JSON", func(r *etlv1.SubmitTransformedBlockRequest) { r.PayloadJson = []byte("{") }, codes.InvalidArgument},
		{"source identity", func(r *etlv1.SubmitTransformedBlockRequest) { r.SourceEventId += "x" }, codes.InvalidArgument},
		{"missing transformation time", func(r *etlv1.SubmitTransformedBlockRequest) { r.TransformedAt = nil }, codes.InvalidArgument},
		{"invalid transformation time", func(r *etlv1.SubmitTransformedBlockRequest) { r.TransformedAt.Nanos = -1 }, codes.InvalidArgument},
		{"time before extraction", func(r *etlv1.SubmitTransformedBlockRequest) { r.TransformedAt = timestamppb.New(time.Unix(99, 0)) }, codes.InvalidArgument},
		{"payload oversize", func(r *etlv1.SubmitTransformedBlockRequest) { r.PayloadJson = make([]byte, 257) }, codes.ResourceExhausted},
	}
	r, err := NewReceiver("1", grpccontract.Limits{PayloadBytes: 256, MessageBytes: 1024}, acceptFunc(func(context.Context, inbound.AcceptTransformedBlockRequest) (inbound.AcceptanceResult, error) {
		t.Fatal("invalid request reached acceptance")
		return inbound.AcceptanceResult{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := request()
			tt.mutate(req)
			resp, err := r.SubmitTransformedBlock(context.Background(), req)
			if resp != nil || status.Code(err) != tt.code {
				t.Fatalf("got %v, %v; want %v", resp, err, tt.code)
			}
		})
	}
	if response, err := r.SubmitTransformedBlock(context.Background(), nil); response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal(response, err)
	}
}

func TestAcceptanceFailuresNeverACK(t *testing.T) {
	tests := []struct {
		name   string
		result inbound.AcceptanceResult
		err    error
		code   codes.Code
	}{
		{"zero result", inbound.AcceptanceResult{}, nil, codes.Internal},
		{"foreign identity", inbound.AcceptanceResult{EventID: "other", Acceptance: inbound.DurablyAccepted}, nil, codes.Internal},
		{"unknown outcome", inbound.AcceptanceResult{EventID: request().Metadata.EventId, Acceptance: 99}, nil, codes.Internal},
		{"invalid", inbound.AcceptanceResult{}, inbound.ErrInvalidEvent, codes.InvalidArgument},
		{"unsupported", inbound.AcceptanceResult{}, inbound.ErrUnsupportedVersion, codes.FailedPrecondition},
		{"conflict", inbound.AcceptanceResult{}, inbound.ErrConflict, codes.AlreadyExists},
		{"offline", inbound.AcceptanceResult{}, inbound.ErrUnavailable, codes.Unavailable},
		{"unimplemented", inbound.AcceptanceResult{}, inbound.ErrNotImplemented, codes.Unimplemented},
		{"canceled", inbound.AcceptanceResult{}, context.Canceled, codes.Canceled},
		{"deadline", inbound.AcceptanceResult{}, context.DeadlineExceeded, codes.DeadlineExceeded},
		{"ambiguous commit", inbound.AcceptanceResult{}, errors.New("private database details"), codes.Internal},
		// Even a positive result must not override a simultaneous error.
		{"result plus error", inbound.AcceptanceResult{EventID: request().Metadata.EventId, Acceptance: inbound.DurablyAccepted}, inbound.ErrUnavailable, codes.Unavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := receiver(t, acceptFunc(func(context.Context, inbound.AcceptTransformedBlockRequest) (inbound.AcceptanceResult, error) {
				if tt.err != nil {
					return tt.result, fmt.Errorf("private context: %w", tt.err)
				}
				return tt.result, nil
			}))
			resp, err := r.SubmitTransformedBlock(context.Background(), request())
			if resp != nil || status.Code(err) != tt.code {
				t.Fatalf("got %v,%v; want %v", resp, err, tt.code)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("error details leaked")
			}
		})
	}
	resp, err := receiver(t, nil).SubmitTransformedBlock(context.Background(), request())
	if resp != nil || status.Code(err) != codes.Unimplemented {
		t.Fatal("skeleton ACKed", resp, err)
	}
}

func TestNoACKBeforePortCompletes(t *testing.T) {
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	r := receiver(t, acceptFunc(func(_ context.Context, req inbound.AcceptTransformedBlockRequest) (inbound.AcceptanceResult, error) {
		close(entered)
		<-release
		return inbound.AcceptanceResult{EventID: req.EventID, Acceptance: inbound.DurablyAccepted}, nil
	}))
	go func() { _, err := r.SubmitTransformedBlock(context.Background(), request()); finished <- err }()
	<-entered
	select {
	case err := <-finished:
		t.Fatalf("returned before acceptance: %v", err)
	default:
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestCanceledBeforeAcceptance(t *testing.T) {
	r := receiver(t, acceptFunc(func(context.Context, inbound.AcceptTransformedBlockRequest) (inbound.AcceptanceResult, error) {
		t.Fatal("canceled request reached port")
		return inbound.AcceptanceResult{}, nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if response, err := r.SubmitTransformedBlock(ctx, request()); response != nil || status.Code(err) != codes.Canceled {
		t.Fatal(response, err)
	}
	ctx, cancel = context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	if response, err := r.SubmitTransformedBlock(ctx, request()); response != nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(response, err)
	}
}

func TestConstructorRejectsInvalidConfig(t *testing.T) {
	if _, err := NewServer("", grpccontract.DefaultLimits(), nil); err == nil {
		t.Fatal("missing chain accepted")
	}
	if _, err := NewServer("1", grpccontract.Limits{}, nil); err == nil {
		t.Fatal("missing limits accepted")
	}
}

// This test exercises protobuf encoding, service registration and configured
// transport limits entirely in memory, with no listener or production sender.
func TestGRPCSkeletonAndLargeMessage(t *testing.T) {
	limits := grpccontract.DefaultLimits()
	server, err := NewServer("1", limits, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); listener.Close(); <-serveDone })
	options, err := limits.ClientOptions()
	if err != nil {
		t.Fatal(err)
	}
	options = append(options, grpcgo.WithTransportCredentials(insecure.NewCredentials()), grpcgo.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	conn, err := grpcgo.NewClient("passthrough:///test", options...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := etlv1.NewLoaderServiceClient(conn)
	req := request()
	req.PayloadJson = []byte("{\"padding\":\"" + strings.Repeat("x", 5<<20) + "\"}")
	for range 2 {
		response, err := client.SubmitTransformedBlock(ctx, req)
		if response != nil || status.Code(err) != codes.Unimplemented {
			t.Fatalf("valid retry must remain unimplemented: %v, %v", response, err)
		}
	}
}

func TestExactPayloadLimitAndEmptyObject(t *testing.T) {
	req := request()
	req.PayloadJson = []byte("{}")

	calls := 0
	r, err := NewReceiver("1", grpccontract.Limits{PayloadBytes: 2, MessageBytes: 1024}, acceptFunc(func(_ context.Context, input inbound.AcceptTransformedBlockRequest) (inbound.AcceptanceResult, error) {
		calls++
		return inbound.AcceptanceResult{EventID: input.EventID, Acceptance: inbound.DurablyAccepted}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if response, err := r.SubmitTransformedBlock(context.Background(), req); err != nil || response == nil || calls != 1 {
		t.Fatal(response, err, calls)
	}
	req.PayloadJson = []byte("{} ")
	if response, err := r.SubmitTransformedBlock(context.Background(), req); response != nil || status.Code(err) != codes.ResourceExhausted || calls != 1 {
		t.Fatal(response, err, calls)
	}
}
