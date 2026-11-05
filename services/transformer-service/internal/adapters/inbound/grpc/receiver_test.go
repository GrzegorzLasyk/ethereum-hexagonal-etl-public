// receiver_test.go exercises gRPC-to-application request mapping and ACK-after-persistence
// semantics in the inbound adapter layer.

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

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/application/ports/inbound"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// acceptFunc adapts a callback to the durable inbound acceptance port for receiver tests.
type acceptFunc func(context.Context, inbound.AcceptRawBlockRequest) (inbound.AcceptanceResult, error)

// AcceptRawBlock delegates to the injected test callback, allowing controlled completion, blocking
// or failure at the application port.
func (f acceptFunc) AcceptRawBlock(ctx context.Context, r inbound.AcceptRawBlockRequest) (inbound.AcceptanceResult, error) {
	return f(ctx, r)
}

// request builds a valid deterministic application/transport request so one invariant can be
// varied per case.
func request() *etlv1.SubmitRawBlockRequest {
	block := &etlv1.BlockIdentity{ChainId: "1", Number: proto.Uint64(10), BlockHash: []byte(strings.Repeat("a", 32)), ParentHash: []byte(strings.Repeat("b", 32))}
	return &etlv1.SubmitRawBlockRequest{
		Metadata: &etlv1.EventMetadata{EventId: grpccontract.EventID("raw-block", block), EventVersion: 1, PayloadVersion: 1, Block: block, Provider: "infura-mainnet", ExtractedAt: timestamppb.New(time.Unix(100, 123))},
		Payload:  &etlv1.RawBlockPayload{BlockJson: []byte(" { \"transactions\": [], \"extra\": 123456789012345678901234567890 } "), Receipts: &etlv1.RawReceipts{Json: [][]byte{[]byte(" { \"logs\": [] } ")}}},
	}
}

// receiver wires a real gRPC receiver to the supplied acceptance fake with valid test limits.
func receiver(t *testing.T, a inbound.RawBlockAcceptor) *Receiver {
	t.Helper()
	r, err := NewReceiver("1", grpccontract.DefaultLimits(), a)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestMappingAndDurableOutcomes checks transport-to-port mapping and only explicit durable
// acceptance results become successful ACKs.
func TestMappingAndDurableOutcomes(t *testing.T) {
	for _, outcome := range []inbound.Acceptance{inbound.DurablyAccepted, inbound.AlreadyAccepted} {
		req := request()
		ctx, cancel := context.WithCancel(context.Background())
		var got inbound.AcceptRawBlockRequest
		r := receiver(t, acceptFunc(func(actual context.Context, input inbound.AcceptRawBlockRequest) (inbound.AcceptanceResult, error) {
			if actual != ctx {
				t.Error("context was replaced")
			}
			got = input
			// Simulate the port reporting commit, followed immediately by cancellation.
			cancel()
			return inbound.AcceptanceResult{EventID: input.EventID, Acceptance: outcome}, nil
		}))
		response, err := r.SubmitRawBlock(ctx, req)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		expected := inbound.AcceptRawBlockRequest{
			EventID: req.Metadata.EventId, EventVersion: 1, PayloadVersion: 1,
			Block:    inbound.BlockIdentity{ChainID: "1", Number: 10, Hash: [32]byte(req.Metadata.Block.BlockHash), ParentHash: [32]byte(req.Metadata.Block.ParentHash)},
			Provider: "infura-mainnet", ExtractedAt: time.Unix(100, 123).UTC(), RawBlock: req.Payload.BlockJson, RawReceipts: req.Payload.Receipts.Json,
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("mapping mismatch: got %#v; want %#v", got, expected)
		}
		if &got.RawBlock[0] != &req.Payload.BlockJson[0] || &got.RawReceipts[0][0] != &req.Payload.Receipts.Json[0][0] {
			t.Fatal("mapping copied payload buffers")
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

// TestValidationNeverDelegates checks invalid receiver metadata/payload cannot reach the durable
// acceptance port.
func TestValidationNeverDelegates(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*etlv1.SubmitRawBlockRequest)
		code   codes.Code
	}{
		{"missing metadata", func(r *etlv1.SubmitRawBlockRequest) { r.Metadata = nil }, codes.InvalidArgument},
		{"unsupported schema", func(r *etlv1.SubmitRawBlockRequest) { r.Metadata.PayloadVersion = 2 }, codes.FailedPrecondition},
		{"wrong chain", func(r *etlv1.SubmitRawBlockRequest) { r.Metadata.Block.ChainId = "2" }, codes.FailedPrecondition},
		{"event ID mismatch", func(r *etlv1.SubmitRawBlockRequest) { r.Metadata.EventId += "x" }, codes.InvalidArgument},
		{"message oversize", func(r *etlv1.SubmitRawBlockRequest) { r.Metadata.Provider = strings.Repeat("a", 2048) }, codes.ResourceExhausted},

		{"missing payload", func(r *etlv1.SubmitRawBlockRequest) { r.Payload = nil }, codes.InvalidArgument},
		{"missing receipts", func(r *etlv1.SubmitRawBlockRequest) { r.Payload.Receipts = nil }, codes.InvalidArgument},
		{"empty block bytes", func(r *etlv1.SubmitRawBlockRequest) { r.Payload.BlockJson = nil }, codes.InvalidArgument},
		{"null block", func(r *etlv1.SubmitRawBlockRequest) { r.Payload.BlockJson = []byte("null") }, codes.InvalidArgument},
		{"invalid block JSON", func(r *etlv1.SubmitRawBlockRequest) { r.Payload.BlockJson = []byte("{") }, codes.InvalidArgument},
		{"empty receipt", func(r *etlv1.SubmitRawBlockRequest) { r.Payload.Receipts.Json[0] = nil }, codes.InvalidArgument},
		{"receipt array", func(r *etlv1.SubmitRawBlockRequest) { r.Payload.Receipts.Json[0] = []byte("[]") }, codes.InvalidArgument},
		{"aggregate oversize", func(r *etlv1.SubmitRawBlockRequest) {
			r.Payload.Receipts.Json = [][]byte{make([]byte, 200), make([]byte, 200)}
		}, codes.ResourceExhausted},
	}
	r, err := NewReceiver("1", grpccontract.Limits{PayloadBytes: 256, MessageBytes: 1024}, acceptFunc(func(context.Context, inbound.AcceptRawBlockRequest) (inbound.AcceptanceResult, error) {
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
			resp, err := r.SubmitRawBlock(context.Background(), req)
			if resp != nil || status.Code(err) != tt.code {
				t.Fatalf("got %v, %v; want %v", resp, err, tt.code)
			}
		})
	}
	if response, err := r.SubmitRawBlock(context.Background(), nil); response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal(response, err)
	}
}

// TestAcceptanceFailuresNeverACK requires receiver errors to remain failures instead of
// synthesizing durable acceptance.
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
		{"overloaded", inbound.AcceptanceResult{}, inbound.ErrOverloaded, codes.Unavailable},
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
			r := receiver(t, acceptFunc(func(context.Context, inbound.AcceptRawBlockRequest) (inbound.AcceptanceResult, error) {
				if tt.err != nil {
					return tt.result, fmt.Errorf("private context: %w", tt.err)
				}
				return tt.result, nil
			}))
			resp, err := r.SubmitRawBlock(context.Background(), request())
			if resp != nil || status.Code(err) != tt.code {
				t.Fatalf("got %v,%v; want %v", resp, err, tt.code)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("error details leaked")
			}
		})
	}
	resp, err := receiver(t, nil).SubmitRawBlock(context.Background(), request())
	if resp != nil || status.Code(err) != codes.Unimplemented {
		t.Fatal("skeleton ACKed", resp, err)
	}
}

// TestNoACKBeforePortCompletes holds the acceptance port open and proves gRPC cannot acknowledge
// input still awaiting persistence.
func TestNoACKBeforePortCompletes(t *testing.T) {
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	r := receiver(t, acceptFunc(func(_ context.Context, req inbound.AcceptRawBlockRequest) (inbound.AcceptanceResult, error) {
		close(entered)
		<-release
		return inbound.AcceptanceResult{EventID: req.EventID, Acceptance: inbound.DurablyAccepted}, nil
	}))
	go func() { _, err := r.SubmitRawBlock(context.Background(), request()); finished <- err }()
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

// TestCanceledBeforeAcceptance requires an already-canceled request to avoid invoking durable
// acceptance.
func TestCanceledBeforeAcceptance(t *testing.T) {
	r := receiver(t, acceptFunc(func(context.Context, inbound.AcceptRawBlockRequest) (inbound.AcceptanceResult, error) {
		t.Fatal("canceled request reached port")
		return inbound.AcceptanceResult{}, nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if response, err := r.SubmitRawBlock(ctx, request()); response != nil || status.Code(err) != codes.Canceled {
		t.Fatal(response, err)
	}
	ctx, cancel = context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	if response, err := r.SubmitRawBlock(ctx, request()); response != nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(response, err)
	}
}

// TestConstructorRejectsInvalidConfig checks invalid configuration fails before opening a usable
// acquisition/receiver boundary.
func TestConstructorRejectsInvalidConfig(t *testing.T) {
	if _, err := NewServer("", grpccontract.DefaultLimits(), nil); err == nil {
		t.Fatal("missing chain accepted")
	}
	if _, err := NewServer("1", grpccontract.Limits{}, nil); err == nil {
		t.Fatal("missing limits accepted")
	}
}

// TestGRPCSkeletonAndLargeMessage checks the real transport boundary with bounded payloads.
//
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
	client := etlv1.NewTransformerServiceClient(conn)
	req := request()
	req.Payload.BlockJson = []byte("{\"padding\":\"" + strings.Repeat("x", 5<<20) + "\"}")
	for range 2 {
		response, err := client.SubmitRawBlock(ctx, req)
		if response != nil || status.Code(err) != codes.Unimplemented {
			t.Fatalf("valid retry must remain unimplemented: %v, %v", response, err)
		}
	}
}

// TestExactPayloadLimitAndEmptyObject checks inclusive payload limits and object shape without
// confusing size validity with domain completeness.
func TestExactPayloadLimitAndEmptyObject(t *testing.T) {
	req := request()
	req.Payload.BlockJson = []byte("{}")
	req.Payload.Receipts = &etlv1.RawReceipts{}

	calls := 0
	r, err := NewReceiver("1", grpccontract.Limits{PayloadBytes: 2, MessageBytes: 1024}, acceptFunc(func(_ context.Context, input inbound.AcceptRawBlockRequest) (inbound.AcceptanceResult, error) {
		calls++
		return inbound.AcceptanceResult{EventID: input.EventID, Acceptance: inbound.DurablyAccepted}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if response, err := r.SubmitRawBlock(context.Background(), req); err != nil || response == nil || calls != 1 {
		t.Fatal(response, err, calls)
	}
	req.Payload.Receipts.Json = [][]byte{[]byte("{}")}
	r.limits.PayloadBytes = 3
	if response, err := r.SubmitRawBlock(context.Background(), req); response != nil || status.Code(err) != codes.ResourceExhausted || calls != 1 {
		t.Fatal(response, err, calls)
	}
}
