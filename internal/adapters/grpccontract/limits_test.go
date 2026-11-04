// limits_test.go exercises approved gRPC payload/message ceilings and validation in the shared
// technical support layer, outside service domains.

package grpccontract

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// TestLimitsValidation checks transport ceilings remain positive, ordered and within the approved
// payload/message maxima.
func TestLimitsValidation(t *testing.T) {
	for _, l := range []Limits{{}, {-1, 10}, {1, 0}, {10, 10}, {MaxPayloadBytes + 1, MaxMessageBytes}, {1, MaxMessageBytes + 1}} {
		if l.Validate() == nil {
			t.Fatalf("accepted invalid limits: %+v", l)
		}
		if _, err := l.ServerOptions(); err == nil {
			t.Fatal("server accepted invalid limits")
		}
		if _, err := l.ClientOptions(); err == nil {
			t.Fatal("client accepted invalid limits")
		}
	}
	if err := DefaultLimits().Validate(); err != nil {
		t.Fatal(err)
	}
}

// TestExactPayloadBounds checks inclusive payload limits and object shape without confusing size
// validity with domain completeness.
func TestExactPayloadBounds(t *testing.T) {
	l := DefaultLimits()
	// One allocation covers the approved boundary and the first rejected byte.
	b := make([]byte, MaxPayloadBytes+1)
	for _, tt := range []struct {
		name     string
		block    []byte
		receipts [][]byte
		want     codes.Code
	}{
		{"exact raw aggregate", b[:MaxPayloadBytes-1], [][]byte{b[:1]}, codes.OK},
		{"raw aggregate overflow", b[:MaxPayloadBytes-1], [][]byte{b[:1], b[:1]}, codes.ResourceExhausted},
		{"exact transformed", b[:MaxPayloadBytes], nil, codes.OK},
		{"transformed overflow", b, nil, codes.ResourceExhausted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := status.Code(l.CheckPayload(tt.block, tt.receipts...)); got != tt.want {
				t.Fatalf("got %v want %v", got, tt.want)
			}
		})
	}
}

// TestEncodedMessageBoundIncludesMetadataAndUnknownFields checks the protobuf size ceiling
// includes envelope and unknown fields, not only application payload bytes.
func TestEncodedMessageBoundIncludesMetadataAndUnknownFields(t *testing.T) {
	r := &etlv1.SubmitRawBlockRequest{Payload: &etlv1.RawBlockPayload{BlockJson: []byte("{}")}, Metadata: &etlv1.EventMetadata{Provider: strings.Repeat("a", 100)}}
	// Unknown future fields still consume the whole-message byte budget.
	r.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
	size := proto.Size(r)
	l := Limits{PayloadBytes: 2, MessageBytes: size}
	if err := l.CheckMessage(r); err != nil {
		t.Fatal(err)
	}
	l.MessageBytes--
	if status.Code(l.CheckMessage(r)) != codes.ResourceExhausted {
		t.Fatal("encoded message overflow accepted")
	}
}

// TestJSONObject distinguishes valid object payload boundaries from arrays, scalars and malformed
// JSON.
func TestJSONObject(t *testing.T) {
	for _, s := range []string{"{}", " \n { \"amount\":123456789012345678901234567890 } "} {
		if !JSONObject([]byte(s)) {
			t.Fatalf("rejected object %q", s)
		}
	}
	for _, s := range []string{"", "null", "[]", "42", "\"x\"", "{", "{} {}", "{\"x\":NaN}"} {
		if JSONObject([]byte(s)) {
			t.Fatalf("accepted invalid object %q", s)
		}
	}
}

// sizeServer returns a controlled gRPC response size for transport boundary tests.
type sizeServer struct {
	etlv1.UnimplementedTransformerServiceServer
	responseBytes int
}

// SubmitRawBlock implements the controlled gRPC peer/client callback so publication and ACK
// failures can be tested deterministically.
func (s *sizeServer) SubmitRawBlock(context.Context, *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
	return &etlv1.SubmitRawBlockResponse{Acceptance: &etlv1.DurableAcceptance{EventId: strings.Repeat("a", s.responseBytes)}}, nil
}

// TestGRPCSendReceiveLimits checks request and response size enforcement on both peers.
//
// Exercise all four gRPC directions; a helper that configures only server
// receive or client send would fail one of these independent cases.
func TestGRPCSendReceiveLimits(t *testing.T) {
	for _, tt := range []struct {
		name                                                  string
		serverLimit, clientLimit, requestBytes, responseBytes int
		want                                                  codes.Code
	}{
		{"success", 1024, 1024, 200, 200, codes.OK},
		{"server receive", 100, 1024, 200, 1, codes.ResourceExhausted},
		{"client send", 1024, 100, 200, 1, codes.ResourceExhausted},
		{"server send", 100, 1024, 1, 200, codes.ResourceExhausted},
		{"client receive", 1024, 100, 1, 200, codes.ResourceExhausted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := (Limits{PayloadBytes: 1, MessageBytes: tt.serverLimit}).ServerOptions()
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer(opts...)
			etlv1.RegisterTransformerServiceServer(server, &sizeServer{responseBytes: tt.responseBytes})
			listener := bufconn.Listen(1024)
			done := make(chan error, 1)
			go func() { done <- server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); listener.Close(); <-done })
			dial, err := (Limits{PayloadBytes: 1, MessageBytes: tt.clientLimit}).ClientOptions()
			if err != nil {
				t.Fatal(err)
			}
			dial = append(dial, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
			conn, err := grpc.NewClient("passthrough:///test", dial...)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = etlv1.NewTransformerServiceClient(conn).SubmitRawBlock(ctx, &etlv1.SubmitRawBlockRequest{Payload: &etlv1.RawBlockPayload{BlockJson: []byte(strings.Repeat("x", tt.requestBytes))}})
			if got := status.Code(err); got != tt.want {
				t.Fatalf("code=%v want=%v err=%v", got, tt.want, err)
			}
		})
	}
}
