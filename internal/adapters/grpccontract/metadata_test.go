package grpccontract

import (
	"math"
	"strings"
	"testing"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func metadata() *etlv1.EventMetadata {
	b := &etlv1.BlockIdentity{ChainId: "1", Number: proto.Uint64(10), BlockHash: []byte(strings.Repeat("a", 32)), ParentHash: []byte(strings.Repeat("b", 32))}
	return &etlv1.EventMetadata{EventId: EventID("raw-block", b), EventVersion: 1, PayloadVersion: 1,
		Block: b, Provider: "infura-mainnet", ExtractedAt: timestamppb.New(time.Unix(100, 123))}
}

func TestMetadataValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*etlv1.EventMetadata)
		code   codes.Code
	}{
		{"valid", func(*etlv1.EventMetadata) {}, codes.OK},
		{"missing block", func(m *etlv1.EventMetadata) { m.Block = nil }, codes.InvalidArgument},
		{"event version", func(m *etlv1.EventMetadata) { m.EventVersion = 2 }, codes.FailedPrecondition},
		{"payload version", func(m *etlv1.EventMetadata) { m.PayloadVersion = 0 }, codes.FailedPrecondition},
		{"missing number", func(m *etlv1.EventMetadata) { m.Block.Number = nil }, codes.InvalidArgument},
		{"wrong chain", func(m *etlv1.EventMetadata) { m.Block.ChainId = "2" }, codes.FailedPrecondition},
		{"noncanonical chain", func(m *etlv1.EventMetadata) { m.Block.ChainId = "01" }, codes.InvalidArgument},
		{"short hash", func(m *etlv1.EventMetadata) { m.Block.BlockHash = []byte{1} }, codes.InvalidArgument},
		{"zero hash", func(m *etlv1.EventMetadata) { m.Block.BlockHash = make([]byte, 32) }, codes.InvalidArgument},
		{"short parent", func(m *etlv1.EventMetadata) { m.Block.ParentHash = nil }, codes.InvalidArgument},
		{"zero parent", func(m *etlv1.EventMetadata) { m.Block.ParentHash = make([]byte, 32) }, codes.InvalidArgument},
		{"event identity", func(m *etlv1.EventMetadata) { m.EventId += "/retry" }, codes.InvalidArgument},
		{"missing time", func(m *etlv1.EventMetadata) { m.ExtractedAt = nil }, codes.InvalidArgument},
		{"invalid time", func(m *etlv1.EventMetadata) { m.ExtractedAt.Nanos = -1 }, codes.InvalidArgument},
		{"zero time", func(m *etlv1.EventMetadata) { m.ExtractedAt = timestamppb.New(time.Time{}) }, codes.InvalidArgument},
		{"provider URL", func(m *etlv1.EventMetadata) { m.Provider = "https://secret" }, codes.InvalidArgument},
		{"provider length", func(m *etlv1.EventMetadata) { m.Provider = strings.Repeat("a", 65) }, codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := metadata()
			tt.change(m)
			if got := status.Code(ValidateMetadata(m, "raw-block", "1")); got != tt.code {
				t.Fatalf("code = %v, want %v", got, tt.code)
			}
		})
	}
	if status.Code(ValidateMetadata(nil, "raw-block", "1")) != codes.InvalidArgument {
		t.Fatal("nil metadata accepted")
	}
}

func TestIdentityBoundaries(t *testing.T) {
	for _, chain := range []string{"0", "1", "184467440737095516160"} {
		for _, number := range []uint64{0, math.MaxUint64} {
			m := metadata()
			m.Block.ChainId, m.Block.Number = chain, proto.Uint64(number)
			if number == 0 {
				m.Block.ParentHash = make([]byte, 32)
			}
			m.EventId = EventID("raw-block", m.Block)
			if err := ValidateMetadata(m, "raw-block", chain); err != nil {
				t.Fatal(err)
			}
			m.Provider, m.ExtractedAt = "other-provider", timestamppb.Now()
			if err := ValidateMetadata(m, "raw-block", chain); err != nil {
				t.Fatal("provenance changed stable identity:", err)
			}
		}
	}
	for _, chain := range []string{"", "00", "01", "-1", "+1", "0x1", " 1", "１"} {
		if ValidChainID(chain) {
			t.Fatalf("accepted chain %q", chain)
		}
	}
}
