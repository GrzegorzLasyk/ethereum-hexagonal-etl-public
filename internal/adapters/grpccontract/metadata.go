// Package grpccontract contains shared transport validation, never service domain models.
package grpccontract

import (
	"fmt"
	"regexp"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var providerLabel = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

func ValidChainID(id string) bool {
	if id == "" || (len(id) > 1 && id[0] == '0') {
		return false
	}
	for _, c := range id {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func EventID(kind string, block *etlv1.BlockIdentity) string {
	return fmt.Sprintf("%s/v1/%s/%d/%x", kind, block.GetChainId(), block.GetNumber(), block.GetBlockHash())
}

func ValidTimestamp(t *timestamppb.Timestamp) bool {
	return t != nil && t.CheckValid() == nil && !t.AsTime().IsZero()
}

// ValidateMetadata checks the envelope only, not Ethereum payload semantics.
func ValidateMetadata(m *etlv1.EventMetadata, kind, expectedChain string) error {
	if m == nil || m.Block == nil {
		return status.Error(codes.InvalidArgument, "event metadata and block identity are required")
	}
	if m.EventVersion != 1 || m.PayloadVersion != 1 {
		return status.Error(codes.FailedPrecondition, "unsupported event or payload version")
	}
	b := m.Block
	if !ValidChainID(b.ChainId) || b.Number == nil || !validHash(b.BlockHash, false) || !validHash(b.ParentHash, b.GetNumber() == 0) {
		return status.Error(codes.InvalidArgument, "invalid chain or block identity")
	}
	if b.ChainId != expectedChain {
		return status.Error(codes.FailedPrecondition, "chain does not match receiver configuration")
	}
	if m.EventId != EventID(kind, b) {
		return status.Error(codes.InvalidArgument, "event ID does not match block identity")
	}
	if !ValidTimestamp(m.ExtractedAt) || !providerLabel.MatchString(m.Provider) {
		return status.Error(codes.InvalidArgument, "invalid source provenance")
	}
	return nil
}

func validHash(b []byte, allowZero bool) bool {
	if len(b) != 32 {
		return false
	}
	return allowZero || [32]byte(b) != [32]byte{}
}
