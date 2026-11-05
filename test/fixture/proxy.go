// proxy.go implements local gRPC ACK-loss injection after actual downstream acceptance in the
// offline test and diagnostic infrastructure.

package fixture

import (
	"context"
	"crypto/sha256"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/gen/etl/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RawProxy injects response loss after forwarding to the durable receiver.
//
// Proxies exist only in the fixture process. An injected error is returned only
// AFTER a real receiver's durable ACK. They never persist or queue events.
type RawProxy struct {
	etlv1.UnimplementedTransformerServiceServer
	Fixture *Server
	Client  etlv1.TransformerServiceClient
}

// SubmitRawBlock validates the raw transport envelope and delegates durable acceptance; only
// explicit committed/already-accepted outcomes receive ACK.
func (p *RawProxy) SubmitRawBlock(ctx context.Context, r *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
	ack, err := p.Client.SubmitRawBlock(ctx, r)
	if err != nil {
		return nil, err
	}
	if p.Fixture.accepted(r.GetMetadata().GetEventId(), r, true, ack.GetAcceptance().GetStatus() == etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED) {
		return nil, status.Error(codes.Unavailable, "fixture dropped durable ACK")
	}
	return ack, nil
}

// TransformedProxy forwards transformed gRPC events and injects only the configured response loss
// after acceptance.
type TransformedProxy struct {
	etlv1.UnimplementedLoaderServiceServer
	Fixture *Server
	Client  etlv1.LoaderServiceClient
}

// SubmitTransformedBlock validates normalized transport metadata and delegates durable Inbox
// acceptance before returning ACK.
func (p *TransformedProxy) SubmitTransformedBlock(ctx context.Context, r *etlv1.SubmitTransformedBlockRequest) (*etlv1.SubmitTransformedBlockResponse, error) {
	ack, err := p.Client.SubmitTransformedBlock(ctx, r)
	if err != nil {
		return nil, err
	}
	if p.Fixture.accepted(r.GetMetadata().GetEventId(), r, false, ack.GetAcceptance().GetStatus() == etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED) {
		return nil, status.Error(codes.Unavailable, "fixture dropped durable ACK")
	}
	return ack, nil
}

// accepted updates fixture ACK/fault counters only after the downstream call has actually accepted
// the event.
func (s *Server) accepted(id string, message proto.Message, raw, duplicate bool) bool {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	s.mu.Lock()
	defer s.mu.Unlock()
	digest := sha256.Sum256(encoded)
	if previous, ok := s.seen[id]; ok && previous != digest || err != nil {
		s.state.Changed = true
	}
	// The synthetic dataset has at most 44 distinct events. Bound diagnostic state.
	if len(s.seen) < 128 {
		s.seen[id] = digest
	}
	if raw {
		if duplicate {
			s.state.RawDuplicates++
		}
		if s.dropRaw {
			s.dropRaw = false
			s.state.RawLost++
			return true
		}
	} else {
		if duplicate {
			s.state.TransformedDuplicates++
		}
		if s.dropTransformed {
			s.dropTransformed = false
			s.state.TransformedLost++
			return true
		}
	}
	return false
}
