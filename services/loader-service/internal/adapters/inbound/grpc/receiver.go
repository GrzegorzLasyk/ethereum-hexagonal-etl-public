// Package grpc implements the service's inbound transport boundary.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/logevent"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/logging"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Receiver struct {
	logger *slog.Logger
	etlv1.UnimplementedLoaderServiceServer
	acceptor inbound.TransformedBlockAcceptor
	chainID  string
	limits   grpccontract.Limits
}

var _ etlv1.LoaderServiceServer = (*Receiver)(nil)

// NewReceiver requires an explicit chain and validated transport budgets.
// A nil acceptor is the Stage 6 skeleton: valid requests return Unimplemented.
func NewReceiver(chainID string, limits grpccontract.Limits, acceptor inbound.TransformedBlockAcceptor, loggers ...*slog.Logger) (*Receiver, error) {
	if !grpccontract.ValidChainID(chainID) {
		return nil, fmt.Errorf("receiver requires a canonical decimal chain ID")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	var logger *slog.Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	return &Receiver{logger: logger, acceptor: acceptor, chainID: chainID, limits: limits}, nil
}

// NewServer registers the receiver and applies matching send/receive bounds.
// It opens no listener. Composition roots own serving and graceful shutdown.
func NewServer(chainID string, limits grpccontract.Limits, acceptor inbound.TransformedBlockAcceptor, loggers ...*slog.Logger) (*grpcgo.Server, error) {
	receiver, err := NewReceiver(chainID, limits, acceptor, loggers...)
	if err != nil {
		return nil, err
	}
	options, err := limits.ServerOptions()
	if err != nil {
		return nil, err
	}
	server := grpcgo.NewServer(options...)
	etlv1.RegisterLoaderServiceServer(server, receiver)
	return server, nil
}

func (r *Receiver) SubmitTransformedBlock(ctx context.Context, req *etlv1.SubmitTransformedBlockRequest) (response *etlv1.SubmitTransformedBlockResponse, returnedErr error) {
	started := time.Now()
	event := logevent.Event{Service: "loader-service", Operation: "accept_inbox", Outcome: "accepted"}
	defer func() {
		if returnedErr != nil {
			if event.ErrorCode == "" {
				event.ErrorCode = transportErrorCode(returnedErr)
			}
			event.Outcome = logevent.FailureOutcome(event.ErrorCode)
		}
		logevent.Write(ctx, r.logger, event, started)
	}()
	if err := ctx.Err(); err != nil {
		return nil, acceptanceError(err)
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := r.limits.CheckMessage(req); err != nil {
		return nil, err
	}
	if err := grpccontract.ValidateMetadata(req.Metadata, "transformed-block", r.chainID); err != nil {
		return nil, err
	}
	event.EventID = req.Metadata.EventId
	event.SourceEventID = req.SourceEventId
	if err := r.limits.CheckPayload(req.PayloadJson); err != nil {
		return nil, err
	}
	if !grpccontract.JSONObject(req.PayloadJson) {
		return nil, status.Error(codes.InvalidArgument, "transformed payload must be a JSON object")
	}
	if req.SourceEventId != grpccontract.EventID("raw-block", req.Metadata.Block) {
		return nil, status.Error(codes.InvalidArgument, "source event ID does not match block identity")
	}
	if !grpccontract.ValidTimestamp(req.TransformedAt) || req.TransformedAt.AsTime().Before(req.Metadata.ExtractedAt.AsTime()) {
		return nil, status.Error(codes.InvalidArgument, "invalid transformation time")
	}
	// Validation may scan a large payload. Recheck cancellation before I/O.
	if err := ctx.Err(); err != nil {
		return nil, acceptanceError(err)
	}
	if r.acceptor == nil {
		return nil, acceptanceError(inbound.ErrNotImplemented)
	}
	m := req.Metadata
	request := inbound.AcceptTransformedBlockRequest{
		EventID: m.EventId, EventVersion: m.EventVersion, PayloadVersion: m.PayloadVersion,
		Block: inbound.BlockIdentity{ChainID: m.Block.ChainId, Number: m.Block.GetNumber(),
			Hash: [32]byte(m.Block.BlockHash), ParentHash: [32]byte(m.Block.ParentHash)},
		Provider: m.Provider, ExtractedAt: m.ExtractedAt.AsTime(),
		PayloadJSON: req.PayloadJson, SourceEventID: req.SourceEventId, TransformedAt: req.TransformedAt.AsTime(),
	}
	result, err := r.acceptor.AcceptTransformedBlock(ctx, request)
	if err != nil {
		event.ErrorCode = logging.ErrorCode(err)
		return nil, acceptanceError(err)
	}
	// The port must positively attest to a committed inbox record. Do not turn a
	// zero result, foreign identity, or unknown outcome into a successful ACK.
	var outcome etlv1.AcceptanceStatus
	switch result.Acceptance {
	case inbound.DurablyAccepted:
		outcome = etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_DURABLY_ACCEPTED
	case inbound.AlreadyAccepted:
		event.Outcome = "already_accepted"
		outcome = etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED
	default:
		return nil, status.Error(codes.Internal, "acceptance did not confirm a durable inbox record")
	}
	if result.EventID != m.EventId {
		return nil, status.Error(codes.Internal, "acceptance returned an inconsistent event identity")
	}
	// A reported commit remains successful even if cancellation arrives now.
	return &etlv1.SubmitTransformedBlockResponse{Acceptance: &etlv1.DurableAcceptance{EventId: result.EventID, Status: outcome}}, nil
}

func acceptanceError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "acceptance canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "acceptance deadline exceeded")
	case errors.Is(err, inbound.ErrInvalidEvent):
		return status.Error(codes.InvalidArgument, "invalid event")
	case errors.Is(err, inbound.ErrUnsupportedVersion):
		return status.Error(codes.FailedPrecondition, "unsupported event version")
	case errors.Is(err, inbound.ErrConflict):
		return status.Error(codes.AlreadyExists, "event identity conflicts with accepted content")
	case errors.Is(err, inbound.ErrUnavailable):
		return status.Error(codes.Unavailable, "durable inbox unavailable")
	case errors.Is(err, inbound.ErrNotImplemented):
		return status.Error(codes.Unimplemented, "durable inbox acceptance is not implemented")
	default:
		return status.Error(codes.Internal, "durable inbox acceptance failed")
	}
}

func transportErrorCode(err error) string {
	switch status.Code(err) {
	case codes.Canceled:
		return "canceled"
	case codes.DeadlineExceeded:
		return "deadline_exceeded"
	case codes.InvalidArgument:
		return "invalid"
	case codes.FailedPrecondition:
		return "unsupported"
	case codes.ResourceExhausted:
		return "oversize"
	case codes.AlreadyExists:
		return "conflict"
	case codes.Unavailable:
		return "unavailable"
	default:
		return "operation_failed"
	}
}
