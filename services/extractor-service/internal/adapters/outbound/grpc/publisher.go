// publisher.go implements one-call gRPC publication and durable-ACK/error classification in the
// outbound adapter layer.

// Package grpc implements the outbound publication port using the v1 contract.
package grpc

import (
	"context"
	"errors"
	"fmt"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
	transport "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Publisher owns the lazy downstream gRPC connection and validates event/ACK identity within the
// configured transport limits.
type Publisher struct {
	client etlv1.TransformerServiceClient
	conn   *transport.ClientConn
	chain  string
	limits grpccontract.Limits
}

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ outbound.Publisher = (*Publisher)(nil)

// New opens a lazy connection. Starting a sender never requires its recipient
// to be online. The caller closes the connection after joining dispatch workers.
func New(address, chain string, limits grpccontract.Limits) (*Publisher, error) {
	if !grpccontract.LocalAddress(address) || !grpccontract.ValidChainID(chain) {
		return nil, fmt.Errorf("publisher requires loopback target and explicit chain")
	}
	options, err := limits.ClientOptions()
	if err != nil {
		return nil, err
	}
	options = append(options, transport.WithTransportCredentials(insecure.NewCredentials()), transport.WithDisableRetry())
	conn, err := transport.NewClient("passthrough:///"+address, options...)
	if err != nil {
		return nil, fmt.Errorf("cannot create publication client")
	}
	return &Publisher{etlv1.NewTransformerServiceClient(conn), conn, chain, limits}, nil
}

// Close closes the owned lazy gRPC connection after dispatch workers have stopped.
func (p *Publisher) Close() error { return p.conn.Close() }

// Publish validates metadata/size, makes one caller-bounded gRPC call and verifies its durable
// ACK; retries belong to the Outbox use case.
func (p *Publisher) Publish(ctx context.Context, e outbound.Publication) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	metadata := &etlv1.EventMetadata{EventId: e.EventID, EventVersion: 1, PayloadVersion: 1, Block: &etlv1.BlockIdentity{ChainId: e.ChainID, Number: proto.Uint64(e.Number), BlockHash: e.Hash[:], ParentHash: e.ParentHash[:]}, Provider: e.Provider, ExtractedAt: timestamppb.New(e.ExtractedAt)}
	if err := grpccontract.ValidateMetadata(metadata, "raw-block", p.chain); err != nil {
		return outbound.ErrPublishInvalid
	}
	if err := p.limits.CheckPayload(e.RawBlock, e.RawReceipts...); err != nil {
		return outbound.ErrPublishOversize
	}
	if !grpccontract.JSONObject(e.RawBlock) {
		return outbound.ErrPublishInvalid
	}
	for _, r := range e.RawReceipts {
		if !grpccontract.JSONObject(r) {
			return outbound.ErrPublishInvalid
		}
	}
	request := &etlv1.SubmitRawBlockRequest{Metadata: metadata, Payload: &etlv1.RawBlockPayload{BlockJson: e.RawBlock, Receipts: &etlv1.RawReceipts{Json: e.RawReceipts}}}
	if err := p.limits.CheckMessage(request); err != nil {
		return outbound.ErrPublishOversize
	}
	response, err := p.client.SubmitRawBlock(ctx, request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if grpccontract.IsOverloaded(err) {
			return errors.Join(outbound.ErrPublishOverloaded, outbound.ErrPublishUnavailable)
		}
		switch status.Code(err) {
		case codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied, codes.Unauthenticated:
			return outbound.ErrPublishInvalid
		case codes.AlreadyExists:
			return outbound.ErrPublishConflict
		case codes.ResourceExhausted:
			return outbound.ErrPublishOversize
		default:
			return outbound.ErrPublishUnavailable
		}
	}
	if response == nil || !grpccontract.DurableACK(response.Acceptance, e.EventID) {
		return outbound.ErrPublishACK
	}
	return nil
}
