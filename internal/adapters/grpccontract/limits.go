// limits.go implements approved gRPC payload/message ceilings and validation in the shared
// technical support layer, outside service domains.

package grpccontract

import (
	"bytes"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// MaxPayloadBytes is the ADR-approved aggregate raw/normalized payload ceiling in bytes.
	MaxPayloadBytes = 64 << 20
	// MaxMessageBytes is the ADR-approved protobuf message ceiling, including envelope and unknown
	// fields.
	MaxMessageBytes = 65 << 20
)

// Limits may be lowered for a deployment. All peers must use compatible limits.
// They bound bytes, not process memory or concurrent in-flight requests.
type Limits struct {
	PayloadBytes int
	MessageBytes int
}

// DefaultLimits returns the approved aggregate-payload and protobuf-message ceilings; deployment
// may lower them.
func DefaultLimits() Limits {
	return Limits{PayloadBytes: MaxPayloadBytes, MessageBytes: MaxMessageBytes}
}

// Validate requires positive payload/message limits, message headroom and the approved upper
// bounds.
func (l Limits) Validate() error {
	if l.PayloadBytes <= 0 || l.PayloadBytes > MaxPayloadBytes || l.MessageBytes <= l.PayloadBytes || l.MessageBytes > MaxMessageBytes {
		return fmt.Errorf("require 0 < payload bytes <= 64 MiB and payload bytes < message bytes <= 65 MiB")
	}
	return nil
}

// ServerOptions bounds both requests received and responses sent.
func (l Limits) ServerOptions() ([]grpc.ServerOption, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return []grpc.ServerOption{grpc.MaxRecvMsgSize(l.MessageBytes), grpc.MaxSendMsgSize(l.MessageBytes)}, nil
}

// ClientOptions supplies matching send AND receive limits for future senders.
// It creates no connection and sends no event. Senders must also check payloads.
func (l Limits) ClientOptions() ([]grpc.DialOption, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(l.MessageBytes), grpc.MaxCallRecvMsgSize(l.MessageBytes))}, nil
}

// CheckMessage covers direct adapter invocation too. proto.Size does not marshal
// or copy the payload. The gRPC receive limit additionally bounds wire decoding.
func (l Limits) CheckMessage(m proto.Message) error {
	if proto.Size(m) > l.MessageBytes {
		return status.Error(codes.ResourceExhausted, "gRPC message exceeds configured byte limit")
	}
	return nil
}

// CheckPayload counts exact source bytes, including whitespace, without joining
// or copying them. Subtraction prevents overflow for many receipt buffers.
func (l Limits) CheckPayload(block []byte, receipts ...[]byte) error {
	remaining := l.PayloadBytes
	if len(block) > remaining {
		return payloadTooLarge()
	}
	remaining -= len(block)
	for _, receipt := range receipts {
		if len(receipt) > remaining {
			return payloadTooLarge()
		}
		remaining -= len(receipt)
	}
	return nil
}

// payloadTooLarge returns the permanent transport size error without including payload bytes.
func payloadTooLarge() error {
	return status.Error(codes.ResourceExhausted, "payload exceeds configured byte limit")
}

// JSONObject validates syntax without decoding to maps or reserializing bytes.
// Ethereum membership, completeness and normalized schema validation are later
// application-stage responsibilities, not business logic in this adapter.
func JSONObject(data []byte) bool {
	b := bytes.TrimSpace(data)
	return len(b) >= 2 && b[0] == '{' && b[len(b)-1] == '}' && json.Valid(b)
}
