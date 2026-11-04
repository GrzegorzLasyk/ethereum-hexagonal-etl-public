// client.go validates trusted loopback peer addresses and matching durable ACK envelopes in the
// shared gRPC technical adapter; it does not open connections or implement delivery retries.

package grpccontract

import (
	"net"
	"strconv"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
)

// LocalAddress preserves the approved unauthenticated loopback-only boundary.
func LocalAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	n, e := strconv.Atoi(port)
	return err == nil && e == nil && n > 0 && n <= 65535 && net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()
}

// DurableACK accepts only a matching event ID with an explicit durable acceptance outcome; nil or
// ambiguous ACKs fail.
func DurableACK(ack *etlv1.DurableAcceptance, eventID string) bool {
	return ack != nil && ack.EventId == eventID && (ack.Status == etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_DURABLY_ACCEPTED || ack.Status == etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED)
}
