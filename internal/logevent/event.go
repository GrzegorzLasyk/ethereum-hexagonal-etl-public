// event.go implements the fixed structured-log vocabulary and safe correlated error reporting in
// the shared technical support layer, outside service domains.

// Package logevent formats adapter logs. It is not a domain model or an
// application dependency. Only explicitly selected metadata crosses this boundary.
package logevent

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// identity recognizes safe stable event identities for correlation fields.
var identity = regexp.MustCompile(`^(raw-block|transformed-block)/v1/(0|[1-9][0-9]*)/(0|[1-9][0-9]*)/([0-9a-f]{64})$`)

// decimal recognizes canonical nonnegative decimal values before log correlation fields are
// accepted.
var decimal = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

// Event contains no payload, provider, connection configuration or error text.
// Operation, Outcome and ErrorCode must be fixed program-defined vocabulary.
type Event struct {
	Service, Operation, EventID, SourceEventID string
	ChainID                                    string
	BlockNumber                                *uint64
	Attempt                                    int
	Duration                                   *time.Duration
	Outcome, ErrorCode                         string
}

// Write emits the same keys for every operation. Unknown identities/attempts
// are null, never fabricated as block zero or a successful first attempt.
// The existing versioned event ID is the source of chain/number/hash correlation.
func Write(ctx context.Context, logger *slog.Logger, e Event, started time.Time) {
	if logger == nil {
		return
	}
	var eventID, sourceID, chain, number, hash, attempt any
	if decimal.MatchString(e.ChainID) {
		chain = e.ChainID
	}
	if e.BlockNumber != nil {
		number = *e.BlockNumber
	}
	parts := identity.FindStringSubmatch(e.EventID)
	if len(parts) != 0 {
		if n, err := strconv.ParseUint(parts[3], 10, 64); err == nil && parts[4] != strings.Repeat("0", 64) {
			eventID, chain, number, hash = e.EventID, parts[2], n, "0x"+parts[4]
			// Do not invent a source link or accept a different block's source ID.
			if e.SourceEventID == "raw-block/v1/"+strings.Join(parts[2:], "/") {
				sourceID = e.SourceEventID
			}
		}
	}
	if e.Attempt > 0 {
		attempt = e.Attempt
	}
	level := slog.LevelInfo
	if e.Outcome == "retry" || e.Outcome == "backpressure" {
		level = slog.LevelWarn
	}
	if e.ErrorCode != "" && e.Outcome != "retry" {
		level = slog.LevelError
	}
	duration := time.Since(started)
	if e.Duration != nil {
		duration = *e.Duration
	}
	logger.LogAttrs(ctx, level, "operation",
		slog.String("service", e.Service), slog.String("operation", e.Operation), slog.Any("event_id", eventID),
		slog.Any("source_event_id", sourceID), slog.Any("chain_id", chain),
		slog.Any("block_number", number), slog.Any("block_hash", hash),
		slog.Any("attempt", attempt), slog.String("outcome", e.Outcome),
		slog.String("error_code", e.ErrorCode), slog.Int64("duration_ms", duration.Milliseconds()))
}

// ErrorCode never formats an error, including unknown third-party errors.
func ErrorCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "operation_failed"
	}
}

// FailureOutcome does not claim rollback or permanent failure for interrupted
// operations; lease recovery or an idempotent retry determines durable state.
func FailureOutcome(code string) string {
	if code == "commit_unknown" {
		return "unknown"
	}
	return "interrupted"
}
