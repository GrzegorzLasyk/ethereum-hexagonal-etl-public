// process.go defines contracts for Inbox processing, validation and bounded failure orchestration
// in the application inbound boundary.

package inbound

import "context"

// ProcessResult exposes safe progress without logging source payloads.
type ProcessResult struct {
	SourceEventID string
	ErrorCode     string
	Found         bool
	EventID       string
	Outcome       string
	Attempt       int
}

// Processor is the worker-facing entry port for claiming and processing at most one Inbox item.
type Processor interface {
	// ProcessOne processes at most one claimed Inbox item and preserves its identity in
	// success/failure progress.
	ProcessOne(context.Context) (ProcessResult, error)
}
