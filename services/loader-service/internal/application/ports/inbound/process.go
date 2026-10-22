package inbound

import "context"

// ProcessResult exposes safe progress without logging source payloads.
type ProcessResult struct {
	Found   bool
	EventID string
	Outcome string
	Attempt int
}
type Processor interface {
	ProcessOne(context.Context) (ProcessResult, error)
}
