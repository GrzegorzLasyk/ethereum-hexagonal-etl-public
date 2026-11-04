// dispatch.go defines contracts for durable delivery coordination and retry policy in the
// application inbound boundary.

package inbound

import "context"

// DispatchResult reports the claimed event/attempt and outcome so workers can log progress without
// payloads.
type DispatchResult struct {
	SourceEventID string
	ErrorCode     string
	Found         bool
	EventID       string
	Attempt       int
	Outcome       string
}

// Dispatcher is the worker-facing entry port for one durable delivery attempt.
type Dispatcher interface {
	// DispatchOne performs at most one durable publication attempt and returns progress for logging
	// and polling.
	DispatchOne(context.Context) (DispatchResult, error)
}
