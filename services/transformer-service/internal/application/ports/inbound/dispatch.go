package inbound

import "context"

type DispatchResult struct {
	SourceEventID string
	ErrorCode     string
	Found         bool
	EventID       string
	Attempt       int
	Outcome       string
}
type Dispatcher interface {
	DispatchOne(context.Context) (DispatchResult, error)
}
