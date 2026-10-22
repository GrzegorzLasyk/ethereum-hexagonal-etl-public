package inbound

import "context"

type DispatchResult struct {
	Found   bool
	EventID string
	Attempt int
	Outcome string
}
type Dispatcher interface {
	DispatchOne(context.Context) (DispatchResult, error)
}
