package inbound

import "context"

type IndexResult struct {
	Processed                            int
	Done, Waiting, Rewound, Backpressure bool
}
type Indexer interface {
	Cycle(context.Context) (IndexResult, error)
}
