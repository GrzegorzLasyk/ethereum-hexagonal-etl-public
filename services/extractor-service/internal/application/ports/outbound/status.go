// status.go defines contracts for owning-schema queue/progress snapshots for operational
// visibility in the application outbound boundary.

package outbound

import "context"

// QueueStatus is a read-only view of this service's retained work.
// Pending includes delayed retries; Failed remains part of unfinished backlog.
type QueueStatus struct {
	Pending, InFlight, Failed, Done int64
	OldestAgeSeconds                float64
}

// Status groups owning-schema queue and progress observations for operational exposition, not
// cross-service completion.
type Status struct {
	Outbox     QueueStatus
	Checkpoint *uint64
	Revision   int64
}

// StatusReader must return an error, never an empty successful view, on failure.
type StatusReader interface {
	// ReadStatus returns one owning-schema operational snapshot or an error; failure must not be
	// exposed as healthy zeros.
	ReadStatus(context.Context) (Status, error)
}
