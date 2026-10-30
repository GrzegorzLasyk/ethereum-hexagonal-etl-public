package outbound

import "context"

// QueueStatus is a read-only view of this service's retained work.
// Pending includes delayed retries; Failed remains part of unfinished backlog.
type QueueStatus struct {
	Pending, InFlight, Failed, Done int64
	OldestAgeSeconds                float64
}
type Status struct {
	AdmissionUsed, AdmissionLimit int64
	Inbox                         QueueStatus
	Outbox                        QueueStatus
}

// StatusReader must return an error, never an empty successful view, on failure.
type StatusReader interface {
	ReadStatus(context.Context) (Status, error)
}
