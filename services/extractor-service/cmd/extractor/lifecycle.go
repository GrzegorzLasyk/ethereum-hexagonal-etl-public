package main

import "context"

// Composition lifecycle: a finite producer finishing must not cancel its sender.
// The completion use case remains bounded by both drain and process deadlines.
func runAcquisition(ctx context.Context, finite bool, acquire func(context.Context) error, dispatch func(context.Context), complete func(context.Context) error) error {
	work, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); dispatch(work) }()
	defer func() { cancel(); <-done }()
	if err := acquire(work); err != nil {
		return err
	}
	if work.Err() != nil {
		if !finite {
			return nil
		}
		return work.Err()
	}
	return complete(work)
}
