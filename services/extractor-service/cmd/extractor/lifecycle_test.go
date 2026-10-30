package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestFiniteAcquisitionKeepsDispatchUntilCompletion(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "delivered", true: "timeout"}[fail], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := make(chan struct{})
				producerDone := make(chan struct{})
				delivered := make(chan struct{})
				joined := false
				err := runAcquisition(t.Context(), true, func(context.Context) error { <-started; close(producerDone); return nil }, func(ctx context.Context) {
					close(started)
					<-producerDone
					if !fail {
						close(delivered)
					}
					<-ctx.Done()
					joined = true
				}, func(ctx context.Context) error {
					if ctx.Err() != nil {
						t.Fatal("dispatcher canceled on finite acquisition completion")
					}
					ctx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					select {
					case <-delivered:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				if !joined || fail && !errors.Is(err, context.DeadlineExceeded) || !fail && err != nil {
					t.Fatal("incorrect lifecycle", err)
				}
			})
		})
	}
}
func TestAcquisitionFailureDoesNotClaimDrainSuccess(t *testing.T) {
	failure := errors.New("acquisition failed")
	err := runAcquisition(t.Context(), true, func(context.Context) error { return failure }, func(ctx context.Context) { <-ctx.Done() }, func(context.Context) error { t.Fatal("drain after failed acquisition"); return nil })
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
}

func TestFollowHeadGracefulCancellation(t *testing.T) {
	for _, finite := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := runAcquisition(ctx, finite, func(context.Context) error { return nil }, func(ctx context.Context) { <-ctx.Done() }, func(context.Context) error { t.Fatal("completion after cancellation"); return nil })
		if !finite && err != nil || finite && !errors.Is(err, context.Canceled) {
			t.Fatal("incorrect cancellation semantics", finite, err)
		}
	}
}
