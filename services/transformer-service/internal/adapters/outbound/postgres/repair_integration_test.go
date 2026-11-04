//go:build postgresintegration

// repair_integration_test.go exercises audited failed-work allowance and stale-claim fencing in
// the outbound adapter layer.

package postgres

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/ethereumjson"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/usecases"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testRepair checks audited finite retry allowance, repeat operation identity and stale-claim
// fencing in PostgreSQL.
func testRepair(t *testing.T, runtime, owner *pgxpool.Pool) {
	for _, queue := range []string{"inbox", "outbox"} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_commit_after_%t", queue, after), func(t *testing.T) {
				ctx := t.Context()
				if _, err := owner.Exec(ctx, "TRUNCATE transformer.inbox_events,transformer.recovery_operations CASCADE"); err != nil {
					t.Fatal(err)
				}
				s, err := New(runtime, "1", time.Minute, 3)
				if err != nil {
					t.Fatal(err)
				}
				event := testEvent(t)
				if _, err := s.Accept(ctx, event); err != nil {
					t.Fatal(err)
				}
				encoder, _ := jsondocument.New(64 << 20)
				process, err := usecases.NewProcess(s, ethereumjson.Decoder{}, encoder, time.Millisecond, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				id := event.EventID
				if queue == "outbox" {
					if _, err := process.ProcessOne(ctx); err != nil {
						t.Fatal(err)
					}
					id = strings.Replace(id, "raw-block/", "transformed-block/", 1)
				}
				table := "transformer." + queue + "_events"
				if _, err := runtime.Exec(ctx, "UPDATE "+table+" SET status='failed',attempts=7,max_attempts=7 WHERE event_id=$1", id); err != nil {
					t.Fatal(err)
				}
				r := outbound.RepairRequest{Queue: queue, EventID: id, OperationID: "audited-repair", Allowance: 3}
				faulty := *s
				faulty.db = faultDB{s.db, after}
				if err := faulty.Repair(ctx, r); !errors.Is(err, outbound.ErrCommitUnknown) {
					t.Fatal("ambiguous repair", err)
				}
				var audit int
				if err := runtime.QueryRow(ctx, "SELECT count(*) FROM transformer.recovery_operations").Scan(&audit); err != nil || audit != map[bool]int{false: 0, true: 1}[after] {
					t.Fatal("atomic recovery audit", err, audit)
				}
				var wg sync.WaitGroup
				errs := make(chan error, 8)
				for i := 0; i < 8; i++ {
					wg.Go(func() { errs <- usecases.Repair(ctx, "1", s, r) })
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						t.Fatal("idempotent repair retry", err)
					}
				}
				var state string
				var attempt, maximum int
				if err := runtime.QueryRow(ctx, "SELECT status,attempts,max_attempts FROM "+table+" WHERE event_id=$1", id).Scan(&state, &attempt, &maximum); err != nil || state != "pending" || attempt != 8 || maximum != 11 {
					t.Fatal("fencing/allowance", state, attempt, maximum, err)
				}
				changed := r
				changed.Allowance++
				if s.Repair(ctx, changed) == nil {
					t.Fatal("operation identity reused with changed content")
				}
				changed = r
				changed.OperationID = "different"
				if s.Repair(ctx, changed) == nil {
					t.Fatal("pending event was reset")
				}
				if _, err := runtime.Exec(ctx, "DELETE FROM transformer.recovery_operations"); err == nil {
					t.Fatal("runtime can erase audit")
				}
				if _, err := runtime.Exec(ctx, "UPDATE transformer.recovery_operations SET allowance=999"); err == nil {
					t.Fatal("runtime can rewrite audit")
				}
				if queue == "inbox" {
					if err := s.Fail(ctx, outbound.Item{Event: event, Attempt: 7}, outbound.Failure{Code: "transient"}); !errors.Is(err, outbound.ErrLeaseLost) {
						t.Fatal("stale failure accepted", err)
					}
					result, err := process.ProcessOne(ctx)
					if err != nil || result.Outcome != "completed" {
						t.Fatal("repaired input did not complete", err, result.Outcome)
					}
					changed.OperationID = "reset-completed"
					if s.Repair(ctx, changed) == nil {
						t.Fatal("completed event reset")
					}
				}
				if queue == "outbox" {
					q, err := NewOutbox(runtime, "1", time.Minute, 3)
					if err != nil {
						t.Fatal(err)
					}
					current, err := q.ClaimDelivery(ctx)
					if err != nil || current == nil || current.Attempt != 9 || current.MaxAttempts != 11 {
						t.Fatal("repair claim", err, current)
					}
					stale := *current
					stale.Attempt = 7
					if err := q.Published(ctx, stale); !errors.Is(err, outbound.ErrDeliveryLeaseLost) {
						t.Fatal("stale publication accepted", err)
					}
					if err := q.Published(ctx, *current); err != nil {
						t.Fatal(err)
					}
					changed.OperationID = "reset-delivered"
					if s.Repair(ctx, changed) == nil {
						t.Fatal("delivered event reset")
					}
				}
				// An ambiguous CLI retry after processing must not requeue again.
				if err := s.Repair(ctx, r); err != nil {
					t.Fatal("completed repair retry", err)
				}
				if duplicate, err := s.Accept(ctx, event); err != nil || !duplicate {
					t.Fatal("original immutable input/dedup changed", err)
				}
				if err := runtime.QueryRow(ctx, "SELECT count(*) FROM transformer.recovery_operations").Scan(&audit); err != nil || audit != 1 {
					t.Fatal("duplicate repair audit", err, audit)
				}
			})
		}
	}
}
