package dbreadiness

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"testing"
	"time"
)

type failingDB struct{ wait bool }

func (d failingDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	if d.wait {
		<-ctx.Done()
	}
	return nil, errors.New("postgres://secret@host private SQL detail")
}
func TestFailureRedactionAndDeadline(t *testing.T) {
	for _, wait := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		start := time.Now()
		err := Check(ctx, failingDB{wait}, Policy{})
		cancel()
		if err != ErrNotReady || time.Since(start) > time.Second {
			t.Fatal("unsafe or unbounded readiness", err)
		}
	}
}

type successfulDB struct {
	pgx.Tx
	seen bool
}

func (d *successfulDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	d.seen = o.AccessMode == pgx.ReadOnly && o.IsoLevel == pgx.RepeatableRead
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("missing timeout")
	}
	return nil, errors.New("stop before SQL")
}
func TestUsesReadOnlySnapshot(t *testing.T) {
	d := &successfulDB{}
	_ = Check(t.Context(), d, Policy{})
	if !d.seen {
		t.Fatal("write transaction or inconsistent snapshot")
	}
}
