// check_test.go exercises bounded read-only schema/migration/privilege checks for readiness in the
// shared technical support layer, outside service domains.

package dbreadiness

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"testing"
	"time"
)

// failingDB injects readiness failure or context waiting without opening a database.
type failingDB struct{ wait bool }

// BeginTx returns the controlled readiness transaction failure/cancellation used to test safe not-
// ready responses.
func (d failingDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	if d.wait {
		<-ctx.Done()
	}
	return nil, errors.New("postgres://secret@host private SQL detail")
}

// TestFailureRedactionAndDeadline checks readiness failure and timeout responses disclose no
// database/credential details.
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

// successfulDB records readiness transaction options while embedding the controlled transaction
// implementation.
type successfulDB struct {
	pgx.Tx
	seen bool
}

// BeginTx captures read-only transaction options for readiness assertions without opening
// PostgreSQL.
func (d *successfulDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	d.seen = o.AccessMode == pgx.ReadOnly && o.IsoLevel == pgx.RepeatableRead
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("missing timeout")
	}
	return nil, errors.New("stop before SQL")
}

// TestUsesReadOnlySnapshot checks readiness queries run inside a read-only snapshot instead of
// mutating the owning database.
func TestUsesReadOnlySnapshot(t *testing.T) {
	d := &successfulDB{}
	_ = Check(t.Context(), d, Policy{})
	if !d.seen {
		t.Fatal("write transaction or inconsistent snapshot")
	}
}
