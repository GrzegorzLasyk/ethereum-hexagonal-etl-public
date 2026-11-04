// batch.go implements bounded pgx INSERT grouping and batch-result cleanup inside a block
// transaction in the outbound adapter layer.

package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// DefaultBatchSize sets the conservative number of INSERT statements per Loader pgx batch.
	DefaultBatchSize = 64
	// MaxBatchSize caps queued statement count independently of the batch byte estimate.
	MaxBatchSize = 256
	// This bounds coalescing, not the size of one otherwise valid source row.
	batchByteBudget = 1 << 20
	// maxStatementParameters bounds parameters in one PostgreSQL statement, not the sum of separate
	// batched statements.
	maxStatementParameters = 65535
)

// recordExecutor is the minimal parameterized INSERT dependency, implemented by direct and bounded
// batch executors.
type recordExecutor interface {
	// Exec executes or queues a parameterized statement inside the caller-owned transaction.
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// batchExecutor adds pgx batch submission to the record executor used inside a single completion
// transaction.
type batchExecutor interface {
	recordExecutor
	// SendBatch submits queued statements inside the same transaction; results must be consumed and
	// closed before further SQL.
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

// recordBatch belongs to one Complete transaction and is never shared between
// workers. It preserves the original parameterized SQL and dependency order.
type recordBatch struct {
	tx    batchExecutor
	limit int
	batch pgx.Batch
	bytes int
	err   error
}

// Exec queues an INSERT or executes an oversized/direct-mode statement, flushing at count/byte
// bounds; it does not commit the block transaction.
func (b *recordBatch) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if b.err != nil {
		return pgconn.CommandTag{}, b.err
	}
	if err := ctx.Err(); err != nil {
		b.err = err
		return pgconn.CommandTag{}, err
	}
	size, err := statementSize(sql, args)
	if err != nil {
		b.err = err
		return pgconn.CommandTag{}, err
	}
	if b.limit == 1 || size > batchByteBudget {
		if err = b.flush(ctx); err != nil {
			return pgconn.CommandTag{}, err
		}
		tag, err := b.tx.Exec(ctx, sql, args...)
		b.err = err
		return tag, err
	}
	if b.batch.Len() > 0 && b.bytes+size > batchByteBudget {
		if err = b.flush(ctx); err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	b.batch.Queue(sql, args...)
	b.bytes += size
	if b.batch.Len() == b.limit {
		err = b.flush(ctx)
	}
	// Callers only inspect errors. No rows-affected claim before execution.
	return pgconn.CommandTag{}, err
}

// flush consumes and closes queued pgx results before further SQL, retaining the first failure so
// the enclosing block can roll back.
func (b *recordBatch) flush(ctx context.Context) error {
	if b.err != nil {
		return b.err
	}
	if err := ctx.Err(); err != nil {
		b.err = err
		return err
	}
	if b.batch.Len() == 0 {
		return nil
	}
	results := b.tx.SendBatch(ctx, &b.batch)
	for range b.batch.Len() {
		if _, err := results.Exec(); err != nil {
			b.err = err
			break
		}
	}
	// Always close before any other transaction operation, including rollback.
	// Preserve the first SQL error for existing classification (e.g. 23505).
	if err := results.Close(); b.err == nil {
		b.err = err
	}
	b.batch = pgx.Batch{}
	b.bytes = 0
	return b.err
}

// Estimate framing, SQL and parameter bytes conservatively for the fixed record
// mappings. This is not a wire-size or heap-size measurement. Oversized individual
// rows are sent alone, without splitting, truncating or copying their values.
func statementSize(sql string, args []any) (int, error) {
	if len(args) > maxStatementParameters {
		return 0, errors.New("too many record parameters")
	}
	size := len(sql) + 128
	for _, arg := range args {
		n, err := argumentSize(arg)
		if err != nil {
			return 0, err
		}
		size += 16 + n
	}
	return size, nil
}

// argumentSize estimates supported pgx argument storage for the fixed record mappings; unsupported
// parameter types fail explicitly.
func argumentSize(arg any) (int, error) {
	switch v := arg.(type) {
	case nil, bool:
		return 1, nil
	case string:
		return 2 * len(v), nil
	case []byte:
		return 2 * len(v), nil
	case []string:
		n := 32
		for _, s := range v {
			n += 16 + 2*len(s)
		}
		return n, nil
	case [][]byte:
		n := 32
		for _, s := range v {
			n += 16 + 2*len(s)
		}
		return n, nil
	default:
		return 0, errors.New("unsupported record parameter type")
	}
}
