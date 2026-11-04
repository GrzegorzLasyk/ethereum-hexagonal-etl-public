// batch_test.go exercises bounded pgx INSERT grouping and batch-result cleanup inside a block
// transaction in the outbound adapter layer.

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// recordedStatement retains one parameterized SQL operation and its arguments for field-
// equivalence assertions.
type recordedStatement struct {
	sql  string
	args []any
}

// recordingExecutor captures direct statements and batches and injects result/close failures
// without PostgreSQL.
type recordingExecutor struct {
	statements            []recordedStatement
	batches               []int
	direct, closes, reads int
	failureAt             int
	failure, closeError   error
}

// Exec records a test SQL execution or returns the configured statement failure for batch-
// order/rollback assertions.
func (r *recordingExecutor) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.direct++
	r.statements = append(r.statements, recordedStatement{sql, args})
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

// SendBatch exposes queued pgx statements through controlled result objects so batching and late
// failures are observable.
func (r *recordingExecutor) SendBatch(_ context.Context, b *pgx.Batch) pgx.BatchResults {
	r.batches = append(r.batches, b.Len())
	for _, q := range b.QueuedQueries {
		r.statements = append(r.statements, recordedStatement{q.SQL, q.Arguments})
	}
	return &recordingResults{r: r}
}

// recordingResults implements batch result consumption against the recording executor.
type recordingResults struct {
	pgx.BatchResults
	r *recordingExecutor
}

// Exec records a test SQL execution or returns the configured statement failure for batch-
// order/rollback assertions.
func (r *recordingResults) Exec() (pgconn.CommandTag, error) {
	r.r.reads++
	if r.r.reads == r.r.failureAt {
		return pgconn.CommandTag{}, r.r.failure
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

// Close implements test-owned cleanup and records/releases the controlled resource when
// applicable.
func (r *recordingResults) Close() error { r.r.closes++; return r.r.closeError }

// Synthetic workload with the same row counts as authentic block 12965000.
// It does not claim to be Mainnet data; private captures are never dependencies.
func largeBatchPayload(t *testing.T, count int) []byte {
	t.Helper()
	raw, err := os.ReadFile("../jsondocument/testdata/transformed_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var d map[string]any
	if err = json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	clone := func(v any) map[string]any {
		b, _ := json.Marshal(v)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return m
	}
	txTemplate := d["transactions"].([]any)[0]
	receiptTemplate := d["receipts"].([]any)[0]
	logTemplate := d["logs"].([]any)[0]
	txs, receipts, logs := []any{}, []any{}, []any{}
	for i := 0; i < count; i++ {
		hash := fmt.Sprintf("0x%064x", i+1)
		tx, r := clone(txTemplate), clone(receiptTemplate)
		tx["hash"], tx["transaction_index"] = hash, fmt.Sprint(i)
		r["transaction_hash"], r["transaction_index"], r["cumulative_gas_used"] = hash, fmt.Sprint(i), fmt.Sprint((i+1)*25000)
		txs = append(txs, tx)
		receipts = append(receipts, r)
		n := 2
		if i < 130 {
			n++
		}
		for range n {
			l := clone(logTemplate)
			l["transaction_hash"], l["transaction_index"], l["log_index"] = hash, fmt.Sprint(i), fmt.Sprint(len(logs))
			logs = append(logs, l)
		}
	}
	d["transactions"], d["receipts"], d["logs"] = txs, receipts, logs
	d["block"].(map[string]any)["gas_used"] = fmt.Sprint(count * 25000)
	raw, err = json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// writeTestSnapshot runs the real normalized INSERT mappings against a recording executor for
// direct/batched equivalence checks.
func writeTestSnapshot(t *testing.T, executor recordExecutor, s domain.Snapshot) {
	t.Helper()
	ctx := t.Context()
	if err := insertBlock(ctx, executor, s.ChainID, s.Block, "test-event"); err != nil {
		t.Fatal(err)
	}
	for i, tx := range s.Transactions {
		if err := insertTransaction(ctx, executor, s.ChainID, tx); err != nil {
			t.Fatal(err)
		}
		if err := insertReceipt(ctx, executor, s.ChainID, s.Receipts[i]); err != nil {
			t.Fatal(err)
		}
		for _, l := range s.Receipts[i].Logs {
			if err := insertLog(ctx, executor, s.ChainID, l); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestRecordBatchPreservesAllSQLValuesAndOrder compares direct and batched INSERT arguments/order
// so batching cannot change normalized records.
func TestRecordBatchPreservesAllSQLValuesAndOrder(t *testing.T) {
	for _, count := range []int{0, 1, 259} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			d, err := (jsondocument.Decoder{}).Decode(t.Context(), largeBatchPayload(t, count))
			if err != nil {
				t.Fatal(err)
			}
			direct := &recordingExecutor{}
			writeTestSnapshot(t, direct, d.Snapshot)
			for _, limit := range []int{1, 2, 64, 256} {
				batched := &recordingExecutor{}
				b := &recordBatch{tx: batched, limit: limit}
				writeTestSnapshot(t, b, d.Snapshot)
				if err := b.flush(t.Context()); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(direct.statements, batched.statements) {
					t.Fatal("SQL, field values, null presence or dependency order changed")
				}
				for _, q := range batched.statements {
					if len(q.args) > 28 {
						t.Fatal("unexpected per-statement parameter growth")
					}
				}
				if limit == 1 && batched.direct != len(direct.statements) {
					t.Fatal("reference must use direct Exec")
				}
				if limit > 1 && (len(batched.batches) != (len(direct.statements)+limit-1)/limit || batched.closes != len(batched.batches)) {
					t.Fatal("unexpected batch count", batched.batches)
				}
				if count == 259 && len(batched.statements) != 1167 {
					t.Fatal("wrong synthetic row counts")
				}
			}
		})
	}
}

// TestRecordBatchByteAndParameterBounds checks coalescing estimates, single-row handling and per-
// statement parameter limits without dropping values.
func TestRecordBatchByteAndParameterBounds(t *testing.T) {
	r := &recordingExecutor{}
	b := &recordBatch{tx: r, limit: 64}
	// Each row is below the budget; two together exceed it.
	value := make([]byte, batchByteBudget/3)
	for range 2 {
		if _, err := b.Exec(t.Context(), "INSERT", value); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(r.batches, []int{1}) {
		t.Fatal("byte limit not flushed")
	}
	large := make([]byte, batchByteBudget)
	if _, err := b.Exec(t.Context(), "INSERT", large); err != nil {
		t.Fatal(err)
	}
	if r.direct != 1 || !reflect.DeepEqual(r.batches, []int{1, 1}) {
		t.Fatal("oversized row not isolated")
	}
	if err := b.flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := statementSize("INSERT", make([]any, maxStatementParameters)); err != nil {
		t.Fatal(err)
	}
	if _, err := statementSize("INSERT", make([]any, maxStatementParameters+1)); err == nil {
		t.Fatal("parameter limit ignored")
	}
	if _, err := statementSize("INSERT", []any{struct{}{}}); err == nil {
		t.Fatal("unknown encoding size accepted")
	}
}

// TestRecordBatchErrorsAndCancellation checks result closing and first-error preservation on batch
// failure or canceled work.
func TestRecordBatchErrorsAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"unique", &pgconn.PgError{Code: "23505"}, inbound.ErrConflict},
		{"sql", &pgconn.PgError{Code: "23503"}, inbound.ErrUnavailable},
		{"cancel", context.Canceled, context.Canceled},
		{"deadline", context.DeadlineExceeded, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &recordingExecutor{failureAt: 2, failure: tc.err, closeError: errors.New("later close error")}
			b := &recordBatch{tx: r, limit: 3}
			var err error
			for range 3 {
				_, err = b.Exec(t.Context(), "INSERT", "value")
			}
			if !errors.Is(dbError(err), tc.want) || r.closes != 1 {
				t.Fatal("lost error classification or close", err)
			}
			_, err = b.Exec(t.Context(), "INSERT", "must not execute")
			if !errors.Is(err, tc.err) || len(r.statements) != 3 || r.reads != 2 {
				t.Fatal("continued after failure")
			}
		})
	}
	r := &recordingExecutor{closeError: errors.New("close failed")}
	b := &recordBatch{tx: r, limit: 2}
	_, _ = b.Exec(t.Context(), "INSERT", "x")
	if !errors.Is(b.flush(t.Context()), r.closeError) {
		t.Fatal("close failure suppressed")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r = &recordingExecutor{}
	b = &recordBatch{tx: r, limit: 64}
	_, _ = b.Exec(t.Context(), "INSERT", "x")
	if !errors.Is(b.flush(ctx), context.Canceled) || len(r.statements) != 0 {
		t.Fatal("sent after cancellation")
	}
}

// Exercise the production Complete failure path after a prior successful batch.
// PostgreSQL atomicity itself is verified by the tagged integration companion.
type batchFailureDB struct{ tx *batchFailureTx }

// BeginTx wraps or returns the controlled test transaction so commit/failure boundaries can be
// injected without changing production code.
func (d batchFailureDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return d.tx, nil }

// batchFailureTx records commit/rollback and control statements around a simulated late batch
// failure.
type batchFailureTx struct {
	pgx.Tx
	recordingExecutor
	committed, rolledBack bool
	controlCalls          int
}

// Exec records a test SQL execution or returns the configured statement failure for batch-
// order/rollback assertions.
func (tx *batchFailureTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	tx.controlCalls++
	return pgconn.CommandTag{}, nil
}

// SendBatch exposes queued pgx statements through controlled result objects so batching and late
// failures are observable.
func (tx *batchFailureTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return tx.recordingExecutor.SendBatch(ctx, b)
}

// QueryRow returns the controlled claimed-row state used by the completion fence test.
func (tx *batchFailureTx) QueryRow(context.Context, string, ...any) pgx.Row { return claimedRow{} }

// Rollback records rollback of the simulated transaction after failure for atomicity assertions.
func (tx *batchFailureTx) Rollback(context.Context) error { tx.rolledBack = true; return nil }

// Commit records whether the simulated completion tried to commit after an injected batch failure.
func (tx *batchFailureTx) Commit(context.Context) error { tx.committed = true; return nil }

// claimedRow supplies a valid claimed Inbox row for transaction-boundary unit tests.
type claimedRow struct{}

// Scan populates controlled test row fields for the claimed attempt/state check.
func (claimedRow) Scan(v ...any) error {
	*v[0].(*string) = "processing"
	*v[1].(*int) = 1
	*v[2].(*bool) = true
	return nil
}

// TestCompleteRollsBackAfterEarlierSuccessfulBatch injects a later SQL failure and requires whole-
// block rollback despite earlier successful batch statements.
func TestCompleteRollsBackAfterEarlierSuccessfulBatch(t *testing.T) {
	raw := largeBatchPayload(t, 259)
	d, err := (jsondocument.Decoder{}).Decode(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	r := inbound.AcceptTransformedBlockRequest{
		EventID:       fmt.Sprintf("transformed-block/v1/1/%d/%x", d.Snapshot.Block.Number, d.Snapshot.Block.Hash),
		SourceEventID: d.Provenance.SourceEventID, Provider: d.Provenance.Provider, ExtractedAt: d.Provenance.ExtractedAt, TransformedAt: time.Unix(200, 0),
		EventVersion: 1, PayloadVersion: 1, PayloadJSON: raw,
		Block: inbound.BlockIdentity{ChainID: "1", Number: d.Snapshot.Block.Number, Hash: d.Snapshot.Block.Hash, ParentHash: d.Snapshot.Block.ParentHash},
	}
	tx := &batchFailureTx{recordingExecutor: recordingExecutor{failureAt: 65, failure: &pgconn.PgError{Code: "23505"}}}
	s := &Store{db: batchFailureDB{tx}, chain: "1", batchSize: 64}
	err = s.Complete(t.Context(), outbound.Item{Event: r, Attempt: 1}, d)
	if !errors.Is(err, inbound.ErrConflict) || !tx.rolledBack || tx.committed || tx.controlCalls != 2 || tx.closes != 2 || !reflect.DeepEqual(tx.batches, []int{64, 64}) {
		t.Fatal("failure was not fenced and rolled back", err, tx)
	}
}

// TestRecordBatchTransactionsWithoutLogs checks receipt persistence with no logs and correct
// flushing of sparse block contents.
func TestRecordBatchTransactionsWithoutLogs(t *testing.T) {
	var payload map[string]any
	_ = json.Unmarshal(largeBatchPayload(t, 259), &payload)
	payload["logs"] = []any{}
	raw, _ := json.Marshal(payload)
	d, err := (jsondocument.Decoder{}).Decode(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	r := &recordingExecutor{}
	b := &recordBatch{tx: r, limit: 64}
	writeTestSnapshot(t, b, d.Snapshot)
	if err := b.flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(r.statements) != 519 || len(r.batches) != 9 {
		t.Fatal("missing receipts or invented logs")
	}
}
