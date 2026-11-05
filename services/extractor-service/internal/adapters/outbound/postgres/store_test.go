// store_test.go exercises durable acceptance, transactional state changes and safe database error
// mapping in the outbound adapter layer.

package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
	"github.com/jackc/pgx/v5/pgconn"
)

// snapshot builds a complete deterministic extraction for boundary tests; its hashes/provenance
// are synthetic.
func snapshot(chain domain.ChainID, number uint64, hash, parent byte) domain.Extraction {
	id := domain.BlockID{ChainID: chain, Number: number, Hash: domain.Hash{hash}}
	tx := domain.Transaction{Block: id, Hash: domain.Hash{99}, Index: 0}
	return domain.Extraction{
		Header:       domain.Header{ID: id, ParentHash: domain.Hash{parent}, Timestamp: 123},
		Transactions: []domain.Transaction{tx},
		Receipts:     []domain.Receipt{{Block: id, TransactionHash: tx.Hash, Logs: []domain.Log{{Block: id, TransactionHash: tx.Hash, Address: domain.Address{1}, Topics: []domain.Hash{{2}}, Data: []byte{0, 1, 255}}}}},
		RawBlock:     []byte("{ \"unknown\": 12345678901234567890 }\n"),
		RawReceipts:  [][]byte{[]byte("{ \"logs\": [] }\n")}, Provider: "fixture; ' parameterized",
		ExtractedAt: time.Date(2026, 10, 21, 10, 11, 12, 123456789, time.UTC),
	}
}

// TestSnapshotRoundTrip compares raw bytes, metadata and provenance across the storage encoding
// boundary.
func TestSnapshotRoundTrip(t *testing.T) {
	e := snapshot("1", 42, 2, 1)
	metadata, when, err := encodeMetadata(e)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeSnapshot(metadata, e.RawBlock, e.RawReceipts, e.Provider, when)
	if err != nil || !reflect.DeepEqual(got, e) {
		t.Fatalf("round trip error=%v", err)
	}
	for _, test := range []struct {
		name      string
		metadata  []byte
		timestamp string
	}{
		{"malformed", []byte("{"), when}, {"unknown version", []byte(`{"version":2}`), when},
		{"incomplete", []byte(`{"version":1}`), when}, {"bad timestamp", metadata, "invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeSnapshot(test.metadata, e.RawBlock, e.RawReceipts, e.Provider, test.timestamp); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
}

// TestValidationBeforeDatabaseIO requires invalid extraction input to fail before the storage
// adapter begins database work.
func TestValidationBeforeDatabaseIO(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*outbound.CommitExtraction)
		want   error
	}{
		{"invalid snapshot", func(c *outbound.CommitExtraction) { c.Extraction.RawBlock = nil }, domain.ErrIncomplete},
		{"wrong chain", func(c *outbound.CommitExtraction) { c.Extraction = snapshot("2", 42, 2, 1) }, domain.ErrChainMismatch},
		{"expected chain", func(c *outbound.CommitExtraction) {
			c.ExpectedCheckpoint = &domain.Checkpoint{Block: domain.BlockID{ChainID: "2", Number: 41, Hash: domain.Hash{1}}}
		}, domain.ErrChainMismatch},
		{"invalid expectation", func(c *outbound.CommitExtraction) { c.ExpectedCheckpoint = &domain.Checkpoint{} }, domain.ErrInvalidData},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &Store{chain: "1", start: 42} // nil DB proves invalid input cannot perform I/O.
			c := outbound.CommitExtraction{Extraction: snapshot("1", 42, 2, 1)}
			test.change(&c)
			if _, err := s.Commit(context.Background(), c); !errors.Is(err, test.want) {
				t.Fatalf("want=%v got=%v", test.want, err)
			}
		})
	}
	s := &Store{chain: "1"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Commit(ctx, outbound.CommitExtraction{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Checkpoint(ctx, "1"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, domain.BlockID{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// TestDatabaseErrorsAreSafe ensures storage errors retain useful classification without leaking
// server details or connection credentials.
func TestDatabaseErrorsAreSafe(t *testing.T) {
	err := databaseError(&pgconn.PgError{Code: "23505", Message: "secret payload", Detail: "secret DSN"})
	var dbErr *DatabaseError
	if !errors.As(err, &dbErr) || dbErr.SQLState != "23505" || !errors.Is(err, ErrDatabase) || err.Error() != "extractor database SQLSTATE 23505" {
		t.Fatal(err)
	}
	if databaseError(errors.New("secret connection URL")) != ErrDatabase {
		t.Fatal("transport detail exposed")
	}
	if !errors.Is(databaseError(context.DeadlineExceeded), context.DeadlineExceeded) {
		t.Fatal("lost deadline identity")
	}
}
