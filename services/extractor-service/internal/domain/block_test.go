package domain_test

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

func validExtraction() domain.Extraction {
	id := domain.BlockID{ChainID: "1", Number: 10, Hash: domain.Hash{10}}
	e := domain.Extraction{
		Header:       domain.Header{ID: id, ParentHash: domain.Hash{9}, Timestamp: 100},
		Transactions: []domain.Transaction{},
		Receipts:     []domain.Receipt{},
		RawBlock:     []byte("opaque full block result"),
		RawReceipts:  [][]byte{},
		Provider:     "fixture-provider",
		ExtractedAt:  time.Unix(1000, 0).UTC(),
	}
	for i := 0; i < 2; i++ {
		tx := domain.Transaction{Block: id, Hash: domain.Hash{byte(i + 20)}, Index: uint64(i)}
		e.Transactions = append(e.Transactions, tx)
		e.Receipts = append(e.Receipts, domain.Receipt{
			Block: id, TransactionHash: tx.Hash, TransactionIndex: tx.Index,
			Logs: []domain.Log{{
				Block: id, TransactionHash: tx.Hash, TransactionIndex: tx.Index, Index: uint64(i),
				Address: domain.Address{1}, Topics: []domain.Hash{{2}}, Data: []byte{3},
			}},
		})
		e.RawReceipts = append(e.RawReceipts, []byte("opaque receipt result"))
	}
	return e
}

func TestChainID(t *testing.T) {
	for _, tt := range []struct {
		value domain.ChainID
		valid bool
	}{
		{"0", true}, {"1", true}, {"11155111", true},
		{"184467440737095516160000", true},
		{"", false}, {"01", false}, {"-1", false}, {"+1", false},
		{"0x1", false}, {" 1", false}, {"1.0", false}, {"١", false},
	} {
		t.Run(string(tt.value), func(t *testing.T) {
			err := tt.value.Validate()
			if (err == nil) != tt.valid {
				t.Fatalf("Validate() = %v, valid = %v", err, tt.valid)
			}
			if !tt.valid && !errors.Is(err, domain.ErrInvalidData) {
				t.Fatalf("lost validation identity: %v", err)
			}
		})
	}
}

func TestExtractionValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*domain.Extraction)
		want   error
	}{
		{"complete", func(e *domain.Extraction) {}, nil},
		{"empty block", func(e *domain.Extraction) {
			e.Transactions = []domain.Transaction{}
			e.Receipts = []domain.Receipt{}
			e.RawReceipts = [][]byte{}
		}, nil},
		{"no logs", func(e *domain.Extraction) {
			for i := range e.Receipts {
				e.Receipts[i].Logs = []domain.Log{}
			}
		}, nil},
		{"bad chain", func(e *domain.Extraction) { e.Header.ID.ChainID = "" }, domain.ErrInvalidData},
		{"missing hash", func(e *domain.Extraction) { e.Header.ID.Hash = domain.Hash{} }, domain.ErrInvalidData},
		{"missing parent", func(e *domain.Extraction) { e.Header.ParentHash = domain.Hash{} }, domain.ErrInvalidData},
		{"missing time", func(e *domain.Extraction) { e.ExtractedAt = time.Time{} }, domain.ErrInvalidData},
		{"missing provider", func(e *domain.Extraction) { e.Provider = " " }, domain.ErrInvalidData},
		{"missing raw block", func(e *domain.Extraction) { e.RawBlock = nil }, domain.ErrIncomplete},
		{"missing transactions", func(e *domain.Extraction) { e.Transactions = nil }, domain.ErrIncomplete},
		{"missing receipts", func(e *domain.Extraction) { e.Receipts = nil }, domain.ErrIncomplete},
		{"missing raw receipts", func(e *domain.Extraction) { e.RawReceipts = nil }, domain.ErrIncomplete},
		{"partial receipts", func(e *domain.Extraction) { e.Receipts = e.Receipts[:1] }, domain.ErrIncomplete},
		{"extra receipt", func(e *domain.Extraction) { e.Receipts = append(e.Receipts, e.Receipts[0]) }, domain.ErrIncomplete},
		{"partial raw receipts", func(e *domain.Extraction) { e.RawReceipts = e.RawReceipts[:1] }, domain.ErrIncomplete},
		{"empty raw receipt", func(e *domain.Extraction) { e.RawReceipts[0] = nil }, domain.ErrIncomplete},
		{"missing logs", func(e *domain.Extraction) { e.Receipts[0].Logs = nil }, domain.ErrIncomplete},
		{"missing tx hash", func(e *domain.Extraction) { e.Transactions[0].Hash = domain.Hash{} }, domain.ErrInvalidData},
		{"duplicate tx", func(e *domain.Extraction) { e.Transactions[1].Hash = e.Transactions[0].Hash }, domain.ErrInvalidData},
		{"tx order", func(e *domain.Extraction) { e.Transactions[0].Index = 1 }, domain.ErrInvalidData},
		{"tx chain", func(e *domain.Extraction) { e.Transactions[0].Block.ChainID = "2" }, domain.ErrChainMismatch},
		{"tx block", func(e *domain.Extraction) { e.Transactions[0].Block.Hash = domain.Hash{99} }, domain.ErrBlockConflict},
		{"receipt chain", func(e *domain.Extraction) { e.Receipts[0].Block.ChainID = "2" }, domain.ErrChainMismatch},
		{"receipt block", func(e *domain.Extraction) { e.Receipts[0].Block.Number++ }, domain.ErrBlockConflict},
		{"receipt order", func(e *domain.Extraction) { e.Receipts[0].TransactionIndex = 1 }, domain.ErrInvalidData},
		{"receipt tx", func(e *domain.Extraction) { e.Receipts[0].TransactionHash = domain.Hash{99} }, domain.ErrInvalidData},
		{"log chain", func(e *domain.Extraction) { e.Receipts[0].Logs[0].Block.ChainID = "2" }, domain.ErrChainMismatch},
		{"log block", func(e *domain.Extraction) { e.Receipts[0].Logs[0].Block.Hash = domain.Hash{99} }, domain.ErrBlockConflict},
		{"log tx", func(e *domain.Extraction) { e.Receipts[0].Logs[0].TransactionHash = domain.Hash{99} }, domain.ErrInvalidData},
		{"log tx index", func(e *domain.Extraction) { e.Receipts[0].Logs[0].TransactionIndex++ }, domain.ErrInvalidData},
		{"duplicate log", func(e *domain.Extraction) { e.Receipts[1].Logs[0].Index = 0 }, domain.ErrInvalidData},
		{"log gap", func(e *domain.Extraction) { e.Receipts[1].Logs[0].Index = 2 }, domain.ErrInvalidData},
		{"too many topics", func(e *domain.Extraction) { e.Receipts[0].Logs[0].Topics = make([]domain.Hash, 5) }, domain.ErrInvalidData},
		{"removed log", func(e *domain.Extraction) { e.Receipts[0].Logs[0].Removed = true }, domain.ErrBlockConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validExtraction()
			tt.change(&e)
			if err := e.Validate(); !errors.Is(err, tt.want) {
				t.Fatalf("Validate() = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestGenesis(t *testing.T) {
	e := validExtraction()
	e.Header.ID.Number = 0
	e.Header.ParentHash = domain.Hash{}
	e.Transactions = []domain.Transaction{}
	e.Receipts = []domain.Receipt{}
	e.RawReceipts = [][]byte{}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCloneOwnershipAndEventIdentity(t *testing.T) {
	original := validExtraction()
	clone := original.Clone()
	if !reflect.DeepEqual(clone, original) {
		t.Fatal("clone changed snapshot")
	}
	id := original.Header.ID.EventID()
	clone.RawBlock[0]++
	clone.RawReceipts[0][0]++
	clone.Transactions[0].Hash[0]++
	clone.Receipts[0].Logs[0].Topics[0][0]++
	clone.Receipts[0].Logs[0].Data[0]++
	if !reflect.DeepEqual(original, validExtraction()) {
		t.Fatal("clone aliases source buffers")
	}
	clone.Provider = "other"
	clone.ExtractedAt = clone.ExtractedAt.Add(time.Hour)
	if clone.Header.ID.EventID() != id {
		t.Fatal("provenance changed event identity")
	}
	for _, block := range []domain.BlockID{
		{ChainID: "2", Number: 10, Hash: domain.Hash{10}},
		{ChainID: "1", Number: 11, Hash: domain.Hash{10}},
		{ChainID: "1", Number: 10, Hash: domain.Hash{11}},
	} {
		if block.EventID() == id {
			t.Fatal("different block shares event ID")
		}
	}
}

func TestCheckpointPosition(t *testing.T) {
	cp := func(n uint64) *domain.Checkpoint {
		return &domain.Checkpoint{Block: domain.BlockID{ChainID: "1", Number: n, Hash: domain.Hash{1}}}
	}
	for _, tt := range []struct {
		name             string
		start, requested uint64
		previous         *domain.Checkpoint
		want             error
	}{
		{"first", 10, 10, nil, nil},
		{"genesis", 0, 0, nil, nil},
		{"wrong start", 10, 11, nil, domain.ErrOutOfSequence},
		{"next", 10, 11, cp(10), nil},
		{"retry", 10, 10, cp(10), nil},
		{"gap", 10, 12, cp(10), domain.ErrOutOfSequence},
		{"old", 10, 10, cp(11), domain.ErrOutOfSequence},
		{"changed start", 11, 11, cp(10), domain.ErrCheckpointConflict},
		{"max retry", 0, math.MaxUint64, cp(math.MaxUint64), nil},
		{"overflow", 0, 0, cp(math.MaxUint64), domain.ErrOutOfSequence},
		{"bad checkpoint", 0, 0, &domain.Checkpoint{}, domain.ErrCheckpointConflict},
		{"wrong chain", 0, 10, &domain.Checkpoint{Block: domain.BlockID{ChainID: "2", Number: 10, Hash: domain.Hash{1}}}, domain.ErrChainMismatch},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := domain.CheckPosition("1", tt.start, tt.requested, tt.previous); !errors.Is(err, tt.want) {
				t.Fatalf("CheckPosition() = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestCheckpointContinuity(t *testing.T) {
	previous := &domain.Checkpoint{Block: domain.BlockID{ChainID: "1", Number: 10, Hash: domain.Hash{10}}}
	for _, tt := range []struct {
		name   string
		header domain.Header
		want   error
	}{
		{"retry", domain.Header{ID: previous.Block}, nil},
		{"replacement", domain.Header{ID: domain.BlockID{ChainID: "1", Number: 10, Hash: domain.Hash{99}}}, domain.ErrBlockConflict},
		{"next", domain.Header{ID: domain.BlockID{ChainID: "1", Number: 11, Hash: domain.Hash{11}}, ParentHash: domain.Hash{10}}, nil},
		{"parent mismatch", domain.Header{ID: domain.BlockID{ChainID: "1", Number: 11, Hash: domain.Hash{11}}, ParentHash: domain.Hash{99}}, domain.ErrBlockConflict},
		{"gap", domain.Header{ID: domain.BlockID{ChainID: "1", Number: 12}}, domain.ErrOutOfSequence},
		{"wrong chain", domain.Header{ID: domain.BlockID{ChainID: "2", Number: 11}}, domain.ErrChainMismatch},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := domain.CheckContinuity(tt.header, previous); !errors.Is(err, tt.want) {
				t.Fatalf("CheckContinuity() = %v, want %v", err, tt.want)
			}
		})
	}
}
