// validation_test.go exercises pure value, membership and identity validation at the local
// boundary in the pure domain layer.

package domain

import (
	"errors"
	"strings"
	"testing"
)

// validSnapshot builds internally consistent synthetic normalized records for pure domain
// validation tests.
func validSnapshot() Snapshot {
	hash := Hash{1}
	txhash := Hash{2}
	null := Optional[Address]{Present: true, Null: true}
	return Snapshot{ChainID: "1", Block: Block{Number: 1, Hash: hash, ParentHash: Hash{3}, Difficulty: "0", Size: "1", ExtraData: []byte{}, Uncles: []Hash{}, GasUsed: 1, GasLimit: 2},
		Transactions: []Transaction{{Hash: txhash, BlockHash: hash, BlockNumber: 1, From: Address{1}, To: null, Value: "0", R: "1", S: "2", V: Optional[Quantity]{Present: true, Value: "27"}, GasPrice: Optional[Quantity]{Present: true, Value: "1"}, Gas: 2, Input: []byte{}}},
		Receipts:     []Receipt{{TransactionHash: txhash, BlockHash: hash, BlockNumber: 1, From: Address{1}, To: null, ContractAddress: null, Status: Optional[uint64]{Present: true, Value: 0}, GasUsed: 1, CumulativeGasUsed: 1, Logs: []Log{}}}}
}

// TestSnapshotValidation checks complete normalized membership and type/value invariants
// independently of transport decoding.
func TestSnapshotValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Snapshot)
		want   error
	}{
		{"valid failed transaction", func(*Snapshot) {}, nil},
		{"missing receipt", func(s *Snapshot) { s.Receipts = nil }, ErrIncomplete},
		{"wrong block", func(s *Snapshot) { s.Receipts[0].BlockHash = Hash{9} }, ErrIdentity},
		{"wrong parent", func(s *Snapshot) { s.Block.ParentHash = Hash{} }, ErrIdentity},
		{"missing recipient", func(s *Snapshot) { s.Transactions[0].To.Present = false }, ErrInvalid},
		{"unknown type", func(s *Snapshot) { s.Transactions[0].Type = Optional[uint64]{Present: true, Value: 5} }, ErrUnsupported},
		{"bad value", func(s *Snapshot) { s.Transactions[0].Value = "01" }, ErrInvalid},
		{"bad gas", func(s *Snapshot) { s.Receipts[0].CumulativeGasUsed = 0 }, ErrInvalid},
		{"missing logs", func(s *Snapshot) { s.Receipts[0].Logs = nil }, ErrIncomplete},
		{"signature chain", func(s *Snapshot) { s.Transactions[0].V.Value = "39" }, ErrIdentity},
		{"both outcomes", func(s *Snapshot) { s.Receipts[0].Root = Optional[Hash]{Present: true, Value: Hash{4}} }, ErrInvalid},
		{"neither outcome", func(s *Snapshot) { s.Receipts[0].Status = Optional[uint64]{} }, ErrInvalid},
		{"status range", func(s *Snapshot) { s.Receipts[0].Status.Value = 2 }, ErrInvalid},
		{"historical root", func(s *Snapshot) {
			s.Receipts[0].Status = Optional[uint64]{}
			s.Receipts[0].Root = Optional[Hash]{Present: true, Value: Hash{4}}
		}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validSnapshot()
			tt.change(&s)
			err := s.Validate()
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v want %v", err, tt.want)
			}
			if err != nil {
				var typed *ValidationError
				if !errors.As(err, &typed) {
					t.Fatal("missing typed validation error")
				}
			}
		})
	}
}

// TestExactQuantities rejects noncanonical or lossy numeric representations while retaining
// arbitrarily large exact decimal values.
func TestExactQuantities(t *testing.T) {
	for _, v := range []string{"0", "1", "115792089237316195423570985008687907853269984665640564039457584007913129639935"} {
		if !Quantity(v).Valid() {
			t.Fatal(v)
		}
	}
	for _, v := range []string{"", "-1", "01", "1.0", "1e3", strings.Repeat("9", 79)} {
		if Quantity(v).Valid() {
			t.Fatal(v)
		}
	}
	if !ChainID(strings.Repeat("9", 79)).Valid() {
		t.Fatal("chain ID truncated")
	}
}

// TestLogsAndDuplicateTransactions rejects inconsistent log positions and repeated transaction
// identities in a complete normalized snapshot.
func TestLogsAndDuplicateTransactions(t *testing.T) {
	s := validSnapshot()
	s.Transactions = append(s.Transactions, s.Transactions[0])
	s.Receipts = append(s.Receipts, s.Receipts[0])
	if !errors.Is(s.Validate(), ErrIdentity) {
		t.Fatal("duplicate transaction accepted")
	}
	for _, change := range []func(*Log){
		func(l *Log) { l.Removed = Optional[bool]{Present: true, Value: true} },
		func(l *Log) { l.Index = 1 }, func(l *Log) { l.Topics = make([]Hash, 5) }, func(l *Log) { l.TransactionHash = Hash{7} },
	} {
		s := validSnapshot()
		l := Log{BlockHash: s.Block.Hash, BlockNumber: 1, TransactionHash: s.Transactions[0].Hash, Topics: []Hash{}, Data: []byte{}}
		s.Receipts[0].Logs = []Log{l}
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
		change(&s.Receipts[0].Logs[0])
		if s.Validate() == nil {
			t.Fatal("invalid log accepted")
		}
	}
}
