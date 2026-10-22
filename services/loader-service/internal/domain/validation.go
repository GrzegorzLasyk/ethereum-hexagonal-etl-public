package domain

import (
	"errors"
	"fmt"
	"math"
	"math/big"
)

var (
	ErrInvalid     = errors.New("invalid Ethereum data")
	ErrIncomplete  = errors.New("incomplete Ethereum block")
	ErrIdentity    = errors.New("Ethereum identity mismatch")
	ErrUnsupported = errors.New("unsupported Ethereum data")
)

// ValidationError carries a safe field/rule identifier, never source values.
type ValidationError struct {
	Kind  error
	Field string
}

func (e *ValidationError) Error() string { return fmt.Sprintf("%s: %s", e.Kind, e.Field) }
func (e *ValidationError) Unwrap() error { return e.Kind }
func Invalid(field string) error         { return &ValidationError{ErrInvalid, field} }
func Decimal(s string, bits int) bool {
	if s == "" || len(s) > 1 && s[0] == '0' {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	n, ok := new(big.Int).SetString(s, 10)
	return ok && (bits == 0 || n.BitLen() <= bits)
}
func (q Quantity) Valid() bool           { return Decimal(string(q), 256) }
func (c ChainID) Valid() bool            { return Decimal(string(c), 0) }
func Required[T any](o Optional[T]) bool { return o.HasValue() }
func Kind(t Optional[uint64]) uint64 {
	if t.HasValue() {
		return t.Value
	}
	return 0
}
func sameRecipient(a, b Optional[Address]) bool {
	return a.Present && b.Present && a.Null == b.Null && (a.Null || a.Value == b.Value)
}
func cmp(a, b Quantity) int {
	x, _ := new(big.Int).SetString(string(a), 10)
	y, _ := new(big.Int).SetString(string(b), 10)
	return x.Cmp(y)
}

// Validate enforces membership, completeness and type-dependent invariants.
// It asserts neither consensus canonicality nor cryptographic validity.
func (s Snapshot) Validate() error {
	b := s.Block
	if !s.ChainID.Valid() || b.Hash == (Hash{}) || b.Number != 0 && b.ParentHash == (Hash{}) {
		return &ValidationError{ErrIdentity, "block"}
	}
	if err := s.validateValues(); err != nil {
		return err
	}
	if s.Transactions == nil || s.Receipts == nil || len(s.Transactions) != len(s.Receipts) {
		return &ValidationError{ErrIncomplete, "receipts"}
	}
	if b.GasUsed > b.GasLimit {
		return Invalid("block gas")
	}
	if b.BlobGasUsed.HasValue() != b.ExcessBlobGas.HasValue() {
		return Invalid("blob gas pair")
	}
	seen := map[Hash]bool{}
	var logIndex, totalGas uint64
	for i, t := range s.Transactions {
		r := s.Receipts[i]
		if t.Hash == (Hash{}) || seen[t.Hash] || t.Index != uint64(i) || t.BlockNumber != b.Number || t.BlockHash != b.Hash {
			return &ValidationError{ErrIdentity, "transaction"}
		}
		seen[t.Hash] = true
		if r.TransactionHash != t.Hash || r.Index != t.Index || r.BlockNumber != b.Number || r.BlockHash != b.Hash || r.From != t.From || !sameRecipient(r.To, t.To) {
			return &ValidationError{ErrIdentity, "receipt"}
		}
		if !t.To.Present || !r.ContractAddress.Present || !t.To.Null && !r.ContractAddress.Null {
			return Invalid("recipient/contract address")
		}
		kind := Kind(t.Type)
		if kind > 4 {
			return &ValidationError{ErrUnsupported, "transaction type"}
		}
		if Kind(r.Type) != kind {
			return Invalid("receipt type")
		}
		if kind <= 1 && !Required(t.GasPrice) {
			return Invalid("gas price")
		}
		if kind >= 2 && (!Required(t.MaxFeePerGas) || !Required(t.MaxPriorityFeePerGas) || !Required(b.BaseFeePerGas)) {
			return Invalid("dynamic fees")
		}
		if kind >= 2 && cmp(t.MaxPriorityFeePerGas.Value, t.MaxFeePerGas.Value) > 0 {
			return Invalid("fee order")
		}
		if kind == 0 {
			if !Required(t.V) {
				return Invalid("legacy signature")
			}
			v, _ := new(big.Int).SetString(string(t.V.Value), 10)
			unprotected := v.Cmp(big.NewInt(27)) == 0 || v.Cmp(big.NewInt(28)) == 0
			if !unprotected {
				if v.Cmp(big.NewInt(35)) < 0 {
					return Invalid("legacy v")
				}
				derived := new(big.Int).Div(new(big.Int).Sub(v, big.NewInt(35)), big.NewInt(2))
				if derived.String() != string(s.ChainID) {
					return &ValidationError{ErrIdentity, "signature chain"}
				}
			}
			if t.ChainID.HasValue() && t.ChainID.Value != s.ChainID && !(unprotected && t.ChainID.Value == "0") {
				return &ValidationError{ErrIdentity, "transaction chain"}
			}
		} else {
			if !Required(t.ChainID) || t.ChainID.Value != s.ChainID {
				return &ValidationError{ErrIdentity, "transaction chain"}
			}
			if !Required(t.AccessList) || !Required(t.V) && !Required(t.YParity) {
				return Invalid("typed signature/access list")
			}
			if t.V.HasValue() && t.V.Value != "0" && t.V.Value != "1" || t.YParity.HasValue() && t.YParity.Value > 1 {
				return Invalid("signature parity")
			}
			if t.V.HasValue() && t.YParity.HasValue() && string(t.V.Value) != fmt.Sprint(t.YParity.Value) {
				return Invalid("signature parity mismatch")
			}
		}
		if kind >= 3 && t.To.Null {
			return Invalid("typed recipient")
		}
		if kind == 3 && (!Required(t.MaxFeePerBlobGas) || !Required(t.BlobVersionedHashes) || len(t.BlobVersionedHashes.Value) == 0 || !Required(b.BlobGasUsed) || !Required(r.BlobGasUsed) || !Required(r.BlobGasPrice)) {
			return Invalid("blob fields")
		}
		if kind == 4 && (!Required(t.AuthorizationList) || len(t.AuthorizationList.Value) == 0) {
			return Invalid("authorization list")
		}
		if r.Status.HasValue() == r.Root.HasValue() || r.Status.HasValue() && r.Status.Value > 1 || kind != 0 && !r.Status.HasValue() {
			return Invalid("receipt outcome")
		}
		if b.BaseFeePerGas.HasValue() && !Required(r.EffectiveGasPrice) {
			return Invalid("effective gas price")
		}
		if r.GasUsed > t.Gas || math.MaxUint64-totalGas < r.GasUsed {
			return Invalid("receipt gas")
		}
		totalGas += r.GasUsed
		if r.CumulativeGasUsed != totalGas {
			return Invalid("cumulative gas")
		}
		if r.Logs == nil {
			return &ValidationError{ErrIncomplete, "logs"}
		}
		for _, l := range r.Logs {
			if l.BlockHash != b.Hash || l.BlockNumber != b.Number || l.TransactionHash != t.Hash || l.TransactionIndex != t.Index || l.Index != logIndex {
				return &ValidationError{ErrIdentity, "log"}
			}
			if l.Topics == nil || len(l.Topics) > 4 || l.Removed.HasValue() && l.Removed.Value {
				return Invalid("log topics/removed")
			}
			if l.BlockTimestamp.HasValue() && l.BlockTimestamp.Value != b.Timestamp {
				return Invalid("log timestamp")
			}
			logIndex++
		}
	}
	if totalGas != b.GasUsed {
		return Invalid("block gas total")
	}
	return nil
}
