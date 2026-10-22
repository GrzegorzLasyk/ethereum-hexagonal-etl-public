// Package jsondocument owns normalized JSON DTOs and representation conversion.
package jsondocument

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/big"
	"strings"
	"unicode/utf8"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
)

type object map[string]json.RawMessage
type reader struct {
	o   object
	err error
}

func read[T any](r *reader, key string, decode func(json.RawMessage) (T, error)) T {
	var zero T
	if r.err != nil {
		return zero
	}
	v, err := decode(r.o[key])
	if err != nil {
		r.err = domain.Invalid(key)
		return zero
	}
	return v
}
func optional[T any](r *reader, key string, decode func(json.RawMessage) (T, error), required bool) domain.Optional[T] {
	raw, present := r.o[key]
	result := domain.Optional[T]{Present: present}
	if !present {
		if required && r.err == nil {
			r.err = domain.Invalid(key)
		}
		return result
	}
	result.Null = bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
	if !result.Null {
		result.Value = read(r, key, decode)
	}
	return result
}
func array(raw json.RawMessage) ([]json.RawMessage, error) {
	var v []json.RawMessage
	if json.Unmarshal(raw, &v) != nil || v == nil {
		return nil, domain.Invalid("array")
	}
	return v, nil
}
func plainObject(raw []byte) (object, error) {
	var v object
	if json.Unmarshal(raw, &v) != nil || v == nil {
		return nil, domain.Invalid("object")
	}
	return v, nil
}
func objectOf(ctx context.Context, raw []byte) (object, error) {
	if !utf8.Valid(raw) {
		return nil, domain.Invalid("JSON UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := walk(ctx, d, 0); err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, domain.Invalid("JSON trailing input")
	}
	return plainObject(raw)
}
func walk(ctx context.Context, d *json.Decoder, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > 64 {
		return domain.Invalid("JSON nesting")
	}
	token, err := d.Token()
	if err != nil {
		return domain.Invalid("JSON syntax")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return domain.Invalid("JSON member")
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return domain.Invalid("duplicate JSON member")
			}
			seen[name] = true
			if err := walk(ctx, d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := walk(ctx, d, depth+1); err != nil {
				return err
			}
		}
	default:
		return domain.Invalid("JSON delimiter")
	}
	if _, err := d.Token(); err != nil {
		return domain.Invalid("JSON closing delimiter")
	}
	return nil
}
func number(raw json.RawMessage, bits int) (*big.Int, error) {
	var s string
	if json.Unmarshal(raw, &s) != nil || !domain.Decimal(s, bits) {
		return nil, domain.Invalid("decimal quantity")
	}
	n, _ := new(big.Int).SetString(s, 10)
	return n, nil
}
func quantity(raw json.RawMessage) (domain.Quantity, error) {
	n, err := number(raw, 256)
	if err != nil {
		return "", err
	}
	return domain.Quantity(n.String()), nil
}
func chainID(raw json.RawMessage) (domain.ChainID, error) {
	n, err := number(raw, 0)
	if err != nil {
		return "", err
	}
	return domain.ChainID(n.String()), nil
}
func uintValue(raw json.RawMessage) (uint64, error) {
	n, err := number(raw, 64)
	if err != nil {
		return 0, err
	}
	return n.Uint64(), nil
}
func data(raw json.RawMessage) ([]byte, error) {
	var s string
	if json.Unmarshal(raw, &s) != nil || !strings.HasPrefix(s, "0x") || len(s)%2 != 0 || strings.ToLower(s) != s {
		return nil, domain.Invalid("hex data")
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return nil, domain.Invalid("hex data")
	}
	return b, nil
}
func hash(raw json.RawMessage) (domain.Hash, error) {
	var v domain.Hash
	b, err := data(raw)
	if err != nil || len(b) != len(v) {
		return v, domain.Invalid("hash")
	}
	copy(v[:], b)
	return v, nil
}
func address(raw json.RawMessage) (domain.Address, error) {
	var v domain.Address
	b, err := data(raw)
	if err != nil || len(b) != len(v) {
		return v, domain.Invalid("address")
	}
	copy(v[:], b)
	return v, nil
}
func bloom(raw json.RawMessage) (domain.Bloom, error) {
	var v domain.Bloom
	b, err := data(raw)
	if err != nil || len(b) != len(v) {
		return v, domain.Invalid("bloom")
	}
	copy(v[:], b)
	return v, nil
}
func nonce(raw json.RawMessage) (domain.Nonce, error) {
	var v domain.Nonce
	b, err := data(raw)
	if err != nil || len(b) != len(v) {
		return v, domain.Invalid("nonce")
	}
	copy(v[:], b)
	return v, nil
}
func hashes(raw json.RawMessage) ([]domain.Hash, error) {
	items, err := array(raw)
	if err != nil {
		return nil, err
	}
	v := make([]domain.Hash, 0, len(items))
	for _, item := range items {
		h, err := hash(item)
		if err != nil {
			return nil, err
		}
		v = append(v, h)
	}
	return v, nil
}
func boolean(raw json.RawMessage) (bool, error) {
	if bytes.Equal(raw, []byte("true")) {
		return true, nil
	}
	if bytes.Equal(raw, []byte("false")) {
		return false, nil
	}
	return false, domain.Invalid("boolean")
}
