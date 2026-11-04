// fields.go implements JSON field shape, numeric-width and missing/null conversion helpers in the
// outbound adapter layer.

package jsonrpc

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// object is an adapter-owned wire DTO. Unknown fields remain in the raw result.
type object map[string]json.RawMessage

// invalid returns a safe field-specific validation error without embedding source JSON.
func invalid(field string) error {
	return fmt.Errorf("%w: invalid or missing %s", domain.ErrInvalidData, field)
}

// objectOf requires a JSON object and rejects ambiguous duplicate fields before typed conversion.
func objectOf(raw []byte) (object, error) {
	var o object
	if json.Unmarshal(raw, &o) != nil || o == nil {
		return nil, invalid("object")
	}
	return o, nil
}

// uniqueObject rejects duplicate object keys before typed field decoding can hide ambiguity.
func uniqueObject(raw []byte) (object, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := walkJSON(decoder, 0); err != nil {
		return nil, ErrProtocol
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, ErrProtocol
	}
	return objectOf(raw)
}

// Bound nesting and reject duplicate members, avoiding ambiguous metadata/raw
// interpretations while still preserving unknown extension fields.
func walkJSON(d *json.Decoder, depth int) error {
	if depth > 64 {
		return ErrProtocol
	}
	token, err := d.Token()
	if err != nil {
		return err
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
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return ErrProtocol
			}
			seen[name] = true
			if err := walkJSON(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := walkJSON(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return ErrProtocol
	}
	_, err = d.Token()
	return err
}

// parseQuantity parses a canonical RPC hex integer with the requested bit bound; it never rounds
// through a float.
func parseQuantity(raw json.RawMessage, bits int) (*big.Int, error) {
	var s string
	if json.Unmarshal(raw, &s) != nil || !strings.HasPrefix(s, "0x") ||
		len(s) < 3 || (len(s) > 3 && s[2] == '0') {
		return nil, invalid("quantity")
	}
	for _, c := range s[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return nil, invalid("quantity")
		}
	}
	n, ok := new(big.Int).SetString(s[2:], 16)
	if !ok || bits > 0 && n.BitLen() > bits {
		return nil, invalid("quantity")
	}
	return n, nil
}

// uintField reads a required unsigned integer field and rejects absent, malformed or overflowing
// values.
func uintField(o object, key string) (uint64, error) {
	n, err := parseQuantity(o[key], 64)
	if err != nil {
		return 0, invalid(key)
	}
	return n.Uint64(), nil
}

// quantities validates the listed required RPC quantity fields while leaving raw bytes untouched.
func (o object) quantities(bits int, keys ...string) error {
	for _, key := range keys {
		if _, err := parseQuantity(o[key], bits); err != nil {
			return invalid(key)
		}
	}
	return nil
}

// dataField decodes a hex byte field, applying the requested width and missing/null rules.
func dataField(o object, key string, length int) ([]byte, error) {
	var s string
	if json.Unmarshal(o[key], &s) != nil || !strings.HasPrefix(s, "0x") ||
		len(s)%2 != 0 || length >= 0 && len(s) != 2+2*length {
		return nil, invalid(key)
	}
	data, err := hex.DecodeString(s[2:])
	if err != nil {
		return nil, invalid(key)
	}
	return data, nil
}

// data decodes hexadecimal data with explicit width rules instead of interpreting it as an
// integer.
func (o object) data(length int, keys ...string) error {
	for _, key := range keys {
		if _, err := dataField(o, key, length); err != nil {
			return err
		}
	}
	return nil
}

// hashField decodes a fixed-width hash from a required JSON field.
func hashField(o object, key string) (domain.Hash, error) {
	var result domain.Hash
	b, err := dataField(o, key, len(result))
	copy(result[:], b)
	return result, err
}

// addressField decodes an address while preserving the field's null allowance.
func addressField(o object, key string, nullable bool) (*domain.Address, error) {
	if raw, exists := o[key]; exists && nullable && bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	var result domain.Address
	b, err := dataField(o, key, len(result))
	if err != nil {
		return nil, err
	}
	copy(result[:], b)
	return &result, nil
}

// arrayField requires an explicit JSON array rather than treating missing data as an empty
// collection.
func arrayField(o object, key string) ([]json.RawMessage, error) {
	var items []json.RawMessage
	if json.Unmarshal(o[key], &items) != nil || items == nil {
		return nil, invalid(key)
	}
	return items, nil
}

// blockReference checks that source block hash/number fields bind a child object to the expected
// mined block.
func blockReference(o object, chain domain.ChainID, numberKey, hashKey string) (domain.BlockID, error) {
	number, err := uintField(o, numberKey)
	if err != nil {
		return domain.BlockID{}, err
	}
	hash, err := hashField(o, hashKey)
	if err != nil {
		return domain.BlockID{}, err
	}
	id := domain.BlockID{ChainID: chain, Number: number, Hash: hash}
	return id, id.Validate()
}

// optionalQuantities validates quantities only when supplied, preserving absence and null
// distinctions.
func (o object) optionalQuantities(bits int, keys ...string) error {
	for _, key := range keys {
		if _, exists := o[key]; exists {
			if err := o.quantities(bits, key); err != nil {
				return err
			}
		}
	}
	return nil
}

// optionalData validates optional hex-data fields without materializing defaults for missing
// values.
func (o object) optionalData(length int, keys ...string) error {
	for _, key := range keys {
		if _, exists := o[key]; exists {
			if err := o.data(length, key); err != nil {
				return err
			}
		}
	}
	return nil
}
