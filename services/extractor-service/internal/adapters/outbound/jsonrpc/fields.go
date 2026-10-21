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

func invalid(field string) error {
	return fmt.Errorf("%w: invalid or missing %s", domain.ErrInvalidData, field)
}

func objectOf(raw []byte) (object, error) {
	var o object
	if json.Unmarshal(raw, &o) != nil || o == nil {
		return nil, invalid("object")
	}
	return o, nil
}

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

func uintField(o object, key string) (uint64, error) {
	n, err := parseQuantity(o[key], 64)
	if err != nil {
		return 0, invalid(key)
	}
	return n.Uint64(), nil
}

func (o object) quantities(bits int, keys ...string) error {
	for _, key := range keys {
		if _, err := parseQuantity(o[key], bits); err != nil {
			return invalid(key)
		}
	}
	return nil
}

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

func (o object) data(length int, keys ...string) error {
	for _, key := range keys {
		if _, err := dataField(o, key, length); err != nil {
			return err
		}
	}
	return nil
}

func hashField(o object, key string) (domain.Hash, error) {
	var result domain.Hash
	b, err := dataField(o, key, len(result))
	copy(result[:], b)
	return result, err
}

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

func arrayField(o object, key string) ([]json.RawMessage, error) {
	var items []json.RawMessage
	if json.Unmarshal(o[key], &items) != nil || items == nil {
		return nil, invalid(key)
	}
	return items, nil
}

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
