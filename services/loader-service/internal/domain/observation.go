package domain

import "math"

// BlockReference describes one observed candidate, not a canonical block.
type BlockReference struct {
	ChainID    ChainID
	Number     uint64
	Hash       Hash
	ParentHash Hash
}

type CanonicalStatus string

const CanonicalUnknown CanonicalStatus = "unknown"

type ParentLink string

const (
	ParentUnknown  ParentLink = "unknown"
	ParentLinked   ParentLink = "linked"
	ParentMismatch ParentLink = "mismatch"
)

type Observation struct {
	Block           BlockReference
	CanonicalStatus CanonicalStatus
	HeightConflict  bool
	ParentLink      ParentLink
}

// ObservationRange bounds facts required for the arriving height and its
// immediate children. At the uint64 boundaries it never wraps around.
func ObservationRange(number uint64) (low, high uint64) {
	low, high = number, number
	if number > 0 {
		low--
	}
	if number < math.MaxUint64 {
		high++
	}
	return
}

// ObserveArrival preserves every candidate and describes only local evidence.
// Callers supply all same-chain identities in ObservationRange after the insert.
// Missing parents are unknown; any matching parent links a child even if the
// parent height has competing candidates. Linking never establishes canonicality.
func ObserveArrival(arrived BlockReference, neighbors []BlockReference) []Observation {
	byHeight := make(map[uint64]map[Hash]struct{})
	for _, b := range neighbors {
		if b.ChainID != arrived.ChainID {
			continue
		}
		if byHeight[b.Number] == nil {
			byHeight[b.Number] = make(map[Hash]struct{})
		}
		byHeight[b.Number][b.Hash] = struct{}{}
	}
	result := make([]Observation, 0)
	for _, b := range neighbors {
		if b.ChainID != arrived.ChainID || (b.Number != arrived.Number && (arrived.Number == math.MaxUint64 || b.Number != arrived.Number+1)) {
			continue
		}
		o := Observation{Block: b, CanonicalStatus: CanonicalUnknown, HeightConflict: len(byHeight[b.Number]) > 1, ParentLink: ParentUnknown}
		if b.Number > 0 && len(byHeight[b.Number-1]) > 0 {
			if _, exists := byHeight[b.Number-1][b.ParentHash]; exists {
				o.ParentLink = ParentLinked
			} else {
				o.ParentLink = ParentMismatch
			}
		}
		result = append(result, o)
	}
	return result
}
