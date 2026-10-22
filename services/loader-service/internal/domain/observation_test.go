package domain

import (
	"math"
	"reflect"
	"testing"
)

func TestObserveArrival(t *testing.T) {
	parent := BlockReference{ChainID: "1", Number: 10, Hash: Hash{10}}
	other := BlockReference{ChainID: "1", Number: 10, Hash: Hash{99}}
	child := BlockReference{ChainID: "1", Number: 11, Hash: Hash{11}, ParentHash: parent.Hash}
	foreign := parent
	foreign.ChainID = "2"
	for _, tc := range []struct {
		name    string
		arrival BlockReference
		facts   []BlockReference
		want    []Observation
	}{
		{"missing parent", child, []BlockReference{child}, []Observation{{child, CanonicalUnknown, false, ParentUnknown}}},
		{"foreign parent ignored", child, []BlockReference{foreign, child}, []Observation{{child, CanonicalUnknown, false, ParentUnknown}}},
		{"late wrong parent", other, []BlockReference{other, child}, []Observation{{other, CanonicalUnknown, false, ParentUnknown}, {child, CanonicalUnknown, false, ParentMismatch}}},
		{"late matching competing parent", parent, []BlockReference{other, parent, child}, []Observation{{other, CanonicalUnknown, true, ParentUnknown}, {parent, CanonicalUnknown, true, ParentUnknown}, {child, CanonicalUnknown, false, ParentLinked}}},
		{"child of competing parents", child, []BlockReference{parent, other, child}, []Observation{{child, CanonicalUnknown, false, ParentLinked}}},
		{"duplicate evidence is not conflict", child, []BlockReference{parent, parent, child}, []Observation{{child, CanonicalUnknown, false, ParentLinked}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := append([]BlockReference(nil), tc.facts...)
			if got := ObserveArrival(tc.arrival, tc.facts); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("observations=%+v want=%+v", got, tc.want)
			}
			if !reflect.DeepEqual(before, tc.facts) {
				t.Fatal("modified source facts")
			}
		})
	}
}

func TestObservationBoundaries(t *testing.T) {
	for _, tc := range []struct{ number, low, high uint64 }{{0, 0, 1}, {1, 0, 2}, {math.MaxUint64, math.MaxUint64 - 1, math.MaxUint64}} {
		low, high := ObservationRange(tc.number)
		if low != tc.low || high != tc.high {
			t.Fatalf("range(%d)=(%d,%d)", tc.number, low, high)
		}
	}
	genesis := BlockReference{ChainID: "1", Number: 0, Hash: Hash{1}, ParentHash: Hash{99}}
	last := BlockReference{ChainID: "1", Number: math.MaxUint64, Hash: Hash{99}, ParentHash: Hash{1}}
	for _, b := range []BlockReference{genesis, last} {
		got := ObserveArrival(b, []BlockReference{genesis, last})
		if len(got) != 1 || got[0].Block != b || got[0].ParentLink != ParentUnknown || got[0].CanonicalStatus != CanonicalUnknown {
			t.Fatalf("boundary observation=%+v", got)
		}
	}
}
