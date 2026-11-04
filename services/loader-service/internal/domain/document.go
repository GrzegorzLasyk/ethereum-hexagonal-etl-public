// document.go implements versioned normalized content bound to original source provenance in the
// pure domain layer.

package domain

// Document is the Loader-owned normalized aggregate, independent of JSON/SQL.
type Document struct {
	Snapshot   Snapshot
	Provenance Provenance
}
