package domain

// Document is the Loader-owned normalized aggregate, independent of JSON/SQL.
type Document struct {
	Snapshot   Snapshot
	Provenance Provenance
}
