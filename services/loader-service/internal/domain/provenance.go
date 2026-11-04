// provenance.go implements source-event and extraction-origin identity carried through
// normalization in the pure domain layer.

package domain

import "time"

// Provenance retains the stable source event, non-secret provider label and original extraction
// time.
type Provenance struct {
	SourceEventID string
	Provider      string
	ExtractedAt   time.Time
}
