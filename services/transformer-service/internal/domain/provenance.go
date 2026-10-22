package domain

import "time"

type Provenance struct {
	SourceEventID string
	Provider      string
	ExtractedAt   time.Time
}
