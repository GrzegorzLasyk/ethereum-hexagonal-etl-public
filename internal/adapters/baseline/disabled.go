//go:build !perfbaseline

// disabled.go implements zero-cost baseline hooks in ordinary builds without the perfbaseline tag
// in the shared technical support layer, outside service domains.

// Package baseline supplies opt-in, build-tagged measurement instrumentation.
package baseline

import "github.com/jackc/pgx/v5/pgxpool"

// Probe is an inert lifecycle hook in ordinary builds; the perfbaseline variant owns sampling state.
type Probe struct{}

// Configure returns a no-op probe in ordinary builds; enabling instrumentation requires the
// perfbaseline build tag.
func Configure(*pgxpool.Config) (*Probe, error) { return &Probe{}, nil }

// Attach attaches the pool for baseline observation; ordinary builds keep this hook inert.
func (*Probe) Attach(*pgxpool.Pool) {}

// Close stops sampling and writes exclusive baseline evidence when enabled; ordinary builds
// perform no output.
func (*Probe) Close() {}
