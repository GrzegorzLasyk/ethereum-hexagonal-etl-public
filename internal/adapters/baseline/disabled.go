//go:build !perfbaseline

// Package baseline supplies opt-in, build-tagged measurement instrumentation.
package baseline

import "github.com/jackc/pgx/v5/pgxpool"

type Probe struct{}

func Configure(*pgxpool.Config) (*Probe, error) { return &Probe{}, nil }
func (*Probe) Attach(*pgxpool.Pool)             {}
func (*Probe) Close()                           {}
