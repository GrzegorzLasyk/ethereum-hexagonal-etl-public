// readiness.go defines contracts for owning-schema database readiness at the adapter boundary in
// the application outbound boundary.

package outbound

import "context"

// ReadinessChecker verifies only this service's required local dependencies.
// Implementations must bound I/O and return no sensitive diagnostics.
type ReadinessChecker interface {
	// CheckReadiness checks only owning dependencies/schema privileges within the context; it must not
	// require downstream availability.
	CheckReadiness(context.Context) error
}
