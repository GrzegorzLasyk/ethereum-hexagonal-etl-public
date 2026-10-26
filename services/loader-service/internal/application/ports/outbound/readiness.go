package outbound

import "context"

// ReadinessChecker verifies only this service's required local dependencies.
// Implementations must bound I/O and return no sensitive diagnostics.
type ReadinessChecker interface{ CheckReadiness(context.Context) error }
