// config.go implements validated environment configuration and conservative startup limits in the
// Extractor fixture/operator adapter.

package fixturecapture

import (
	"errors"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/jsonrpc"
)

// ErrConfig signals capture requires explicit opt-in outside CI, a private Mainnet endpoint and
// valid bounds. Callers can match this category with errors.Is without parsing error text.
var ErrConfig = errors.New("capture requires explicit opt-in outside CI, a private Mainnet endpoint and valid bounds")

// endpointPath requires the expected credential-bearing Mainnet endpoint path without exposing its
// value.
var endpointPath = regexp.MustCompile(`^/v3/[A-Za-z0-9_-]+$`)

// Settings never includes the endpoint in errors or persisted metadata.
func Settings(get func(string) string, c Candidate) (jsonrpc.Config, time.Duration, error) {
	var zero jsonrpc.Config
	if c.Validate() != nil || get("CI") != "" || get("GITHUB_ACTIONS") != "" || get("ETHEREUM_CAPTURE_OPT_IN") != "true" {
		return zero, 0, ErrConfig
	}
	u, err := url.Parse(get("ETHEREUM_RPC_URL"))
	if err != nil || u.Scheme != "https" || u.Host != "mainnet.infura.io" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || !endpointPath.MatchString(u.Path) {
		return zero, 0, ErrConfig
	}
	max, err := strconv.ParseUint(get("ETHEREUM_CAPTURE_MAX_REQUESTS"), 10, 64)
	if err != nil || max < uint64(c.Counts.Transactions)+3 || max > 1000 {
		return zero, 0, ErrConfig
	}
	duration, err := time.ParseDuration(get("ETHEREUM_CAPTURE_TIMEOUT"))
	if err != nil || duration < time.Millisecond || duration > 2*time.Minute {
		return zero, 0, ErrConfig
	}
	return jsonrpc.Config{URL: u.String(), ChainID: "1", Provider: "infura-mainnet", RequestTimeout: 15 * time.Second, MaxAttempts: 3, MaxRequests: max, InitialBackoff: 500 * time.Millisecond, MaxBackoff: 5 * time.Second, MinRequestInterval: 200 * time.Millisecond, MaxResponseBytes: 8 << 20, MaxSnapshotBytes: 64 << 20}, duration, nil
}

// SecretTokens is also usable offline without opt-in or a network client.
// An empty environment needs no credentials to validate public fixture data.
func SecretTokens(endpoint string) []string {
	if endpoint == "" {
		return nil
	}
	tokens := []string{endpoint}
	if u, err := url.Parse(endpoint); err == nil && endpointPath.MatchString(u.Path) {
		tokens = append(tokens, strings.TrimPrefix(u.Path, "/v3/"))
	}
	return tokens
}
