// client.go implements bounded JSON-RPC transport, attempt accounting, pacing and response
// validation in the outbound adapter layer.

package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
)

var (
	// ErrConfig signals invalid RPC configuration. Callers can match this category with errors.Is
	// without parsing error text.
	ErrConfig = errors.New("invalid RPC configuration")
	// ErrProtocol signals invalid JSON-RPC response. Callers can match this category with errors.Is
	// without parsing error text.
	ErrProtocol = errors.New("invalid JSON-RPC response")
	// ErrTransport signals RPC transport failed. Callers can match this category with errors.Is
	// without parsing error text.
	ErrTransport = errors.New("RPC transport failed")
	// ErrResponseTooLarge signals RPC response size limit exceeded. Callers can match this category
	// with errors.Is without parsing error text.
	ErrResponseTooLarge = errors.New("RPC response size limit exceeded")
	// ErrSnapshotTooLarge signals raw block snapshot size limit exceeded. Callers can match this
	// category with errors.Is without parsing error text.
	ErrSnapshotTooLarge = errors.New("raw block snapshot size limit exceeded")
)

// Config has no implicit network or resource-budget defaults.
// ReceiptConcurrency zero preserves the conservative single-worker default. MaxAttempts includes
// the first request. URL is secret-bearing and must never be logged.
type Config struct {
	ReceiptConcurrency int // 1..8; zero means DefaultReceiptConcurrency.
	URL                string
	ChainID            domain.ChainID
	Provider           string
	RequestTimeout     time.Duration
	MaxAttempts        int
	MaxRequests        uint64 // Total HTTP attempts per client/run; zero preserves unbounded local operation.
	InitialBackoff     time.Duration
	MaxBackoff         time.Duration
	MinRequestInterval time.Duration
	MaxResponseBytes   int64
	MaxSnapshotBytes   int64
}

// HTTPError and RPCError expose numeric diagnostics, never provider text.
type HTTPError struct{ Status int }

// Error formats only the numeric HTTP status, never provider response text or the private
// endpoint.
func (e *HTTPError) Error() string { return fmt.Sprintf("RPC HTTP status %d", e.Status) }

// RPCError retains only a numeric provider error code so errors cannot disclose response bodies or
// private URLs.
type RPCError struct{ Code int }

// Error formats only the numeric JSON-RPC code, never the provider message or endpoint.
func (e *RPCError) Error() string { return fmt.Sprintf("JSON-RPC error code %d", e.Code) }

// Client owns HTTP transport, atomic request identity/budget and a mutex-protected pacing gate
// shared by receipt workers.
type Client struct {
	config      Config
	http        *http.Client
	id          atomic.Uint64
	requests    atomic.Uint64
	gate        sync.Mutex
	metrics     *metrics.Registry
	nextRequest time.Time // protected by gate
}

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ outbound.BlockSource = (*Client)(nil)

// New validates endpoint and RPC limits, wraps attempt accounting and disables redirects;
// construction itself makes no requests.
func New(config Config, client *http.Client, registries ...*metrics.Registry) (*Client, error) {
	if config.ReceiptConcurrency == 0 {
		config.ReceiptConcurrency = DefaultReceiptConcurrency
	}
	if config.ReceiptConcurrency < 1 || config.ReceiptConcurrency > MaxReceiptConcurrency {
		return nil, fmt.Errorf("%w: receipt concurrency must be 1..8", ErrConfig)
	}
	u, err := url.Parse(config.URL)
	if err != nil || u == nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return nil, fmt.Errorf("%w: URL must be an absolute endpoint without userinfo or fragment", ErrConfig)
	}
	loopback := u.Hostname() == "localhost"
	if ip := net.ParseIP(u.Hostname()); ip != nil {
		loopback = ip.IsLoopback()
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return nil, fmt.Errorf("%w: HTTPS is required except on loopback", ErrConfig)
	}
	if err := config.ChainID.Validate(); err != nil {
		return nil, fmt.Errorf("%w: expected chain ID", ErrConfig)
	}
	if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`).MatchString(config.Provider) {
		return nil, fmt.Errorf("%w: provider must be a non-secret label", ErrConfig)
	}
	if config.RequestTimeout <= 0 || config.MaxAttempts < 1 ||
		config.InitialBackoff <= 0 || config.MaxBackoff < config.InitialBackoff ||
		config.MinRequestInterval < 0 || config.MaxResponseBytes <= 0 ||
		config.MaxResponseBytes == math.MaxInt64 || config.MaxSnapshotBytes < config.MaxResponseBytes {
		return nil, fmt.Errorf("%w: invalid timeout, retry, pacing, or size bounds", ErrConfig)
	}
	h := http.Client{}
	if client != nil {
		h = *client
	}
	// Never forward a credential-bearing endpoint to a redirect target.
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	var registry *metrics.Registry
	if len(registries) > 0 {
		registry = registries[0]
	}
	c := &Client{config: config, http: &h, metrics: registry}
	registry.ConfigureRPC(config.MaxRequests)
	transport := h.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	h.Transport = attemptTransport{client: c, next: transport}
	return c, nil
}

// ChainID queries and validates the provider chain against configured identity before source data
// can be trusted.
func (c *Client) ChainID(ctx context.Context) (domain.ChainID, error) {
	raw, err := c.call(ctx, "eth_chainId", []any{})
	if err != nil {
		return "", err
	}
	n, err := parseQuantity(raw, 0)
	if err != nil {
		return "", invalid("chainId")
	}
	chain := domain.ChainID(n.String())
	if chain != c.config.ChainID {
		return "", domain.ErrChainMismatch
	}
	return chain, nil
}

// Block acquires and validates every receipt plus the numbered block, returning no partial
// extraction on failure and preserving raw result bytes.
func (c *Client) Block(ctx context.Context, number uint64) (domain.Extraction, error) {
	var empty domain.Extraction
	chain, err := c.ChainID(ctx)
	if err != nil {
		return empty, fmt.Errorf("verify source chain: %w", err)
	}
	raw, err := c.call(ctx, "eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", number), true})
	if err != nil {
		return empty, fmt.Errorf("fetch block: %w", err)
	}
	extraction, txs, gasUsed, err := decodeBlock(raw, chain, number)
	if err != nil {
		return empty, err
	}
	if err := c.fetchReceipts(ctx, &extraction, txs, gasUsed); err != nil {
		return empty, err
	}
	// Detect endpoint chain changes across a multi-request snapshot.
	if _, err := c.ChainID(ctx); err != nil {
		return empty, fmt.Errorf("recheck source chain: %w", err)
	}
	extraction.Provider = c.config.Provider
	extraction.ExtractedAt = time.Now().UTC()
	if err := extraction.Validate(); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return extraction, nil
}

// wait waits for a retry or pacing delay with context cancellation; nonpositive delays do not
// start a timer.
func wait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Pacing is shared by all RPC methods and retries. Never hold the gate while
// waiting: responses must be able to extend a global Retry-After cooldown.
func (c *Client) pace(ctx context.Context) error {
	for {
		c.gate.Lock()
		if err := ctx.Err(); err != nil {
			c.gate.Unlock()
			return err
		}
		if c.config.MaxRequests > 0 && c.Requests() >= c.config.MaxRequests {
			c.gate.Unlock()
			return outbound.ErrRPCBudgetExceeded
		}
		delay := time.Until(c.nextRequest)
		if delay <= 0 {
			c.nextRequest = time.Now().Add(c.config.MinRequestInterval)
			c.gate.Unlock()
			return nil
		}
		c.gate.Unlock()
		if err := wait(ctx, delay); err != nil {
			return err
		}
	}
}

// cooldown extends the client's shared next-request time so a rate-limit response delays
// concurrent callers too.
func (c *Client) cooldown(delay time.Duration) {
	if delay <= 0 {
		return
	}
	c.gate.Lock()
	defer c.gate.Unlock()
	until := time.Now().Add(delay)
	if until.After(c.nextRequest) {
		c.nextRequest = until
	}
}

// call performs bounded logical RPC attempts, classifying provider failures and respecting
// context, pacing and Retry-After.
func (c *Client) call(ctx context.Context, method string, params []any) (json.RawMessage, error) {
	id := c.id.Add(1)
	payload, err := json.Marshal(struct {
		Version string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Method  string `json:"method"`
		Params  []any  `json:"params"`
	}{"2.0", id, method, params})
	if err != nil {
		return nil, ErrProtocol
	}
	backoff := c.config.InitialBackoff
	for attempt := 1; ; attempt++ {
		if err := c.pace(ctx); err != nil {
			return nil, err
		}
		attemptContext := ctx
		if attempt > 1 {
			attemptContext = context.WithValue(ctx, rpcRetryKey{}, true)
		}
		raw, retry, retryAfter, err := c.attempt(attemptContext, payload, id)
		if retry {
			c.cooldown(retryAfter)
		}
		if err == nil {
			return raw, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if c.config.MaxRequests > 0 && c.Requests() >= c.config.MaxRequests {
			return nil, errors.Join(err, outbound.ErrRPCBudgetExceeded)
		}
		if !retry || attempt >= c.config.MaxAttempts || retryAfter > c.config.MaxBackoff {
			if retry && retryAfter > 0 {
				err = errors.Join(err, fmt.Errorf("%w: wait at least %s", outbound.ErrRetryDeferred, retryAfter))
			}
			return nil, fmt.Errorf("%s after %d attempt(s): %w", method, attempt, err)
		}
		// Equal jitter: [backoff/2, backoff], capped without duration overflow.
		half := backoff / 2
		delay := half + time.Duration(rand.Int64N(int64(backoff-half)+1))
		if retryAfter > delay {
			delay = retryAfter
		}
		if err := wait(ctx, delay); err != nil {
			return nil, err
		}
		if backoff > c.config.MaxBackoff/2 {
			backoff = c.config.MaxBackoff
		} else {
			backoff *= 2
		}
	}
}

// safeTransportError preserves cancellation/deadline identity while removing endpoint-bearing
// transport error details.
func safeTransportError(ctx context.Context, err error) error {
	if errors.Is(err, outbound.ErrRPCBudgetExceeded) {
		return outbound.ErrRPCBudgetExceeded
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrTransport, context.DeadlineExceeded)
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	// In particular, do not wrap url.Error: it embeds the credential-bearing URL.
	return ErrTransport
}

// attempt encodes one JSON-RPC request, enforces response limits and validates its envelope before
// exposing result bytes.
func (c *Client) attempt(parent context.Context, payload []byte, id uint64) (json.RawMessage, bool, time.Duration, error) {
	ctx, cancel := context.WithTimeout(parent, c.config.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.URL, bytes.NewReader(payload))
	if err != nil {
		return nil, false, 0, ErrConfig
	}
	req.Header.Set("Content-Type", "application/json")
	// Check cancellation before handing off to HTTP. The transport wrapper counts
	// an attempt only when RoundTrip is invoked, including failed transport attempts.
	if err := ctx.Err(); err != nil {
		return nil, false, 0, err
	}
	response, err := c.http.Do(req)
	if err != nil {
		return nil, true, 0, safeTransportError(ctx, err)
	}
	defer response.Body.Close()
	// Classify status before reading an untrusted error body. A stalled or
	// oversized 401/402/403 body must not turn a permanent rejection into retry.
	after := retryDelay(response.Header.Get("Retry-After"), time.Now())
	if response.StatusCode != http.StatusOK {
		retry := response.StatusCode == 408 || response.StatusCode == 429 ||
			response.StatusCode == 500 || response.StatusCode == 502 ||
			response.StatusCode == 503 || response.StatusCode == 504
		err := error(&HTTPError{Status: response.StatusCode})
		if !retry {
			err = errors.Join(outbound.ErrSourcePermanent, err)
		}
		return nil, retry, after, err
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, c.config.MaxResponseBytes+1))
	if int64(len(body)) > c.config.MaxResponseBytes {
		return nil, false, 0, ErrResponseTooLarge
	}
	if err != nil {
		return nil, true, 0, safeTransportError(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, true, 0, err
	}
	envelope, err := uniqueObject(body)
	if err != nil {
		return nil, false, 0, ErrProtocol
	}
	var version string
	var responseID uint64
	if json.Unmarshal(envelope["jsonrpc"], &version) != nil || version != "2.0" ||
		json.Unmarshal(envelope["id"], &responseID) != nil || responseID != id {
		return nil, false, 0, ErrProtocol
	}
	result, hasResult := envelope["result"]
	rpcError, hasError := envelope["error"]
	if hasResult == hasError {
		return nil, false, 0, ErrProtocol
	}
	if hasError {
		e, err := objectOf(rpcError)
		if err != nil {
			return nil, false, 0, ErrProtocol
		}
		var code int
		var message string
		if json.Unmarshal(e["code"], &code) != nil || bytes.Equal(e["code"], []byte("null")) ||
			json.Unmarshal(e["message"], &message) != nil || bytes.Equal(e["message"], []byte("null")) {
			return nil, false, 0, ErrProtocol
		}
		retry := code == 429 || code == -32005
		err = &RPCError{Code: code}
		if !retry {
			err = errors.Join(outbound.ErrSourcePermanent, err)
		}
		return nil, retry, after, err
	}
	if bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
		return nil, false, 0, domain.ErrUnavailable
	}
	return result, false, 0, nil
}

// retryDelay computes the bounded retry delay, including server Retry-After when usable.
func retryDelay(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		if seconds > math.MaxInt64/int64(time.Second) {
			return time.Duration(math.MaxInt64)
		}
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && when.After(now) {
		return when.Sub(now)
	}
	return 0
}

// Requests returns attempted HTTP dispatches, including retries and failures.
// It is process-local, not a provider billing or wire-delivery counter.
func (c *Client) Requests() uint64 { return c.requests.Load() }

// rpcRetryKey marks a subsequent logical-call attempt in context for transport-level retry
// accounting.
type rpcRetryKey struct{}

// reserveRequest atomically consumes one shared HTTP-attempt allowance; exhaustion or cancellation
// prevents dispatch.
func (c *Client) reserveRequest(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := c.requests.Load()
		if c.config.MaxRequests > 0 && n >= c.config.MaxRequests {
			return outbound.ErrRPCBudgetExceeded
		}
		if c.requests.CompareAndSwap(n, n+1) {
			retry, _ := ctx.Value(rpcRetryKey{}).(bool)
			c.metrics.RPCAttempt(retry)
			return nil
		}
	}
}

// attemptTransport is the accounting boundary: canceled requests rejected by
// net/http before RoundTrip consume no budget. Once reserved, dispatch is an
// in-flight attempt and is never refunded, even if cancellation races with it.
// Never hold a client mutex across the underlying HTTP exchange.
type attemptTransport struct {
	client *Client
	next   http.RoundTripper
}

// RoundTrip reserves shared pacing and attempt budget immediately before the underlying HTTP
// transport can dispatch.
func (t attemptTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := t.client.reserveRequest(r.Context()); err != nil {
		return nil, err
	}
	return t.next.RoundTrip(r)
}
