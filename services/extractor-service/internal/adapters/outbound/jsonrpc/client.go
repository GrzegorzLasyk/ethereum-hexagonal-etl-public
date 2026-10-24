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
	"sync/atomic"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

var (
	ErrConfig           = errors.New("invalid RPC configuration")
	ErrProtocol         = errors.New("invalid JSON-RPC response")
	ErrTransport        = errors.New("RPC transport failed")
	ErrResponseTooLarge = errors.New("RPC response size limit exceeded")
	ErrSnapshotTooLarge = errors.New("raw block snapshot size limit exceeded")
)

// Config has no implicit network or resource-budget defaults. MaxAttempts includes
// the first request. URL is secret-bearing and must never be logged.
type Config struct {
	URL                string
	ChainID            domain.ChainID
	Provider           string
	RequestTimeout     time.Duration
	MaxAttempts        int
	InitialBackoff     time.Duration
	MaxBackoff         time.Duration
	MinRequestInterval time.Duration
	MaxResponseBytes   int64
	MaxSnapshotBytes   int64
}

// HTTPError and RPCError expose numeric diagnostics, never provider text.
type HTTPError struct{ Status int }

func (e *HTTPError) Error() string { return fmt.Sprintf("RPC HTTP status %d", e.Status) }

type RPCError struct{ Code int }

func (e *RPCError) Error() string { return fmt.Sprintf("JSON-RPC error code %d", e.Code) }

type Client struct {
	config      Config
	http        *http.Client
	id          atomic.Uint64
	gate        chan struct{}
	nextRequest time.Time // protected by gate
}

var _ outbound.BlockSource = (*Client)(nil)

func New(config Config, client *http.Client) (*Client, error) {
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
	return &Client{config: config, http: &h, gate: make(chan struct{}, 1)}, nil
}

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
	total := int64(len(raw))
	var cumulative uint64
	for i, tx := range txs {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		receiptRaw, err := c.call(ctx, "eth_getTransactionReceipt", []any{fmt.Sprintf("0x%x", tx.meta.Hash)})
		if err != nil {
			return empty, fmt.Errorf("fetch receipt %d: %w", i, err)
		}
		if int64(len(receiptRaw)) > c.config.MaxSnapshotBytes-total {
			return empty, ErrSnapshotTooLarge
		}
		total += int64(len(receiptRaw))
		receipt, used, err := decodeReceipt(receiptRaw, extraction.Header, tx)
		if err != nil {
			return empty, fmt.Errorf("receipt %d: %w", i, err)
		}
		if used > math.MaxUint64-cumulative {
			return empty, invalid("cumulativeGasUsed")
		}
		cumulative += used
		o, _ := objectOf(receiptRaw)
		reported, _ := uintField(o, "cumulativeGasUsed")
		if cumulative != reported {
			return empty, invalid("cumulativeGasUsed")
		}
		extraction.Receipts = append(extraction.Receipts, receipt)
		extraction.RawReceipts = append(extraction.RawReceipts, receiptRaw)
	}
	if cumulative != gasUsed {
		return empty, invalid("gasUsed")
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

func (c *Client) pace(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.gate <- struct{}{}:
	}
	defer func() { <-c.gate }()
	if err := wait(ctx, time.Until(c.nextRequest)); err != nil {
		return err
	}
	c.nextRequest = time.Now().Add(c.config.MinRequestInterval)
	return nil
}

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
		raw, retry, retryAfter, err := c.attempt(ctx, payload, id)
		if err == nil {
			return raw, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !retry || attempt >= c.config.MaxAttempts || retryAfter > c.config.MaxBackoff {
			if retryAfter > 0 {
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

func safeTransportError(ctx context.Context, err error) error {
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

func (c *Client) attempt(parent context.Context, payload []byte, id uint64) (json.RawMessage, bool, time.Duration, error) {
	ctx, cancel := context.WithTimeout(parent, c.config.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.URL, bytes.NewReader(payload))
	if err != nil {
		return nil, false, 0, ErrConfig
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return nil, true, 0, safeTransportError(ctx, err)
	}
	defer response.Body.Close()
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
	after := retryDelay(response.Header.Get("Retry-After"), time.Now())
	if response.StatusCode != http.StatusOK {
		retry := response.StatusCode == 408 || response.StatusCode == 429 ||
			response.StatusCode == 500 || response.StatusCode == 502 ||
			response.StatusCode == 503 || response.StatusCode == 504
		return nil, retry, after, &HTTPError{Status: response.StatusCode}
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
		return nil, code == 429 || code == -32005, after, &RPCError{Code: code}
	}
	if bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
		return nil, false, 0, domain.ErrUnavailable
	}
	return result, false, 0, nil
}

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
