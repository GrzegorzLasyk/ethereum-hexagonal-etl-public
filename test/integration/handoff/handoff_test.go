//go:build postgresintegration

package handoff_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This harness imports only wire contracts. Services run as separate binaries,
// each with exactly one runtime role; assertions use separate role connections.
type child struct {
	cmd     *exec.Cmd
	done    chan error
	output  bytes.Buffer
	stopped bool
}

func launch(t *testing.T, binary string, env []string) *child {
	t.Helper()
	p := &child{cmd: exec.Command(binary), done: make(chan error, 1)}
	p.cmd.Env = env
	p.cmd.Stdout = &p.output
	p.cmd.Stderr = &p.output
	if err := p.cmd.Start(); err != nil {
		t.Fatal("cannot start service", err)
	}
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() { p.stop(t, false) })
	return p
}
func (p *child) stop(t *testing.T, crash bool) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	if crash {
		_ = p.cmd.Process.Kill()
	} else {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case err := <-p.done:
		if !crash && err != nil {
			t.Fatalf("service shutdown failed: %v\n%s", err, p.output.String())
		}
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		t.Fatal("service did not shut down")
	}
	if strings.Contains(p.output.String(), "DATA RACE") {
		t.Fatal("service data race", p.output.String())
	}
}
func await(t *testing.T, label string, predicate func() bool) {
	t.Helper()
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if predicate() {
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-timer.C:
			t.Fatal("timeout:", label)
		case <-tick.C:
		}
	}
}
func reserve(t *testing.T) net.Listener {
	t.Helper()
	l, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	return l
}
func pool(t *testing.T, service string) *pgxpool.Pool {
	t.Helper()
	p, e := pgxpool.New(t.Context(), os.Getenv(strings.ToUpper(service)+"_TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal("database configuration")
	}
	t.Cleanup(p.Close)
	var role string
	if e = p.QueryRow(t.Context(), "SELECT current_user").Scan(&role); e != nil || role != service+"_runtime" {
		t.Fatal("incorrect role", service)
	}
	return p
}
func queryInt(t *testing.T, p *pgxpool.Pool, q string) int {
	t.Helper()
	var n int
	if e := p.QueryRow(t.Context(), q).Scan(&n); e != nil {
		t.Fatal("query failed", e)
	}
	return n
}
func connection(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	opts, e := grpccontract.DefaultLimits().ClientOptions()
	if e != nil {
		t.Fatal(e)
	}
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	c, e := grpc.NewClient("passthrough:///"+address, opts...)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

type rawProxy struct {
	etlv1.UnimplementedTransformerServiceServer
	client     etlv1.TransformerServiceClient
	lost       atomic.Bool
	committed  chan struct{}
	duplicates atomic.Int32
	mu         sync.Mutex
	first      *etlv1.SubmitRawBlockRequest
	changed    atomic.Bool
}

func (p *rawProxy) SubmitRawBlock(c context.Context, r *etlv1.SubmitRawBlockRequest) (*etlv1.SubmitRawBlockResponse, error) {
	p.mu.Lock()
	if p.first == nil {
		p.first = r
	} else if !proto.Equal(p.first, r) {
		p.changed.Store(true)
	}
	p.mu.Unlock()
	ack, e := p.client.SubmitRawBlock(c, r)
	if e != nil {
		return nil, e
	}
	if ack.GetAcceptance().GetStatus() == etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED {
		p.duplicates.Add(1)
	}
	if p.lost.CompareAndSwap(false, true) {
		close(p.committed)
		<-c.Done()
		return nil, status.Error(codes.DeadlineExceeded, "injected lost ACK")
	}
	return ack, nil
}

type transformedProxy struct {
	etlv1.UnimplementedLoaderServiceServer
	client     etlv1.LoaderServiceClient
	lost       atomic.Bool
	committed  chan struct{}
	duplicates atomic.Int32
	mu         sync.Mutex
	first      *etlv1.SubmitTransformedBlockRequest
	changed    atomic.Bool
}

func (p *transformedProxy) SubmitTransformedBlock(c context.Context, r *etlv1.SubmitTransformedBlockRequest) (*etlv1.SubmitTransformedBlockResponse, error) {
	p.mu.Lock()
	if p.first == nil {
		p.first = r
	} else if !proto.Equal(p.first, r) {
		p.changed.Store(true)
	}
	p.mu.Unlock()
	ack, e := p.client.SubmitTransformedBlock(c, r)
	if e != nil {
		return nil, e
	}
	if ack.GetAcceptance().GetStatus() == etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED {
		p.duplicates.Add(1)
	}
	if p.lost.CompareAndSwap(false, true) {
		close(p.committed)
		<-c.Done()
		return nil, status.Error(codes.DeadlineExceeded, "injected lost ACK")
	}
	return ack, nil
}
func serve(t *testing.T, l net.Listener, register func(*grpc.Server)) {
	t.Helper()
	opts, e := grpccontract.DefaultLimits().ServerOptions()
	if e != nil {
		t.Fatal(e)
	}
	s := grpc.NewServer(opts...)
	register(s)
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() { s.Stop(); l.Close(); <-done })
}
func TestHandoff(t *testing.T) {
	if os.Getenv("HANDOFF_TEST_ISOLATED") != "1" {
		t.Skip("run go run ./test/integration/cmd/handoff-postgres")
	}
	real := readRealInput(t)
	root, e := filepath.Abs("../../..")
	if e != nil {
		t.Fatal(e)
	}
	binaries := map[string]string{}
	for _, service := range []string{"extractor", "transformer", "loader"} {
		binary := filepath.Join(t.TempDir(), service)
		cmd := exec.CommandContext(t.Context(), "go", "build", "-race", "-o", binary, "./services/"+service+"-service/cmd/"+service)
		cmd.Dir = root
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("build %s: %v %s", service, e, out)
		}
		binaries[service] = binary
	}
	extracted, transformed, loaded := pool(t, "extractor"), pool(t, "transformer"), pool(t, "loader")
	verificationPools := map[string]*pgxpool.Pool{"extractor": extracted, "transformer": transformed, "loader": loaded}
	target := completionTarget(t, extracted, real)
	verifyPartialCompletion(t, verificationPools, target)
	transformerSlot, loaderSlot := reserve(t), reserve(t)
	transformerAddress, loaderAddress := transformerSlot.Addr().String(), loaderSlot.Addr().String()
	transformerSlot.Close()
	loaderSlot.Close()
	rawListener, transformedListener := reserve(t), reserve(t)
	raw := &rawProxy{client: etlv1.NewTransformerServiceClient(connection(t, transformerAddress)), committed: make(chan struct{})}
	normalized := &transformedProxy{client: etlv1.NewLoaderServiceClient(connection(t, loaderAddress)), committed: make(chan struct{})}
	serve(t, rawListener, func(s *grpc.Server) { etlv1.RegisterTransformerServiceServer(s, raw) })
	serve(t, transformedListener, func(s *grpc.Server) { etlv1.RegisterLoaderServiceServer(s, normalized) })
	environment := func(service string) []string {
		var env []string
		for _, entry := range os.Environ() {
			name, _, _ := strings.Cut(entry, "=")
			if strings.HasPrefix(name, "EXTRACTOR_") || strings.HasPrefix(name, "TRANSFORMER_") || strings.HasPrefix(name, "LOADER_") || strings.HasPrefix(name, "HANDOFF_") || strings.HasPrefix(name, "PG") || strings.HasPrefix(name, "ETHEREUM_") || strings.HasPrefix(name, "REAL_FIXTURE_") || name == "LOG_LEVEL" {
				continue
			}
			env = append(env, entry)
		}
		prefix := strings.ToUpper(service)
		env = append(env, prefix+"_DATABASE_URL="+os.Getenv(prefix+"_TEST_DATABASE_URL"), prefix+"_CHAIN_ID=1", "LOG_LEVEL=INFO")
		if service != "extractor" {
			address := loaderAddress
			if service == "transformer" {
				address = transformerAddress
			}
			env = append(env, prefix+"_GRPC_ADDRESS="+address, prefix+"_POLL_INTERVAL=20ms", prefix+"_WORKERS=2")
		}
		if service != "loader" {
			target := rawListener.Addr().String()
			if service == "transformer" {
				target = transformedListener.Addr().String()
			}
			env = append(env, prefix+"_DISPATCH_TARGET="+target, prefix+"_DISPATCH_WORKERS=2", prefix+"_DISPATCH_MAX_ATTEMPTS=100", prefix+"_DISPATCH_POLL_INTERVAL=20ms", prefix+"_DISPATCH_RPC_TIMEOUT=1s", prefix+"_DISPATCH_TIMEOUT=2s", prefix+"_DISPATCH_LEASE=3s", prefix+"_DISPATCH_INITIAL_BACKOFF=50ms", prefix+"_DISPATCH_MAX_BACKOFF=200ms")
		}
		return env
	}
	ex := launch(t, binaries["extractor"], environment("extractor"))
	await(t, "extractor retries while transformer offline", func() bool {
		return queryInt(t, extracted, "SELECT count(*) FROM extractor.outbox_events WHERE attempts>0 AND status='pending' AND last_error_code='unavailable'") == 1
	})
	tr := launch(t, binaries["transformer"], environment("transformer"))
	await(t, "transformer committed before lost raw ACK", func() bool {
		select {
		case <-raw.committed:
			return true
		default:
			return false
		}
	})
	if queryInt(t, transformed, "SELECT count(*) FROM transformer.inbox_events") != 1 || queryInt(t, extracted, "SELECT count(*) FROM extractor.outbox_events WHERE status='sending'") != 1 {
		t.Fatal("raw ACK durability boundary")
	}
	ex.stop(t, true)
	ex = launch(t, binaries["extractor"], environment("extractor"))
	await(t, "raw lease recovery and duplicate ACK", func() bool {
		return queryInt(t, extracted, "SELECT count(*) FROM extractor.outbox_events WHERE status='delivered'") == 1
	})
	await(t, "transformer retries while loader offline", func() bool {
		return queryInt(t, transformed, "SELECT count(*) FROM transformer.outbox_events WHERE attempts>0 AND status='pending' AND last_error_code='unavailable'") == 1
	})
	lo := launch(t, binaries["loader"], environment("loader"))
	await(t, "loader committed before lost transformed ACK", func() bool {
		select {
		case <-normalized.committed:
			return true
		default:
			return false
		}
	})
	if queryInt(t, loaded, "SELECT count(*) FROM loader.inbox_events") != 1 || queryInt(t, transformed, "SELECT count(*) FROM transformer.outbox_events WHERE status='sending'") != 1 {
		t.Fatal("transformed ACK durability boundary")
	}
	tr.stop(t, true)
	tr = launch(t, binaries["transformer"], environment("transformer"))
	await(t, "transformed lease recovery and duplicate ACK", func() bool {
		return queryInt(t, transformed, "SELECT count(*) FROM transformer.outbox_events WHERE status='delivered'") == 1
	})
	await(t, "normalized load completion", func() bool {
		return queryInt(t, loaded, "SELECT count(*) FROM loader.inbox_events WHERE status='completed'") == 1
	})
	counts := map[string]int{"blocks": 1, "transactions": 2, "receipts": 2, "logs": 1}
	if real != nil {
		counts["transactions"] = len(real.transactions)
		counts["receipts"] = len(real.receipts)
		counts["logs"] = len(real.logs)
	}
	for table, want := range counts {
		if got := queryInt(t, loaded, "SELECT count(*) FROM loader."+table); got != want {
			t.Fatalf("%s=%d want=%d", table, got, want)
		}
	}
	if queryInt(t, loaded, "SELECT count(*) FROM loader.blocks WHERE canonical_status='unknown' AND NOT height_conflict") != 1 {
		t.Fatal("canonicality changed")
	}
	if raw.duplicates.Load() < 1 || normalized.duplicates.Load() < 1 || raw.changed.Load() || normalized.changed.Load() {
		t.Fatal("duplicate delivery changed event or was not observed")
	}
	if real != nil {
		assertRealPipeline(t, real, extracted, transformed, loaded, raw, normalized)
	}
	// Verify role isolation from the same credentials used by the processes.
	for _, check := range []struct {
		p       *pgxpool.Pool
		foreign string
	}{{extracted, "transformer.inbox_events"}, {transformed, "loader.inbox_events"}, {loaded, "extractor.outbox_events"}} {
		if _, e := check.p.Exec(t.Context(), "SELECT * FROM "+check.foreign); e == nil {
			t.Fatal("cross-service access allowed")
		}
	}
	ex.stop(t, false)
	tr.stop(t, false)
	lo.stop(t, false)
	verifyCompleteAndFaults(t, verificationPools, target)
	t.Log(fmt.Sprintf("durable handoff completed; raw duplicate ACKs=%d, transformed duplicate ACKs=%d", raw.duplicates.Load(), normalized.duplicates.Load()))
}
