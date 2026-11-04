// admission_test.go exercises bounded receiver resources and reservation release in the shared
// technical support layer, outside service domains.

package grpccontract

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestAdmissionConcurrentBoundAndCancellation contends for request reservations and verifies
// capacity is released on cancellation instead of leaking slots.
func TestAdmissionConcurrentBoundAndCancellation(t *testing.T) {
	l := DefaultLimits()
	r := DefaultResources(l)
	r.Incoming = 8
	r.InFlightBytes = 2 * l.MessageBytes
	a, e := NewAdmission(r, l, nil)
	if e != nil {
		t.Fatal(e)
	}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	hold := make(chan struct{})
	ready := make(chan struct{}, 32)
	for range 32 {
		wg.Go(func() {
			release, e := a.enter()
			if e == nil {
				accepted.Add(1)
			} else if !IsOverloaded(e) {
				t.Error("nonretryable overload")
			}
			ready <- struct{}{}
			if e == nil {
				<-hold
				release()
			}
		})
	}
	for range 32 {
		<-ready
	}
	if accepted.Load() != 2 {
		t.Fatal("reservation bound violated", accepted.Load())
	}
	close(hold)
	wg.Wait()
	release, e := a.enter()
	if e != nil {
		t.Fatal("capacity leaked")
	}
	release()
}

// TestAdmissionBeforeDecodeAndAfterHandler checks reservation lifetime around the method handler.
//
// Exercise the wrapped MethodDesc directly: overloaded calls must not invoke
// decoder, and cancellation must not release a slot while a handler still runs.
func TestAdmissionBeforeDecodeAndAfterHandler(t *testing.T) {
	l := DefaultLimits()
	r := DefaultResources(l)
	r.Incoming = 1
	a, _ := NewAdmission(r, l, nil)
	entered, finish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	handler := a.wrap(func(_ any, ctx context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		if e := decode(nil); e != nil {
			return nil, e
		}
		close(entered)
		<-finish
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	go func() { defer close(done); _, _ = handler(nil, ctx, func(any) error { return nil }, nil) }()
	<-entered
	cancel()
	var decoded bool
	_, err := handler(nil, t.Context(), func(any) error { decoded = true; return nil }, nil)
	if decoded || !IsOverloaded(err) {
		t.Fatal("overload decoded payload or cancellation released active handler")
	}
	close(finish)
	<-done
	bad := a.wrap(func(_ any, _ context.Context, d func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		return nil, d(nil)
	})
	if _, err := bad(nil, t.Context(), func(any) error { return errors.New("decode failed") }, nil); err == nil {
		t.Fatal("missing decode failure")
	}
	release, err := a.enter()
	if err != nil {
		t.Fatal("decode failure leaked capacity")
	}
	release()
	if status.Code(Overloaded()) != codes.Unavailable || IsOverloaded(status.Error(codes.ResourceExhausted, "size")) {
		t.Fatal("overload classification")
	}
}

// TestResourceConfiguration checks supported resource ranges and incompatible message/reservation
// combinations before startup.
func TestResourceConfiguration(t *testing.T) {
	l := DefaultLimits()
	for _, key := range []string{"MAX_INCOMING_REQUESTS", "MAX_INFLIGHT_BYTES", "MAX_GRPC_CONNECTIONS", "DB_MAX_CONNECTIONS"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv("TEST_"+key, "0")
			if _, e := LoadResources("TEST", l); e == nil {
				t.Fatal("unbounded resource setting")
			}
		})
	}
	r := DefaultResources(l)
	r.InFlightBytes = l.MessageBytes - 1
	if r.Validate(l) == nil {
		t.Fatal("cannot admit even one message")
	}
}

// fakeAdmissionListener returns controlled sockets to exercise connection reservation/release.
type fakeAdmissionListener struct{ conns []net.Conn }

// Accept returns a controlled socket so the connection reservation/release boundary can be
// exercised.
func (l *fakeAdmissionListener) Accept() (net.Conn, error) {
	if len(l.conns) == 0 {
		return nil, io.EOF
	}
	c := l.conns[0]
	l.conns = l.conns[1:]
	return c, nil
}

// Close implements test-owned cleanup and records/releases the controlled resource when
// applicable.
func (*fakeAdmissionListener) Close() error { return nil }

// Addr supplies a synthetic listener address required by the net.Listener test double.
func (*fakeAdmissionListener) Addr() net.Addr { return &net.TCPAddr{} }

// TestConnectionBudgetReleasedExactlyOnce closes a wrapped connection repeatedly and verifies the
// reservation is returned only once.
func TestConnectionBudgetReleasedExactlyOnce(t *testing.T) {
	limits := DefaultLimits()
	resources := DefaultResources(limits)
	resources.Connections = 1
	gate, _ := NewAdmission(resources, limits, nil)
	first, peer1 := net.Pipe()
	second, peer2 := net.Pipe()
	defer peer1.Close()
	defer peer2.Close()
	listener := gate.LimitListener(&fakeAdmissionListener{[]net.Conn{first, second}})
	conn, e := listener.Accept()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = listener.Accept(); e != io.EOF {
		t.Fatal("connection beyond budget accepted")
	}
	if _, e = peer2.Read(make([]byte, 1)); e != io.EOF {
		t.Fatal("excess connection not closed")
	}
	_ = conn.Close()
	_ = conn.Close()
	third, peer3 := net.Pipe()
	defer peer3.Close()
	next, e := gate.LimitListener(&fakeAdmissionListener{[]net.Conn{third}}).Accept()
	if e != nil {
		t.Fatal("connection budget leaked")
	}
	_ = next.Close()
}
