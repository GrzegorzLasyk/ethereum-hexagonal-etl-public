package grpccontract

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/metrics"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Resources bounds admitted serialized messages, not the Go heap. Protobuf,
// SQL and normalization can hold additional copies; container limits are separate.
type Resources struct{ Incoming, InFlightBytes, Connections, PoolConnections int }

func DefaultResources(l Limits) Resources { return Resources{2, 2 * l.MessageBytes, 16, 12} }
func (r Resources) Validate(l Limits) error {
	if err := l.Validate(); err != nil {
		return err
	}
	if r.Incoming < 1 || r.Incoming > 8 || r.InFlightBytes < l.MessageBytes || r.InFlightBytes > 520<<20 || r.Connections < 1 || r.Connections > 64 || r.PoolConnections < 2 || r.PoolConnections > 32 {
		return errors.New("invalid resource limits")
	}
	return nil
}
func LoadResources(prefix string, l Limits) (Resources, error) {
	r := DefaultResources(l)
	for _, v := range []struct {
		name string
		p    *int
	}{{"MAX_INCOMING_REQUESTS", &r.Incoming}, {"MAX_INFLIGHT_BYTES", &r.InFlightBytes}, {"MAX_GRPC_CONNECTIONS", &r.Connections}, {"DB_MAX_CONNECTIONS", &r.PoolConnections}} {
		if raw := os.Getenv(prefix + "_" + v.name); raw != "" {
			n, e := strconv.Atoi(raw)
			if e != nil {
				return r, fmt.Errorf("invalid %s_%s", prefix, v.name)
			}
			*v.p = n
		}
	}
	return r, r.Validate(l)
}
func Overloaded() error {
	s, _ := status.New(codes.Unavailable, "receiver overloaded; retry with backoff").WithDetails(&errdetails.ErrorInfo{Reason: "RECEIVER_OVERLOADED", Domain: "ethereum-etl"})
	return s.Err()
}
func IsOverloaded(err error) bool {
	if status.Code(err) != codes.Unavailable {
		return false
	}
	for _, detail := range status.Convert(err).Details() {
		if e, ok := detail.(*errdetails.ErrorInfo); ok && e.Reason == "RECEIVER_OVERLOADED" && e.Domain == "ethereum-etl" {
			return true
		}
	}
	return false
}

type Admission struct {
	slots       chan struct{}
	connections chan struct{}
	bytes       int
	registry    *metrics.Registry
}

func NewAdmission(r Resources, l Limits, registry *metrics.Registry) (*Admission, error) {
	if err := r.Validate(l); err != nil {
		return nil, err
	}
	slots := min(r.Incoming, r.InFlightBytes/l.MessageBytes)
	a := &Admission{make(chan struct{}, slots), make(chan struct{}, r.Connections), l.MessageBytes, registry}
	registry.ConfigureAdmission(slots, l.MessageBytes, r.InFlightBytes)
	return a, nil
}
func (a *Admission) enter() (func(), error) {
	select {
	case a.slots <- struct{}{}:
		a.registry.AdmissionActive(1)
		return func() { <-a.slots; a.registry.AdmissionActive(-1) }, nil
	default:
		a.registry.AdmissionRejected("transport")
		return nil, Overloaded()
	}
}
func (a *Admission) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.MaxConcurrentStreams(uint32(cap(a.slots))), grpc.MaxHeaderListSize(16 << 10), grpc.ConnectionTimeout(5 * time.Second)}
}

// Wrap the method handler BEFORE its decoder, not a unary interceptor (which
// runs after decoding). Clone descriptors; generated contracts remain untouched.
// The slot is released only after handler return, even if context is canceled.
func (a *Admission) Register(server *grpc.Server, desc *grpc.ServiceDesc, implementation any) {
	copy := *desc
	copy.Methods = append([]grpc.MethodDesc(nil), desc.Methods...)
	for i := range copy.Methods {
		handler := copy.Methods[i].Handler
		copy.Methods[i].Handler = a.wrap(handler)
	}
	server.RegisterService(&copy, implementation)
}

func (a *Admission) wrap(handler grpc.MethodHandler) grpc.MethodHandler {
	return func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		release, err := a.enter()
		if err != nil {
			return nil, err
		}
		defer release()
		response, err := handler(srv, ctx, decode, interceptor)
		if IsOverloaded(err) {
			a.registry.AdmissionRejected("durable")
		}
		return response, err
	}
}

type admissionListener struct {
	net.Listener
	a *Admission
}
type admissionConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *admissionConn) Close() error                          { err := c.Conn.Close(); c.once.Do(c.release); return err }
func (a *Admission) LimitListener(l net.Listener) net.Listener { return &admissionListener{l, a} }
func (l *admissionListener) Accept() (net.Conn, error) {
	for {
		c, e := l.Listener.Accept()
		if e != nil {
			return nil, e
		}
		select {
		case l.a.connections <- struct{}{}:
			return &admissionConn{Conn: c, release: func() { <-l.a.connections }}, nil
		default:
			l.a.registry.AdmissionRejected("connections")
			_ = c.Close()
		}
	}
}
