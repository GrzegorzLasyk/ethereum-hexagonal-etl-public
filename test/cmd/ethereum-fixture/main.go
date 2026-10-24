package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/test/fixture"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(ctx context.Context) error {
	head := uint64(104)
	if value := os.Getenv("FIXTURE_HEAD"); value != "" {
		var err error
		head, err = strconv.ParseUint(value, 10, 64)
		if err != nil || head < fixture.Start || head > fixture.Last {
			return errors.New("invalid FIXTURE_HEAD")
		}
	}
	f := fixture.New(head)
	clientOptions, err := grpccontract.DefaultLimits().ClientOptions()
	if err != nil {
		return err
	}
	clientOptions = append(clientOptions, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	raw, err := grpc.NewClient("passthrough:///127.0.0.1:50051", clientOptions...)
	if err != nil {
		return err
	}
	defer raw.Close()
	transformed, err := grpc.NewClient("passthrough:///127.0.0.1:50052", clientOptions...)
	if err != nil {
		return err
	}
	defer transformed.Close()
	serverOptions, err := grpccontract.DefaultLimits().ServerOptions()
	if err != nil {
		return err
	}
	for _, rawSide := range []bool{true, false} {
		address := "127.0.0.1:51052"
		if rawSide {
			address = "127.0.0.1:51051"
		}
		l, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}
		s := grpc.NewServer(serverOptions...)
		if rawSide {
			etlv1.RegisterTransformerServiceServer(s, &fixture.RawProxy{Fixture: f, Client: etlv1.NewTransformerServiceClient(raw)})
		} else {
			etlv1.RegisterLoaderServiceServer(s, &fixture.TransformedProxy{Fixture: f, Client: etlv1.NewLoaderServiceClient(transformed)})
		}
		go func() { _ = s.Serve(l) }()
		defer s.Stop()
	}
	l, err := net.Listen("tcp", ":8545")
	if err != nil {
		return err
	}
	s := &http.Server{Handler: f.Handler(), ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	select {
	case <-ctx.Done():
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Shutdown(shutdown)
}
