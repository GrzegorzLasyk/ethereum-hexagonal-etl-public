// mainnet.go implements manifest-verified authentic offline fixture runner configuration in the
// offline test and diagnostic infrastructure.

package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

// realCase pins one local authentic artifact and manifest identity for an isolated noncontiguous-
// height run.
type realCase struct{ id, path, manifest, digest string }

// Explicit local mode; ordinary unit tests/CI never read private artifacts.
// Verify ALL inputs before starting any PostgreSQL project. Every height gets
// fresh roles/storage/checkpoints; retained resources are never removed here.
func startReal(ctx context.Context, directory string) error {
	root, err := os.Getwd()
	if err != nil {
		return errors.New("cannot locate repository")
	}
	directory, err = filepath.Abs(directory)
	if err != nil {
		return errors.New("invalid artifact directory")
	}
	manifest := filepath.Join(root, "test/fixtures/mainnet/v1/manifest.json")
	var cases []realCase
	for _, selected := range []struct{ id, filename string }{
		{"historical-smoke", "1000000.json"}, {"pectra-blob-dynamic-fee", "22431084.json"}, {"london-access-list", "12965000.json"},
	} {
		path := filepath.Join(directory, selected.filename)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() > 96<<20 {
			return errors.New("required local fixture missing, nonregular or oversized")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return errors.New("cannot read required fixture")
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(data))
		limited, cancel := context.WithTimeout(ctx, 2*time.Minute)
		err = execute(limited, command{phase: "verify local artifact " + selected.filename, program: "go", root: root, env: isolatedEnvironment(os.Environ()), args: []string{"run", "./services/extractor-service/cmd/fixture-capture", "verify", manifest, selected.id, path}})
		cancel()
		if err != nil {
			return err
		}
		cases = append(cases, realCase{selected.id, path, manifest, digest})
	}
	for _, fixture := range cases {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return errors.New("cannot select local PostgreSQL port")
		}
		port := listener.Addr().(*net.TCPAddr).Port
		if listener.Close() != nil {
			return errors.New("cannot release local port")
		}
		fmt.Println("offline Mainnet regression:", fixture.id)
		if err := runCase(ctx, root, os.Environ(), port, execute, &fixture); err != nil {
			return err
		}
	}
	return nil
}
