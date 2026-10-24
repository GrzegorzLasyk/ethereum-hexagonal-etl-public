// Command fixture-e2e runs Stage 11 tests in a disposable Compose project.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const composeFile = "deployments/compose/compose.yaml"

var passwordNames = []string{
	"POSTGRES_PASSWORD", "EXTRACTOR_MIGRATOR_PASSWORD", "EXTRACTOR_RUNTIME_PASSWORD",
	"TRANSFORMER_MIGRATOR_PASSWORD", "TRANSFORMER_RUNTIME_PASSWORD",
	"LOADER_MIGRATOR_PASSWORD", "LOADER_RUNTIME_PASSWORD",
}

type command struct {
	phase, program, root string
	args, env, secrets   []string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func start(ctx context.Context) error {
	if len(os.Args) == 3 && os.Args[1] == "--init" {
		return initialize(os.Args[2])
	}
	if len(os.Args) != 1 {
		return errors.New("usage: fixture-e2e [--init PATH] (run from repository root)")
	}
	root, err := os.Getwd()
	if err != nil {
		return errors.New("cannot locate repository root")
	}
	if _, err := os.Stat(filepath.Join(root, composeFile)); err != nil {
		return errors.New("run fixture-e2e from the repository root")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return errors.New("cannot select a loopback test port")
	}
	port := listener.Addr().(*net.TCPAddr).Port
	// Keep the first reservation open until the second is chosen, so the OS
	// cannot assign both published services the same ephemeral port.
	defer listener.Close()
	fixtureListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return errors.New("cannot select fixture test port")
	}
	fixturePort := fixtureListener.Addr().(*net.TCPAddr).Port
	if err := fixtureListener.Close(); err != nil {
		return errors.New("cannot release fixture test port")
	}
	if err := listener.Close(); err != nil {
		return errors.New("cannot release test port")
	}
	return run(ctx, root, os.Environ(), port, fixturePort, execute)
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", errors.New("cannot generate test credentials")
	}
	return hex.EncodeToString(b), nil
}

// Preserve toolchain/Docker connection settings but not inherited Compose,
// PostgreSQL or test database settings. Explicit -f, -p and --env-file select
// this run's resources; the user's .env is never sourced.
func isolatedEnvironment(parent []string) []string {
	env := make([]string, 0, len(parent))
	for _, entry := range parent {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "COMPOSE_") || strings.HasPrefix(name, "PG") ||
			strings.HasPrefix(name, "POSTGRES_") || strings.HasPrefix(name, "LOADER_") || strings.HasPrefix(name, "TRANSFORMER_") || strings.HasPrefix(name, "EXTRACTOR_") || strings.HasPrefix(name, "HANDOFF_") || strings.HasPrefix(name, "FIXTURE_") {
			continue
		}
		secret := false
		for _, password := range passwordNames {
			if name == password {
				secret = true
				break
			}
		}
		if !secret {
			env = append(env, entry)
		}
	}
	return env
}

func run(ctx context.Context, root string, parent []string, port, fixturePort int, invoke func(context.Context, command) error) (result error) {
	id, err := randomHex(12)
	if err != nil {
		return err
	}
	project := "ethereum-stage11-" + id
	passwords := make(map[string]string, len(passwordNames))
	var secrets []string
	var content strings.Builder
	for _, name := range passwordNames {
		password, err := randomHex(24)
		if err != nil {
			return err
		}
		passwords[name] = password
		secrets = append(secrets, password)
		fmt.Fprintf(&content, "%s=%s\n", name, password)
	}
	fmt.Fprintf(&content, "POSTGRES_PORT=%d\nFIXTURE_PORT=%d\nFIXTURE_HEAD=100\n", port, fixturePort)
	file, err := os.CreateTemp("", "ethereum-stage11-*.env")
	if err != nil {
		return errors.New("cannot create private test configuration")
	}
	defer func() {
		if err := os.Remove(file.Name()); err != nil {
			result = errors.Join(result, errors.New("cannot remove temporary credential file"))
		}
	}()
	// Explicit chmod also ensures exactly 0600 under a restrictive umask.
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		return errors.New("cannot protect temporary credentials")
	}
	_, writeErr := io.WriteString(file, content.String())
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return errors.New("cannot write private test configuration")
	}
	env := isolatedEnvironment(parent)
	base := []string{"compose", "--env-file", file.Name(), "-p", project, "-f", filepath.Join(root, composeFile), "-f", filepath.Join(root, "deployments/compose/fixture.yaml"), "--profile", "fixture"}
	docker := func(phase string, args ...string) command {
		return command{phase: phase, program: "docker", root: root, args: append(append([]string{}, base...), args...), env: env, secrets: secrets}
	}
	defer func() {
		// Cleanup is independent of the failed/cancelled operation. Only this
		// newly generated project and its volume are eligible for deletion.
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := invoke(cleanup, docker("cleanup", "down", "--volumes", "--remove-orphans")); err != nil {
			result = errors.Join(result, fmt.Errorf("cleanup failed for project %s: %w", project, err))
		}
	}()
	steps := []struct {
		cmd     command
		timeout time.Duration
	}{
		{docker("validate Compose", "config", "--quiet"), 30 * time.Second},
		{docker("build fixture images", "build", "fixture"), 10 * time.Minute},
		{docker("start PostgreSQL", "up", "-d", "--wait", "--wait-timeout", "120", "postgres"), 3 * time.Minute},
		{docker("verify migrations and permissions", "run", "--rm", "verify"), 3 * time.Minute},
		{docker("start complete fixture", "up", "-d", "--wait", "--wait-timeout", "120", "fixture", "extractor", "transformer", "loader"), 3 * time.Minute},
	}
	for _, step := range steps {
		limited, cancel := context.WithTimeout(ctx, step.timeout)
		err := invoke(limited, step.cmd)
		cancel()
		if err != nil {
			return err
		}
	}
	testEnv := append([]string{}, env...)
	testEnv = append(testEnv, "FIXTURE_E2E_ISOLATED=1", "FIXTURE_E2E_PROJECT="+project, "FIXTURE_E2E_ENV_FILE="+file.Name(), fmt.Sprintf("FIXTURE_E2E_URL=http://127.0.0.1:%d", fixturePort))
	for _, service := range []string{"EXTRACTOR", "TRANSFORMER", "LOADER"} {
		url := fmt.Sprintf("postgres://%s_runtime:%s@127.0.0.1:%d/ethereum_etl?sslmode=disable", strings.ToLower(service), passwords[service+"_RUNTIME_PASSWORD"], port)
		testEnv = append(testEnv, service+"_TEST_DATABASE_URL="+url)
		secrets = append([]string{url}, secrets...)
	}
	testCtx, cancel := context.WithTimeout(ctx, 7*time.Minute)
	defer cancel()
	return invoke(testCtx, command{phase: "full Compose fixture E2E", program: "go", root: root, env: testEnv, secrets: secrets, args: []string{"test", "-tags", "composeintegration", "-race", "-run", "^TestFixtureE2E$", "-count=1", "-timeout=6m", "-v", "./test/integration/fixture"}})
}

// Buffer before redaction so credentials split across writes cannot leak.
// If output exceeds the bound, discard it ALL rather than expose a secret
// fragment at a truncation boundary. Command exit status is always reported.
type outputBuffer struct {
	buffer   bytes.Buffer
	overflow bool
}

func (b *outputBuffer) Write(p []byte) (int, error) {
	if b.overflow {
		return len(p), nil
	}
	if b.buffer.Len()+len(p) > 4<<20 {
		b.buffer.Reset()
		b.overflow = true
		return len(p), nil
	}
	return b.buffer.Write(p)
}

func (b *outputBuffer) safe(secrets []string) string {
	if b.overflow {
		return "command output omitted: exceeded 4 MiB\n"
	}
	s := b.buffer.String()
	for _, secret := range secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[REDACTED]")
		}
	}
	return s
}

func execute(ctx context.Context, c command) error {
	fmt.Fprintln(os.Stdout, c.phase)
	cmd := exec.CommandContext(ctx, c.program, c.args...)
	cmd.Dir, cmd.Env = c.root, c.env
	cmd.WaitDelay = 5 * time.Second
	var output outputBuffer
	cmd.Stdout, cmd.Stderr = &output, &output
	err := cmd.Run()
	fmt.Fprint(os.Stdout, output.safe(c.secrets))
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return fmt.Errorf("%s: %w", c.phase, ctx.Err())
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return errors.New(c.phase + ": exit status " + strconv.Itoa(exit.ExitCode()))
	}
	// Do not print exec errors, command arguments, environments or DSNs.
	return errors.New(c.phase + ": could not execute command")
}

// initialize never overwrites an existing developer configuration.
func initialize(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("cannot create environment file; destination must not exist")
	}
	defer file.Close()
	var content strings.Builder
	for _, name := range passwordNames {
		value, err := randomHex(24)
		if err != nil {
			return err
		}
		fmt.Fprintf(&content, "%s=%s\n", name, value)
	}
	content.WriteString("POSTGRES_PORT=5432\nFIXTURE_PORT=18545\nFIXTURE_HEAD=104\n")
	if _, err := io.WriteString(file, content.String()); err != nil {
		return errors.New("cannot write environment file")
	}
	if err := file.Close(); err != nil {
		return errors.New("cannot close environment file")
	}
	return nil
}
