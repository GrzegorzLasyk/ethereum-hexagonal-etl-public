// main_test.go exercises command startup, explicit adapter wiring and bounded resource lifetime in
// the offline test and diagnostic infrastructure.

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

// argAfter returns a required child-command argument for assertions and fails if the expected flag
// is absent.
func argAfter(t *testing.T, args []string, key string) string {
	t.Helper()
	for i, arg := range args {
		if arg == key && i+1 < len(args) {
			return args[i+1]
		}
	}
	t.Fatalf("missing argument %s", key)
	return ""
}

// TestLifecycle inspects isolated runner command order, bounded startup and owned-resource cleanup
// with fake executors.
func TestLifecycle(t *testing.T) {
	phases := []string{"validate Compose", "start PostgreSQL", "verify migrations and permissions", "seed deterministic extraction", "end-to-end handoff tests", "cleanup"}
	for _, failAt := range append([]string{""}, phases...) {
		t.Run("failure_at_"+failAt, func(t *testing.T) {
			var seen []string
			var filename, project string
			failure := errors.New("injected failure")
			root := t.TempDir()
			parent := []string{"PATH=/toolchain", "GOCACHE=/cache", "DOCKER_CONTEXT=local",
				"POSTGRES_PASSWORD=inherited", "POSTGRES_PORT=5432", "EXTRACTOR_RUNTIME_PASSWORD=inherited",
				"COMPOSE_PROJECT_NAME=existing", "COMPOSE_FILE=other.yml", "COMPOSE_ENV_FILES=private.env",
				"PGHOST=production", "PGPASSWORD=inherited", "LOADER_TEST_DATABASE_URL=production"}
			invoke := func(ctx context.Context, c command) error {
				seen = append(seen, c.phase)
				if c.root != root {
					t.Fatal("incorrect working directory")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("unbounded command")
				}
				if c.program == "docker" {
					path := argAfter(t, c.args, "--env-file")
					name := argAfter(t, c.args, "-p")
					if filename == "" {
						filename, project = path, name
					}
					if filename != path || project != name || !strings.HasPrefix(name, "ethereum-stage9-") {
						t.Fatal("project changed or not isolated")
					}
					info, err := os.Stat(path)
					if err != nil || info.Mode().Perm() != 0600 {
						t.Fatal("credential file is not mode 0600")
					}
					body, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					credentials := map[string]bool{}
					for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
						key, value, _ := strings.Cut(line, "=")
						if key == "POSTGRES_PORT" {
							if value != "54321" {
								t.Fatal("wrong port")
							}
							continue
						}
						if len(value) != 48 || credentials[value] {
							t.Fatal("credentials missing or reused")
						}
						credentials[value] = true
						if strings.Contains(strings.Join(c.args, " "), value) {
							t.Fatal("secret in command arguments")
						}
					}
					if len(credentials) != 7 {
						t.Fatal("missing role credentials")
					}
					if c.phase == "cleanup" && !reflect.DeepEqual(c.args[len(c.args)-3:], []string{"down", "--volumes", "--remove-orphans"}) {
						t.Fatal("incorrect cleanup")
					}
				} else {
					if c.program != "go" || !strings.Contains(strings.Join(c.args, " "), "-tags postgresintegration -race") {
						t.Fatal("integration test/race mode missing")
					}
					for _, key := range []string{"LOADER_TEST_DATABASE_URL=", "LOADER_TEST_MIGRATOR_URL="} {
						found := false
						for _, v := range c.env {
							if strings.HasPrefix(v, key) && strings.Contains(v, "@127.0.0.1:54321/ethereum_etl?") {
								found = true
							}
						}
						if !found {
							t.Fatal("missing isolated test connection")
						}
					}
				}
				for _, entry := range c.env {
					if strings.Contains(entry, "inherited") || strings.Contains(entry, "production") || strings.HasPrefix(entry, "COMPOSE_") {
						t.Fatal("inherited database or compose setting")
					}
				}
				if c.phase == failAt {
					return failure
				}
				return nil
			}
			err := run(context.Background(), root, parent, 54321, invoke)
			if (failAt == "") != (err == nil) || failAt != "" && !errors.Is(err, failure) {
				t.Fatalf("unexpected result: %v", err)
			}
			if seen[len(seen)-1] != "cleanup" {
				t.Fatal("cleanup was skipped")
			}
			want := phases
			for i, phase := range phases[:5] {
				if phase == failAt {
					want = append(append([]string{}, phases[:i+1]...), "cleanup")
					break
				}
			}
			if !reflect.DeepEqual(seen, want) {
				t.Fatalf("phases=%v want=%v", seen, want)
			}
			if _, err := os.Stat(filename); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("credential file was not removed")
			}
		})
	}
}

// TestCancellationAndCleanupError checks bounded runner cancellation and surfaces owned-resource
// cleanup failure rather than reporting a false PASS.
func TestCancellationAndCleanupError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cleanupErr := errors.New("cleanup unavailable")
	var filename string
	err := run(ctx, t.TempDir(), nil, 54321, func(ctx context.Context, c command) error {
		filename = argAfter(t, c.args, "--env-file")
		if c.phase == "cleanup" {
			if ctx.Err() != nil {
				t.Fatal("cleanup inherited cancellation")
			}
			return cleanupErr
		}
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cleanupErr) {
		t.Fatal("failure or cleanup error was lost")
	}
	if _, err := os.Stat(filename); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cleanup failure leaked secret file")
	}
}

// TestUniqueProjects requires generated test project identities to differ so concurrent runs
// cannot share database storage.
func TestUniqueProjects(t *testing.T) {
	projects := map[string]bool{}
	for range 2 {
		var project string
		err := run(context.Background(), t.TempDir(), nil, 54321, func(_ context.Context, c command) error {
			if c.program == "docker" {
				project = argAfter(t, c.args, "-p")
			}
			return nil
		})
		if err != nil || projects[project] {
			t.Fatal("project name was reused")
		}
		projects[project] = true
	}
}

// TestOutputRedactionAndBounds checks generated credentials and excessive child output cannot
// escape runner diagnostic limits.
func TestOutputRedactionAndBounds(t *testing.T) {
	var b outputBuffer
	for _, chunk := range []string{"before private-", "secret after"} {
		if _, err := b.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if got := b.safe([]string{"private-secret"}); got != "before [REDACTED] after" {
		t.Fatal("split secret not redacted")
	}
	if _, err := b.Write(make([]byte, 4<<20)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write([]byte("private-secret")); err != nil {
		t.Fatal(err)
	}
	if got := b.safe(nil); got != "command output omitted: exceeded 4 MiB\n" {
		t.Fatal("overflow exposed partial output")
	}
}

// TestPreserveToolSettings checks runner environment filtering removes application/database
// overrides but retains necessary toolchain/Docker settings.
func TestPreserveToolSettings(t *testing.T) {
	want := []string{"PATH=/bin", "GOCACHE=/cache", "GOMODCACHE=/modules", "DOCKER_HOST=local"}
	if got := isolatedEnvironment(want); !reflect.DeepEqual(got, want) {
		t.Fatal("tool settings removed")
	}
}

// TestRetainResources checks retention preserves runner-owned evidence.
//
// Retain mode must preserve the private configuration and never request volume deletion.
func TestRetainResources(t *testing.T) {
	var filename string
	stopped := false
	err := run(context.Background(), t.TempDir(), []string{"ETL_TEST_RETAIN=1"}, 54321, func(_ context.Context, c command) error {
		if c.program == "docker" {
			filename = argAfter(t, c.args, "--env-file")
			for _, arg := range c.args {
				if arg == "down" || arg == "--volumes" {
					t.Fatal("destructive cleanup in retained mode")
				}
			}
			if c.phase == "cleanup" {
				stopped = reflect.DeepEqual(c.args[len(c.args)-3:], []string{"stop", "-t", "30"})
			}
		}
		return nil
	})
	if err != nil || !stopped {
		t.Fatal("retain lifecycle failed", err)
	}
	if info, err := os.Stat(filename); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("private configuration not retained")
	}
	// This fake created no database; remove only its test credential file.
	t.Cleanup(func() { _ = os.Remove(filename) })
}

// TestRealFixtureIsolatedRetainedLifecycle checks authentic noncontiguous cases run in distinct
// retained projects with offline acquisition arguments.
func TestRealFixtureIsolatedRetainedLifecycle(t *testing.T) {
	for _, failSeed := range []bool{false, true} {
		t.Run(fmt.Sprint(failSeed), func(t *testing.T) {
			var filename, project string
			seeded, handed, stopped := false, false, false
			failure := errors.New("seed failed")
			fixture := realCase{id: "pinned-case", path: "/offline/block.json", manifest: "/offline/manifest.json", digest: "digest"}
			err := runCase(t.Context(), t.TempDir(), []string{"ETHEREUM_RPC_URL=forbidden", "REAL_FIXTURE_ARTIFACT=inherited", "COMPOSE_FILE=existing", "ETL_TEST_RETAIN=0"}, 54321, func(ctx context.Context, c command) error {
				for _, entry := range c.env {
					if strings.Contains(entry, "forbidden") || strings.Contains(entry, "inherited") || strings.Contains(entry, "COMPOSE_FILE=") {
						t.Fatal("unsafe inherited configuration")
					}
				}
				if c.program == "docker" {
					filename = argAfter(t, c.args, "--env-file")
					name := argAfter(t, c.args, "-p")
					if !strings.HasPrefix(name, "ethereum-stage13c-") || (project != "" && project != name) {
						t.Fatal("project isolation")
					}
					project = name
					if c.phase == "cleanup" {
						stopped = true
						if !reflect.DeepEqual(c.args[len(c.args)-3:], []string{"stop", "-t", "30"}) {
							t.Fatal("real regression deleted storage")
						}
					}
					return nil
				}
				for _, entry := range []string{"REAL_FIXTURE_ARTIFACT=" + fixture.path, "REAL_FIXTURE_CASE=" + fixture.id, "REAL_FIXTURE_SHA256=" + fixture.digest, "REAL_FIXTURE_MANIFEST=" + fixture.manifest, "HANDOFF_TEST_ISOLATED=1"} {
					found := false
					for _, e := range c.env {
						found = found || e == entry
					}
					if !found {
						t.Fatal("missing pinned replay configuration")
					}
				}
				if c.phase == "seed deterministic extraction" {
					seeded = true
					if c.expected != "TestSeedRealFixture" || argAfter(t, c.args, "-run") != "^TestSeedRealFixture$" {
						t.Fatal("synthetic seed substituted")
					}
					if failSeed {
						return failure
					}
				} else {
					handed = true
					if !seeded || c.expected != "TestHandoff" {
						t.Fatal("handoff before seed or without PASS gate")
					}
				}
				return nil
			}, &fixture)
			if !stopped || !seeded || handed == failSeed || (failSeed && !errors.Is(err, failure)) || (!failSeed && err != nil) {
				t.Fatal("incorrect failure/retention lifecycle", err)
			}
			if _, err := os.Stat(filename); err != nil {
				t.Fatal("retained configuration missing")
			}
			// Only this unit test's fake invocation file is removed; no DB was started.
			t.Cleanup(func() { _ = os.Remove(filename) })
		})
	}
}

// TestMissingRealFixturesFailBeforeDocker requires authentic artifact prerequisites to fail before
// creating infrastructure.
func TestMissingRealFixturesFailBeforeDocker(t *testing.T) {
	if err := startReal(t.Context(), t.TempDir()); err == nil {
		t.Fatal("missing private inputs silently accepted")
	}
}
