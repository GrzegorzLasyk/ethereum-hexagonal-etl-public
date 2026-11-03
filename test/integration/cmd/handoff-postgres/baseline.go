package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/test/performance"
)

type baselineRun struct{ specPath, testBinary string }
type baselineOptions struct {
	opt                             bool
	dataset, fixtures, out, workers string
	blocks, repeats, warmup         int
	loaderBatchSize                 int
}

func baselineFlags(args []string) (baselineOptions, error) {
	var o baselineOptions
	f := flag.NewFlagSet("baseline", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.BoolVar(&o.opt, "opt-in", false, "")
	f.StringVar(&o.dataset, "dataset", "synthetic", "")
	f.StringVar(&o.fixtures, "fixtures", ".mainnet/fixtures/stage13-3b", "")
	f.StringVar(&o.out, "out", "", "")
	f.StringVar(&o.workers, "workers", "1,2,4,8", "")
	f.IntVar(&o.loaderBatchSize, "loader-batch-size", 64, "")
	f.IntVar(&o.blocks, "blocks", 100, "")
	f.IntVar(&o.repeats, "repeat", 3, "")
	f.IntVar(&o.warmup, "warmup", 1, "")
	if f.Parse(args) != nil || o.loaderBatchSize < 1 || o.loaderBatchSize > 256 || f.NArg() != 0 || !o.opt || o.out == "" || (o.dataset != "synthetic" && o.dataset != "authentic" && o.dataset != "all") || o.blocks < 1 || o.blocks > 1000 || o.repeats < 1 || o.repeats > 10 || o.warmup < 0 || o.warmup > 2 {
		return o, errors.New("usage: --baseline --opt-in --out NEW_DIRECTORY [--dataset synthetic|authentic|all --fixtures DIRECTORY --blocks 1..1000 --workers 1,2,4,8 --repeat 1..10 --warmup 0..2 --loader-batch-size 1..256]")
	}
	_, err := baselineWorkers(o.workers)
	return o, err
}
func baselineWorkers(raw string) ([]int, error) {
	var values []int
	seen := map[int]bool{}
	for _, s := range strings.Split(raw, ",") {
		n, e := strconv.Atoi(s)
		if e != nil || seen[n] || (n != 1 && n != 2 && n != 4 && n != 8) {
			return nil, errors.New("workers must be unique values from 1,2,4,8")
		}
		seen[n] = true
		values = append(values, n)
	}
	return values, nil
}
func startBaseline(ctx context.Context, args []string) error {
	o, e := baselineFlags(args)
	if e != nil {
		return e
	}
	if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return errors.New("baseline execution is disabled in CI")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()
	root, e := os.Getwd()
	if e != nil {
		return errors.New("cannot locate repository")
	}
	out, e := filepath.Abs(o.out)
	if e != nil {
		return errors.New("invalid output path")
	}
	// No existing report, fixture, private configuration or user volume is replaced.
	if e = os.Mkdir(out, 0700); e != nil {
		return errors.New("output directory must not exist and its parent must exist")
	}
	env := isolatedEnvironment(os.Environ())
	var datasets []performance.Scenario
	if o.dataset != "authentic" {
		datasets = append(datasets, performance.Scenario{Dataset: "synthetic-stage11", Blocks: o.blocks, Start: 100})
	}
	if o.dataset != "synthetic" {
		for _, c := range []struct {
			id string
			n  uint64
		}{{"historical-smoke", 1000000}, {"london-access-list", 12965000}, {"pectra-blob-dynamic-fee", 22431084}} {
			path, _ := filepath.Abs(filepath.Join(o.fixtures, fmt.Sprint(c.n)+".json"))
			manifest := filepath.Join(root, "test/fixtures/mainnet/v1/manifest.json")
			info, err := os.Lstat(path)
			if err != nil || !info.Mode().IsRegular() || info.Size() > 96<<20 {
				return errors.New("required authentic artifact missing, nonregular or oversized")
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return errors.New("cannot read artifact")
			}
			sum := sha256.Sum256(data)
			verify, cancel := context.WithTimeout(ctx, 2*time.Minute)
			err = execute(verify, command{phase: "verify authentic baseline input " + fmt.Sprint(c.n), program: "go", args: []string{"run", "./services/extractor-service/cmd/fixture-capture", "verify", manifest, c.id, path}, root: root, env: env})
			cancel()
			if err != nil {
				return err
			}
			datasets = append(datasets, performance.Scenario{Dataset: "authentic-mainnet", Blocks: 1, Start: c.n, Artifact: path, Manifest: manifest, Case: c.id, Digest: hex.EncodeToString(sum[:])})
		}
	}
	binaries := filepath.Join(out, "binaries")
	if os.Mkdir(binaries, 0700) != nil {
		return errors.New("cannot create binary directory")
	}
	for _, service := range []string{"extractor", "transformer", "loader"} {
		if err := execute(ctx, command{phase: "build baseline " + service, program: "go", args: []string{"build", "-tags", "perfbaseline", "-o", filepath.Join(binaries, service), "./services/" + service + "-service/cmd/" + service}, root: root, env: env}); err != nil {
			return err
		}
	}
	for _, b := range []struct{ name, pkg string }{{"source.test", "./services/extractor-service/internal/adapters/outbound/postgres"}, {"pipeline.test", "./test/integration/handoff"}} {
		if err := execute(ctx, command{phase: "build baseline harness " + b.name, program: "go", args: []string{"test", "-c", "-tags", "postgresintegration,perfbaseline", "-o", filepath.Join(binaries, b.name), b.pkg}, root: root, env: env}); err != nil {
			return err
		}
	}
	hardware := map[string]any{"go_version": runtime.Version(), "host_os": runtime.GOOS, "host_arch": runtime.GOARCH, "host_logical_cpus": runtime.NumCPU(), "race_instrumentation": false, "build_tag": "perfbaseline", "startup_policy": "fresh_processes_and_fresh_database_each_sample", "warmup_policy": "separate_disposable_runs_excluded_not_shared_database_cache"}
	read := func(program string, a ...string) string {
		cmd := exec.CommandContext(ctx, program, a...)
		cmd.Dir = root
		data, err := cmd.Output()
		if err != nil {
			return "unavailable"
		}
		return strings.TrimSpace(string(data))
	}
	hardware["git_head"] = read("git", "rev-parse", "HEAD")
	hardware["git_dirty"] = read("git", "status", "--porcelain") != ""
	// Fingerprint every nonignored source file, never copy its contents into evidence.
	names := strings.Split(read("git", "ls-files", "-co", "--exclude-standard", "-z"), "\x00")
	h := sha256.New()
	seen := map[string]bool{}
	for _, name := range names {
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		path := filepath.Join(root, name)
		if strings.HasPrefix(path, out+string(os.PathSeparator)) {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		data, err := os.ReadFile(path)
		if err == nil {
			h.Write([]byte(name + "\x00"))
			h.Write(data)
		}
	}
	hardware["source_sha256"] = hex.EncodeToString(h.Sum(nil))
	hardware["docker"] = read("docker", "info", "--format", "server={{.ServerVersion}} os={{.OperatingSystem}} arch={{.Architecture}} cpus={{.NCPU}} memory_bytes={{.MemTotal}}")
	if runtime.GOOS == "darwin" {
		hardware["cpu_model"] = read("sysctl", "-n", "machdep.cpu.brand_string")
		hardware["host_memory_bytes"] = read("sysctl", "-n", "hw.memsize")
	}
	if err := performance.ExclusiveJSON(filepath.Join(out, "environment.json"), hardware); err != nil {
		return err
	}
	workers, _ := baselineWorkers(o.workers)
	for _, data := range datasets {
		for _, w := range workers {
			for rep := 0; rep < o.warmup+o.repeats; rep++ {
				s := data
				s.Workers = w
				s.LoaderBatchSize = o.loaderBatchSize
				s.Warmup = rep < o.warmup
				s.Repetition = rep - o.warmup + 1
				s.Binaries = binaries
				s.Directory = filepath.Join(out, fmt.Sprintf("%s-%d-w%d-run%02d", s.Dataset, s.Start, w, rep))
				if os.Mkdir(s.Directory, 0700) != nil {
					return errors.New("cannot create exclusive sample directory")
				}
				spec := filepath.Join(s.Directory, "scenario.json")
				if err := performance.ExclusiveJSON(spec, performance.WireSpec{Scenario: s, ArtifactPath: s.Artifact, ManifestPath: s.Manifest, OutputDirectory: s.Directory, BinaryDirectory: binaries}); err != nil {
					return err
				}
				l, err := net.Listen("tcp4", "127.0.0.1:0")
				if err != nil {
					return errors.New("cannot reserve test port")
				}
				port := l.Addr().(*net.TCPAddr).Port
				l.Close()
				fmt.Printf("Baseline %s height=%d blocks=%d workers=%d warmup=%t\n", s.Dataset, s.Start, s.Blocks, w, s.Warmup)
				if err = runCase(ctx, root, os.Environ(), port, execute, nil, &baselineRun{spec, filepath.Join(binaries, "pipeline.test")}); err != nil {
					return err
				}
				if s.Artifact != "" {
					bytes, err := os.ReadFile(s.Artifact)
					if err != nil {
						return errors.New("artifact disappeared")
					}
					sum := sha256.Sum256(bytes)
					if hex.EncodeToString(sum[:]) != s.Digest {
						return errors.New("authentic artifact changed")
					}
				}
			}
		}
	}
	return performance.ExclusiveJSON(filepath.Join(out, "completed.json"), map[string]any{"status": "passed", "report_version": "stage14.1/v1", "workers": workers, "repetitions": o.repeats, "warmup": o.warmup, "datasets": len(datasets), "live_rpc_requests": 0})
}
