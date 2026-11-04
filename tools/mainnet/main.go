// main.go implements command startup, explicit adapter wiring and bounded resource lifetime in the
// local operator tooling.

// Command mainnet owns a separate, retained local Compose environment.
// Only explicit extract, e2e and reindex commands can start Ethereum acquisition.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// composeFile selects the checked-in Compose definition; project identity and private credentials
// are passed separately.
const composeFile = "deployments/compose/mainnet.yaml"

// defaults defines private operator configuration keys and conservative defaults; initialization
// supplies credentials separately.
var defaults = map[string]string{
	"MAINNET_PROJECT": "", "POSTGRES_PASSWORD": "",
	"MAINNET_VERIFY_TIMEOUT": "2m", "EXTRACTOR_DRAIN_TIMEOUT": "30s",
	"EXTRACTOR_MIGRATOR_PASSWORD": "", "EXTRACTOR_RUNTIME_PASSWORD": "",
	"TRANSFORMER_MIGRATOR_PASSWORD": "", "TRANSFORMER_RUNTIME_PASSWORD": "",
	"LOADER_MIGRATOR_PASSWORD": "", "LOADER_RUNTIME_PASSWORD": "",
	"EXTRACTOR_CHAIN_ID": "1", "EXTRACTOR_MAINNET_OPT_IN": "false",
	"EXTRACTOR_START_BLOCK": "", "EXTRACTOR_END_BLOCK": "",
	"EXTRACTOR_CONFIRMATION_DEPTH": "", "EXTRACTOR_RECOVERY_DEPTH": "",
	"EXTRACTOR_MAX_RPC_REQUESTS": "", "EXTRACTOR_RUN_TIMEOUT": "", "EXTRACTOR_RPC_URL": "",
	"EXTRACTOR_MAX_OUTSTANDING_EVENTS": "100", "EXTRACTOR_MAX_OUTSTANDING_BYTES": "67108864",
	"EXTRACTOR_MAX_DATABASE_BYTES": "1073741824",
}

// main invokes the local tool entry point and reports a nonzero status on configuration or
// execution failure.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := start(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// start validates the private operator configuration and orchestrates only the selected bounded
// Compose action through the command boundary.
func start(ctx context.Context, args []string) error {
	var recovery recoveryRequest
	if len(args) > 0 && (args[0] == "repair" || args[0] == "replay" || args[0] == "reindex") {
		var err error
		recovery, err = parseRecovery(args)
		if err != nil {
			return err
		}
	} else if len(args) != 2 {
		return errors.New("usage: mainnet <init|check|up|restart|extract|verify|e2e|drain|stop> CONFIG_PATH (from repository root)")
	}
	if args[0] == "init" {
		return initialize(args[1])
	}
	c, err := readConfig(args[1])
	if err != nil {
		return err
	}
	return run(ctx, args[0], c, os.Environ(), execute, recovery)
}

// initialize creates a new private Mainnet configuration with generated credentials; existing
// files are never overwritten.
func initialize(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("cannot create private configuration; destination must not exist")
	}
	defer f.Close()
	if err := f.Chmod(0600); err != nil {
		return errors.New("cannot protect configuration")
	}
	keys := make([]string, 0, len(defaults))
	for k := range defaults {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := defaults[k]
		if strings.HasSuffix(k, "_PASSWORD") || k == "MAINNET_PROJECT" {
			b := make([]byte, 24)
			if _, err := rand.Read(b); err != nil {
				return errors.New("cannot generate private configuration")
			}
			v = hex.EncodeToString(b)
			if k == "MAINNET_PROJECT" {
				v = "ethereum-mainnet-" + v[:24]
			}
		}
		if _, err := fmt.Fprintf(f, "%s=%s\n", k, v); err != nil {
			return errors.New("cannot write private configuration")
		}
	}
	return nil
}

// This deliberately small format uses literal NAME=value lines, not shell or
// dotenv evaluation. Never source it; URL punctuation is not executable input.
func readConfig(path string) (map[string]string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 16384 {
		return nil, errors.New("configuration must be a regular, private 0600 file of at most 16 KiB")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot read private configuration")
	}
	defer f.Close()
	c := make(map[string]string)
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		_, known := defaults[k]
		_, duplicate := c[k]
		if !ok || (!known && !performanceKey(k)) || duplicate || strings.ContainsAny(v, "\r\x00") {
			return nil, errors.New("invalid or duplicate configuration entry")
		}
		c[k] = v
	}
	if s.Err() != nil {
		return nil, errors.New("cannot parse private configuration")
	}
	return c, nil
}

// validate requires isolated Mainnet project identity, owning credentials and explicit finite
// acquisition bounds before operator execution.
func validate(c map[string]string, acquisition bool) error {
	if err := validatePerformance(c); err != nil {
		return err
	}
	if !regexp.MustCompile(`^ethereum-mainnet-[0-9a-f]{24}$`).MatchString(c["MAINNET_PROJECT"]) || c["EXTRACTOR_CHAIN_ID"] != "1" {
		return errors.New("requires isolated Mainnet project and chain ID 1")
	}
	for k := range defaults {
		if strings.HasSuffix(k, "_PASSWORD") && !regexp.MustCompile(`^[0-9a-f]{48}$`).MatchString(c[k]) {
			return errors.New("use generated database credentials")
		}
	}
	for _, k := range []string{"EXTRACTOR_MAX_OUTSTANDING_EVENTS", "EXTRACTOR_MAX_OUTSTANDING_BYTES", "EXTRACTOR_MAX_DATABASE_BYTES"} {
		n, err := strconv.ParseUint(c[k], 10, 63)
		if err != nil || n == 0 {
			return fmt.Errorf("invalid %s", k)
		}
	}
	if raw := c["EXTRACTOR_DRAIN_TIMEOUT"]; raw != "" {
		d, e := time.ParseDuration(raw)
		if e != nil || d < time.Millisecond || d > 2*time.Minute {
			return errors.New("invalid EXTRACTOR_DRAIN_TIMEOUT")
		}
	}
	if !acquisition {
		return nil
	}
	if c["EXTRACTOR_MAINNET_OPT_IN"] != "true" {
		return errors.New("explicit EXTRACTOR_MAINNET_OPT_IN=true required")
	}
	var numbers []uint64
	for _, k := range []string{"EXTRACTOR_START_BLOCK", "EXTRACTOR_END_BLOCK", "EXTRACTOR_CONFIRMATION_DEPTH", "EXTRACTOR_RECOVERY_DEPTH", "EXTRACTOR_MAX_RPC_REQUESTS"} {
		n, err := strconv.ParseUint(c[k], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid or missing %s", k)
		}
		numbers = append(numbers, n)
	}
	if numbers[1] < numbers[0] || numbers[3] < 1 || numbers[3] > 10000 || numbers[4] == 0 {
		return errors.New("invalid range, recovery depth or request budget")
	}
	d, err := time.ParseDuration(c["EXTRACTOR_RUN_TIMEOUT"])
	if err != nil || d < time.Millisecond || d > 24*time.Hour {
		return errors.New("invalid EXTRACTOR_RUN_TIMEOUT")
	}
	u, err := url.Parse(c["EXTRACTOR_RPC_URL"])
	if err != nil || u == nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return errors.New("invalid HTTPS RPC endpoint")
	}
	return nil
}

// Filter inherited settings so shell variables cannot override this file or
// select fixture storage. Keep PATH, Docker connection and toolchain settings.
func environment(parent []string, c map[string]string) []string {
	var env []string
	for _, entry := range parent {
		k, _, _ := strings.Cut(entry, "=")
		blocked := false
		for _, prefix := range []string{"COMPOSE_", "MAINNET_", "EXTRACTOR_", "TRANSFORMER_", "LOADER_", "POSTGRES_", "PG", "FIXTURE_", "ETHEREUM_"} {
			blocked = blocked || strings.HasPrefix(k, prefix)
		}
		if !blocked {
			env = append(env, entry)
		}
	}
	for k, v := range c {
		env = append(env, k+"="+v)
	}
	return env
}

// executor is the injectable child-command boundary used to test operator safety without starting
// Docker or RPC.
type executor func(context.Context, []string, []string) ([]byte, error)

// run validates the private operator configuration and orchestrates only the selected bounded
// Compose action through the command boundary.
func run(ctx context.Context, action string, c map[string]string, parent []string, invoke executor, recovery ...recoveryRequest) error {
	if action != "check" && action != "up" && action != "extract" && action != "stop" && action != "verify" && action != "e2e" && action != "drain" && action != "replay" && action != "repair" && action != "reindex" && action != "restart" {
		return errors.New("unknown Mainnet command")
	}
	if err := validate(c, action == "extract" || action == "e2e" || action == "reindex"); err != nil {
		return err
	}

	var repair recoveryRequest
	if len(recovery) > 0 {
		repair = recovery[0]
	}
	if action == "repair" || action == "replay" || action == "reindex" {
		if !operationID(repair.operation) {
			return errors.New("validated recovery operation required")
		}
	}
	if action == "reindex" {
		if _, err := completionBounds(c, true, parent); err != nil {
			return err
		}
	}
	if action == "drain" {
		if _, err := completionBounds(c, false, parent); err != nil {
			return err
		}
	}
	if action == "repair" || action == "replay" || action == "reindex" || action == "drain" {
		limited, cancel := context.WithTimeout(ctx, 2*time.Minute+40*time.Second)
		defer cancel()
		ctx = limited
	}
	var verifyTimeout time.Duration
	if action == "verify" || action == "e2e" {
		var e error
		verifyTimeout, e = completionBounds(c, action == "e2e", parent)
		if e != nil {
			return e
		}
		total := verifyTimeout
		if action == "e2e" {
			total += 2*time.Minute + 40*time.Second
		}
		limited, cancel := context.WithTimeout(ctx, total)
		defer cancel()
		ctx = limited
	}
	env := environment(parent, c)
	project := c["MAINNET_PROJECT"]
	file, err := filepath.Abs(composeFile)
	if err != nil {
		return errors.New("cannot locate Mainnet Compose file")
	}
	base := []string{"compose", "--env-file", os.DevNull, "-p", project, "-f", file}
	command := func(phase string, args ...string) error {
		fmt.Println(phase)
		_, err := invoke(ctx, append(append([]string{}, base...), args...), env)
		if err != nil {
			return errors.New(phase + " failed (command output suppressed to protect credentials)")
		}
		return nil
	}
	if err := command("Validate isolated Compose configuration", "config", "--quiet"); err != nil {
		return err
	}
	// Refuse unknown/pre-existing volumes instead of overwriting or relabeling.
	volume := project + "_mainnet_postgres_data"
	list, err := invoke(ctx, []string{"volume", "ls", "--format", "{{.Name}}", "--filter", "name=" + volume}, env)
	if err != nil {
		return errors.New("cannot verify Mainnet storage isolation")
	}
	for _, name := range strings.Fields(string(list)) {
		if name != volume {
			continue
		}
		label, err := invoke(ctx, []string{"volume", "inspect", "--format", `{{index .Labels "ethereum-etl.environment"}}|{{index .Labels "com.docker.compose.project"}}|{{index .Labels "com.docker.compose.volume"}}`, volume}, env)
		if err != nil || strings.TrimSpace(string(label)) != "mainnet|"+project+"|mainnet_postgres_data" {
			return errors.New("refusing storage without matching Mainnet ownership labels")
		}
	}
	switch action {
	case "drain", "replay", "repair", "reindex":
		return command("Run explicit "+action+" operation", recoveryArgs(action, project, repair)...)
	case "up", "restart":
		if action == "restart" {
			active, err := invoke(ctx, []string{"ps", "-q", "--filter", "name=^/" + project + "-acquisition$"}, env)
			if err != nil || len(strings.TrimSpace(string(active))) != 0 {
				return errors.New("restart requires no active acquisition, replay or drain; stop it explicitly first")
			}
			if err := command("Stop existing consumers and database", "stop", "-t", "30", "transformer", "loader", "postgres"); err != nil {
				return err
			}
		}
		if err := command("Build local services", "build", "extractor"); err != nil {
			return err
		}
		if err := command("Start isolated PostgreSQL", "up", "-d", "--wait", "--wait-timeout", "120", "postgres"); err != nil {
			return err
		}
		// An exited one-shot migration is not a long-running --wait target.
		for _, service := range []string{"extractor", "transformer", "loader"} {
			if err := command("Apply "+service+" migrations", "run", "--rm", "migrate-"+service); err != nil {
				return err
			}
		}
		return command("Start Mainnet consumers (no acquisition)", "up", "-d", "--wait", "--wait-timeout", "120", "transformer", "loader")
	case "verify":
		return verifyCompletion(ctx, c, parent, base, invoke, verifyTimeout)
	case "e2e":
		if err := command("Run bounded acquisition and durable dispatch", "run", "--rm", "--no-deps", "--name", project+"-acquisition", "extractor"); err != nil {
			return err
		}
		return verifyCompletion(ctx, c, parent, base, invoke, verifyTimeout)
	case "extract":
		// Fixed container name prevents overlapping acquisition in this project.
		// No restart policy or wrapper retry replenishes an exhausted budget.
		return command("Run explicitly bounded acquisition", "run", "--rm", "--no-deps", "--name", project+"-acquisition", "extractor")
	case "stop":
		return command("Stop services; retain all history and credentials", "stop", "-t", "30")
	}
	return nil
}

// execute runs a bounded child command and returns its exit status through redacted, size-limited
// output handling.
func execute(ctx context.Context, args, env []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env, cmd.Stderr = env, io.Discard
	cmd.WaitDelay = 5 * time.Second
	if len(args) > 0 && (args[0] == "volume" || args[0] == "ps" || containsArg(args, "psql")) {
		return cmd.Output()
	}
	cmd.Stdout = io.Discard
	// On interruption allow Compose to forward SIGTERM and join service shutdown.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 35 * time.Second
	return nil, cmd.Run()
}

// containsArg checks for a literal CLI argument when enforcing operator command safety gates.
func containsArg(args []string, want string) bool {
	for _, v := range args {
		if v == want {
			return true
		}
	}
	return false
}
