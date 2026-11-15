// Command tsi is the single binary behind every role in the demo. The first
// argument picks the role. Settings come from environment variables, because
// that is the simplest thing to pass to a container.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/chstore"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/fakenet"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/ingest"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/launch"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/proxy"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/sandbox"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

const usage = "usage: tsi sandbox | proxy | ingest | fakenet | launch up | launch down"

func main() {
	// Every log line carries the IDs this process was started with.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if id := os.Getenv("TSI_SANDBOX_ID"); id != "" {
		logger = logger.With("tenant_id", os.Getenv("TSI_TENANT_ID"), "sandbox_id", id)
	}
	slog.SetDefault(logger)

	// ctx is cancelled on Ctrl-C or docker stop, which shuts every role down cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("exiting", "role", os.Args[1], "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	var c clock.Clock = clock.Real{}
	switch args[0] {
	case "sandbox":
		if env("TSI_BEHAVIOUR", "normal") == "probe" {
			return sandbox.Probe(ctx, list("TSI_PROBE_TARGETS"), os.Stdout)
		}
		return sandbox.Run(ctx, sandbox.Config{
			Behaviour: env("TSI_BEHAVIOUR", "normal"), Start: duration("TSI_START", 0), Rate: number("TSI_RATE", 0),
			EventsURL: env("TSI_EVENTS_URL", ""), EgressProxy: env("TSI_EGRESS_PROXY", ""), Clock: c,
		})
	case "proxy":
		return proxy.New(proxy.Config{
			TenantID: env("TSI_TENANT_ID", ""), SandboxID: env("TSI_SANDBOX_ID", ""),
			ListenIP: env("TSI_LISTEN_IP", ""), PeerIP: env("TSI_PEER_IP", ""),
			IngestURL: env("TSI_INGEST_URL", "http://ingest:8080"),
			Rate:      float64(number("TSI_RATE", 100)), Burst: number("TSI_BURST", 200),
			Sites: scenario.Sites{Trusted: list("TSI_SITES_TRUSTED"), Watched: list("TSI_SITES_WATCHED"), Bad: list("TSI_SITES_BAD")},
			Clock: c,
		}).Run(ctx)
	case "ingest":
		store, err := chstore.Open(env("TSI_CLICKHOUSE_ADDR", "clickhouse:9000"), env("TSI_CLICKHOUSE_DB", "tsi"),
			env("TSI_CLICKHOUSE_USER", "tsi"), env("TSI_CLICKHOUSE_PASSWORD", ""), c)
		if err != nil {
			return err
		}
		return ingest.New(ingest.Config{
			Addr: env("TSI_ADDR", ":8080"), Sink: store,
			QueueLimit: number("TSI_QUEUE_LIMIT", 50000), FlushRows: number("TSI_FLUSH_ROWS", 10000),
			FlushEvery: duration("TSI_FLUSH_EVERY", time.Second),
		}).Run(ctx)
	case "fakenet":
		return fakenet.Run(ctx, env("TSI_ADDR", ":80"))
	case "launch":
		// Starting 17 pairs takes a few seconds. Two minutes means something is stuck.
		ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		l, err := launch.New(env("TSI_IMAGE", "tsi:dev"), env("TSI_INGEST_URL", "http://ingest:8080"))
		if err != nil {
			return err
		}
		if len(args) > 1 && args[1] == "down" {
			return l.Down(ctx)
		}
		sc, err := scenario.Load(env("TSI_SCENARIO", "scenarios/default.yaml"))
		if err != nil {
			return err
		}
		return l.Up(ctx, sc)
	}
	return fmt.Errorf("unknown role %q\n%s", args[0], usage)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// number and duration fall back on a missing or unparsable value. A typo
// in a limit then means the default limit, never no limit.
func number(key string, fallback int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return n
	}
	return fallback
}

func duration(key string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return d
	}
	return fallback
}

func list(key string) []string {
	if v := os.Getenv(key); v != "" {
		return strings.Split(v, ",")
	}
	return nil
}
