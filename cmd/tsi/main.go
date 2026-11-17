// Command tsi is the single binary behind every role in the demo. The first
// argument picks the role. Settings come from environment variables, because
// that is the simplest thing to pass to a container.
package main

import (
	"context"
	"errors"
	"flag"
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
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/guard"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/ingest"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/launch"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/proxy"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/replay"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/sandbox"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

const usage = "usage: tsi sandbox | proxy | ingest | guard | fakenet | launch up | launch down | ab --recording R --a RULES --b RULES"

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
			IngestURL: env("TSI_INGEST_URL", "http://ingest:8080"), GuardURL: env("TSI_GUARD_URL", ""),
			Rate: float64(number("TSI_RATE", 100)), Burst: number("TSI_BURST", 200),
			Sites: scenario.Sites{Trusted: list("TSI_SITES_TRUSTED"), Watched: list("TSI_SITES_WATCHED"), Bad: list("TSI_SITES_BAD")},
			Clock: c,
		}).Run(ctx)
	case "ingest":
		store, err := openStore(c)
		if err != nil {
			return err
		}
		return ingest.New(ingest.Config{
			Addr: env("TSI_ADDR", ":8080"), Sink: store, GuardURL: env("TSI_GUARD_URL", ""),
			QueueLimit: number("TSI_QUEUE_LIMIT", 50000), QueueBytes: number("TSI_QUEUE_BYTES", 64<<20),
			FlushRows:  number("TSI_FLUSH_ROWS", 10000),
			FlushEvery: duration("TSI_FLUSH_EVERY", time.Second),
		}).Run(ctx)
	case "guard":
		sc, err := scenario.Load(env("TSI_SCENARIO", "scenarios/default.yaml"))
		if err != nil {
			return err
		}
		rules, err := guard.LoadRules(env("TSI_RULES", "rules/a.yaml"))
		if err != nil {
			return err
		}
		// For recording runs: with every rule in shadow the traffic is not
		// shaped by any rule set, so a replay can compare them fairly.
		// Anything that does not parse as true leaves the rules enforcing.
		shadow, _ := strconv.ParseBool(env("TSI_SHADOW", "false"))
		if shadow {
			for _, r := range rules.Rules {
				r.Mode = guard.Shadow
			}
		}
		var record *guard.Recorder // stays nil, and nothing is recorded, unless a path is given
		path := env("TSI_RECORD", "")
		if path != "" {
			f, err := os.Create(path)
			if err != nil {
				return err
			}
			defer f.Close()
			if record, err = guard.NewRecorder(f, guard.Header{Enforced: !shadow, Tenants: sc.Tenants}); err != nil {
				return err
			}
		}
		store, err := openStore(c)
		if err != nil {
			return err
		}
		// The guard kills through the same Docker code that launch builds with.
		docker, err := launch.New("", "", "")
		if err != nil {
			return err
		}
		err = guard.New(guard.Config{
			Addr: env("TSI_ADDR", ":8080"), Rules: rules, Tiers: sc.Tiers(), Runtime: docker, Sink: store, Record: record, Clock: c,
		}).Run(ctx)
		if record != nil {
			// A recording with a gap would score rule sets on traffic that
			// never happened. With the file gone, make record keeps the old one.
			if gap := record.Close(); gap != nil {
				_ = os.Remove(path)
				return errors.Join(err, fmt.Errorf("recording removed: %w", gap))
			}
		}
		return err
	case "ab":
		// The one role with flags: it is run by hand, not in a container.
		flags := flag.NewFlagSet("ab", flag.ContinueOnError)
		recording := flags.String("recording", "", "the recording to replay")
		a, b := flags.String("a", "rules/a.yaml", "rule set A"), flags.String("b", "rules/b.yaml", "rule set B")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		// A replayed guard logs every decision. The report is the output,
		// and it lists the rules that could not be evaluated.
		log := slog.Default()
		slog.SetDefault(slog.New(slog.DiscardHandler))
		defer slog.SetDefault(log)
		return replay.AB(os.Stdout, *recording, *a, *b)
	case "fakenet":
		return fakenet.Run(ctx, env("TSI_ADDR", ":80"))
	case "launch":
		// Starting 17 pairs takes a few seconds and the timeline about a
		// minute. Five minutes means something is stuck.
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		l, err := launch.New(env("TSI_IMAGE", "tsi:dev"), env("TSI_INGEST_URL", "http://ingest:8080"), env("TSI_GUARD_URL", "http://guard:8080"))
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
		if err := l.Up(ctx, sc); err != nil {
			return err
		}
		return l.Watch(ctx, sc, duration("TSI_WATCH", 70*time.Second), os.Stdout)
	}
	return fmt.Errorf("unknown role %q\n%s", args[0], usage)
}

func openStore(c clock.Clock) (*chstore.Store, error) {
	return chstore.Open(env("TSI_CLICKHOUSE_ADDR", "clickhouse:9000"), env("TSI_CLICKHOUSE_DB", "tsi"),
		env("TSI_CLICKHOUSE_USER", "tsi"), env("TSI_CLICKHOUSE_PASSWORD", ""), c)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// number and duration fall back on a missing, unparsable, zero or negative
// value. A typo in a limit then means the default limit, never no limit and
// never a crash in a ticker or a queue.
func number(key string, fallback int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return fallback
}

func duration(key string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil && d > 0 {
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
