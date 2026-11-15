//go:build integration

// These tests need a Docker engine and the compose stack (`make up`). They
// prove the two properties that only the real network can prove.
package launch

import (
	"bytes"
	"context"
	"encoding/json"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/sandbox"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

// A range of its own, so the tests can run next to a live demo.
var testBase = netip.MustParsePrefix("10.78.0.0/24")

var testScenario = &scenario.Scenario{
	Limits: scenario.Limits{EventsPerSec: 100, Burst: 200},
	Sites:  scenario.Sites{Trusted: []string{"allowed.test"}},
}

func setup(t *testing.T) (*Launcher, context.Context) {
	t.Helper()
	l, err := New("tsi:dev", "http://ingest:8080")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return l, ctx
}

// pair creates one sandbox network with its proxy running, and removes both
// when the test ends.
func pair(t *testing.T, l *Launcher, ctx context.Context, index int, id string) Pair {
	t.Helper()
	p := PairFor(testBase, index, id)
	remove := func() {
		for _, name := range []string{"tsi-proxy-" + id, "tsi-sandbox-" + id} {
			_, _ = l.cli.ContainerRemove(context.Background(), name, client.ContainerRemoveOptions{Force: true})
		}
		_, _ = l.cli.NetworkRemove(context.Background(), p.Network, client.NetworkRemoveOptions{})
	}
	remove() // left over from a crashed run
	t.Cleanup(remove)
	if err := l.createNetwork(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := l.startProxy(ctx, testScenario, "it-tenant", id, p); err != nil {
		t.Fatal(err)
	}
	return p
}

// address returns a container's IP on one network.
func address(t *testing.T, l *Launcher, ctx context.Context, containerName, networkName string) string {
	t.Helper()
	info, err := l.cli.ContainerInspect(ctx, containerName, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("%s is not running, did you run `make up`? %v", containerName, err)
	}
	return info.Container.NetworkSettings.Networks[networkName].IPAddress.String()
}

// probe runs the probe where a container with these networks would run and
// returns what it could reach.
func probe(t *testing.T, l *Launcher, ctx context.Context, name string, targets []string, primaryNet string, endpoints map[string]*network.EndpointSettings) map[string]bool {
	t.Helper()
	env := []string{"TSI_BEHAVIOUR=probe", "TSI_PROBE_TARGETS=" + strings.Join(targets, ",")}
	id, err := l.start(ctx, name, "sandbox", name, env, primaryNet, endpoints)
	defer func() {
		_, _ = l.cli.ContainerRemove(context.Background(), name, client.ContainerRemoveOptions{Force: true})
	}()
	if err != nil {
		t.Fatal(err)
	}
	wait := l.cli.ContainerWait(ctx, id, client.ContainerWaitOptions{})
	select {
	case <-wait.Result:
	case err := <-wait.Error:
		t.Fatal(err)
	}
	logs, err := l.cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true})
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	var stdout bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &bytes.Buffer{}, logs); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	dec := json.NewDecoder(&stdout)
	for dec.More() {
		var r sandbox.ProbeResult
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("bad probe output: %v", err)
		}
		got[r.Name] = r.Reachable
	}
	if len(got) != len(targets) {
		t.Fatalf("probe reported %d of %d targets: %v", len(got), len(targets), got)
	}
	return got
}

// asSandbox probes from the exact network position of a sandbox.
func asSandbox(t *testing.T, l *Launcher, ctx context.Context, p Pair, name string, targets []string) map[string]bool {
	return probe(t, l, ctx, name, targets, p.Network, map[string]*network.EndpointSettings{
		p.Network: {IPAMConfig: &network.EndpointIPAMConfig{IPv4Address: p.SandboxIP}},
	})
}

func TestSandboxCanReachNothingButItsProxy(t *testing.T) {
	l, ctx := setup(t)
	mine := pair(t, l, ctx, 0, "it-a")
	other := pair(t, l, ctx, 1, "it-b")

	forbidden := []string{
		"ingest=" + address(t, l, ctx, "tsi-ingest-1", controlNet) + ":8080",
		"clickhouse-native=" + address(t, l, ctx, "tsi-clickhouse-1", "tsi-storage") + ":9000",
		"clickhouse-http=" + address(t, l, ctx, "tsi-clickhouse-1", "tsi-storage") + ":8123",
		"fake-internet=" + address(t, l, ctx, "tsi-fakenet-1", internetNet) + ":80",
		"own-proxy-control-side=" + address(t, l, ctx, "tsi-proxy-it-a", controlNet) + ":8080",
		"other-proxy-events=" + other.ProxyIP.String() + ":8080",
		"other-proxy-egress=" + other.ProxyIP.String() + ":3128",
		"public-internet=1.1.1.1:443",
		"dns-ingest=dns:ingest",
		"dns-fake-host=dns:allowed.test",
		"dns-public=dns:example.com",
	}
	allowed := []string{
		"own-proxy-events=" + mine.ProxyIP.String() + ":8080",
		"own-proxy-egress=" + mine.ProxyIP.String() + ":3128",
	}

	// Control: from a proxy's position the same targets are reachable. So
	// when the sandbox fails to reach them, it is the network saying no,
	// not a target that happens to be down.
	control := probe(t, l, ctx, "tsi-it-control", []string{forbidden[0], "dns-ingest=dns:ingest"}, controlNet, map[string]*network.EndpointSettings{controlNet: {}})
	if !control["ingest"] || !control["dns-ingest"] {
		t.Fatalf("control probe cannot reach ingest, so this test would prove nothing: %v", control)
	}

	got := asSandbox(t, l, ctx, mine, "tsi-sandbox-it-a", append(allowed, forbidden...))
	for _, target := range allowed {
		if name, _, _ := strings.Cut(target, "="); !got[name] {
			t.Errorf("sandbox cannot reach %s", target)
		}
	}
	for _, target := range forbidden {
		if name, _, _ := strings.Cut(target, "="); got[name] {
			t.Errorf("sandbox reached %s", target)
		}
	}
}

func TestSandboxWithDeadProxyCanSendNothing(t *testing.T) {
	l, ctx := setup(t)
	p := pair(t, l, ctx, 2, "it-c")
	targets := []string{
		"own-proxy-events=" + p.ProxyIP.String() + ":8080",
		"own-proxy-egress=" + p.ProxyIP.String() + ":3128",
		"ingest=" + address(t, l, ctx, "tsi-ingest-1", controlNet) + ":8080",
		"fake-internet=" + address(t, l, ctx, "tsi-fakenet-1", internetNet) + ":80",
		"public-internet=1.1.1.1:443",
	}
	before := asSandbox(t, l, ctx, p, "tsi-sandbox-it-c", targets)
	if !before["own-proxy-events"] || !before["own-proxy-egress"] {
		t.Fatalf("sandbox cannot reach its live proxy: %v", before)
	}

	if _, err := l.cli.ContainerRemove(ctx, "tsi-proxy-it-c", client.ContainerRemoveOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	// No fallback path appears when the proxy is gone: fail closed.
	for name, reachable := range asSandbox(t, l, ctx, p, "tsi-sandbox-it-c", targets) {
		if reachable {
			t.Errorf("with its proxy dead the sandbox still reached %s", name)
		}
	}
}
