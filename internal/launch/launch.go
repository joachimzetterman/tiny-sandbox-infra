// Package launch starts sandbox and proxy pairs through the Docker API.
// The isolation of a sandbox is decided here, by how its network is built.
package launch

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

const (
	// Every container and network launch creates carries this label, so
	// Down can find and remove exactly those and nothing else.
	labelManaged = "tsi.managed"
	controlNet   = "tsi-control"  // proxies, ingest, guard
	internetNet  = "tsi-internet" // proxies and the fake internet
	// killEach bounds the kill of one container, so a call that hangs on
	// the sandbox cannot use up the time meant for its proxy.
	killEach = 2 * time.Second
)

type Launcher struct {
	cli       *client.Client
	Image     string
	IngestURL string
	GuardURL  string
	Clock     clock.Clock
	began     time.Time            // when Up started, the zero of the timeline
	started   map[string]time.Time // sandbox ID to when its container started
}

func New(image, ingestURL, guardURL string) (*Launcher, error) {
	cli, err := client.New(client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &Launcher{cli: cli, Image: image, IngestURL: ingestURL, GuardURL: guardURL, Clock: clock.Real{}, started: map[string]time.Time{}}, nil
}

// Pair is the private network of one sandbox and the two addresses on it.
type Pair struct {
	Network            string
	Subnet             netip.Prefix
	ProxyIP, SandboxIP netip.Addr
}

// PairFor gives sandbox number index its own /29 inside base. Addresses are
// chosen here and not by Docker, so the proxy's address is known before
// either container exists.
func PairFor(base netip.Prefix, index int, sandboxID string) Pair {
	first := base.Masked().Addr().As4()
	start := binary.BigEndian.Uint32(first[:]) + uint32(index)*8 // a /29 holds 8 addresses
	addr := func(offset uint32) netip.Addr {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], start+offset)
		return netip.AddrFrom4(b)
	}
	return Pair{Network: "tsi-sbx-" + sandboxID, Subnet: netip.PrefixFrom(addr(0), 29), ProxyIP: addr(1), SandboxIP: addr(2)}
}

// Up removes anything left from an earlier run, then starts every pair.
func (l *Launcher) Up(ctx context.Context, sc *scenario.Scenario) error {
	if err := l.Down(ctx); err != nil {
		return err
	}
	base := netip.MustParsePrefix(sc.Subnet) // validated when the scenario was loaded
	l.began = l.Clock.Now()
	index := 0
	for _, t := range sc.Tenants {
		for _, s := range t.Sandboxes {
			pair := PairFor(base, index, s.ID)
			index++
			if err := l.createNetwork(ctx, pair); err != nil {
				return fmt.Errorf("network for %s: %w", s.ID, err)
			}
			if err := l.startProxy(ctx, sc, t.ID, s.ID, pair); err != nil {
				return fmt.Errorf("proxy for %s: %w", s.ID, err)
			}
			env := []string{
				"TSI_TENANT_ID=" + t.ID, "TSI_SANDBOX_ID=" + s.ID, // for its own log lines only
				"TSI_BEHAVIOUR=" + s.Behaviour, "TSI_START=" + s.Start.String(), "TSI_RATE=" + strconv.Itoa(s.Rate),
			}
			if _, err := l.startSandbox(ctx, s.ID, pair, env); err != nil {
				return fmt.Errorf("sandbox %s: %w", s.ID, err)
			}
			// Start offsets in the scenario count from this moment.
			l.started[s.ID] = l.Clock.Now()
			slog.Debug("started pair", "tenant_id", t.ID, "sandbox_id", s.ID, "behaviour", s.Behaviour, "subnet", pair.Subnet.String())
		}
	}
	return nil
}

// createNetwork builds the sandbox's only network.
//   - Internal: no route out and no NAT.
//   - gateway mode isolated: the bridge gets no address on the host, so the
//     sandbox cannot reach services on the Docker host either.
//   - explicit subnet: Docker's default address pools are never consulted.
func (l *Launcher) createNetwork(ctx context.Context, pair Pair) error {
	_, err := l.cli.NetworkCreate(ctx, pair.Network, client.NetworkCreateOptions{
		Driver:   "bridge",
		Internal: true,
		IPAM:     &network.IPAM{Config: []network.IPAMConfig{{Subnet: pair.Subnet}}},
		Options:  map[string]string{"com.docker.network.bridge.gateway_mode_ipv4": "isolated"},
		Labels:   map[string]string{labelManaged: "1"},
	})
	return err
}

// startProxy attaches the proxy to three networks: its sandbox's, control
// (toward ingest and guard) and internet (toward the fake hosts).
func (l *Launcher) startProxy(ctx context.Context, sc *scenario.Scenario, tenantID, sandboxID string, pair Pair) error {
	env := []string{
		"TSI_TENANT_ID=" + tenantID, "TSI_SANDBOX_ID=" + sandboxID,
		"TSI_LISTEN_IP=" + pair.ProxyIP.String(), "TSI_PEER_IP=" + pair.SandboxIP.String(),
		"TSI_INGEST_URL=" + l.IngestURL, "TSI_GUARD_URL=" + l.GuardURL,
		"TSI_RATE=" + strconv.Itoa(sc.Limits.EventsPerSec), "TSI_BURST=" + strconv.Itoa(sc.Limits.Burst),
		"TSI_SITES_TRUSTED=" + strings.Join(sc.Sites.Trusted, ","),
		"TSI_SITES_WATCHED=" + strings.Join(sc.Sites.Watched, ","),
		"TSI_SITES_BAD=" + strings.Join(sc.Sites.Bad, ","),
	}
	_, err := l.start(ctx, "tsi-proxy-"+sandboxID, "proxy", sandboxID, env, controlNet, map[string]*network.EndpointSettings{
		pair.Network: {IPAMConfig: &network.EndpointIPAMConfig{IPv4Address: pair.ProxyIP}},
		controlNet:   {},
		internetNet:  {},
	})
	return err
}

// startSandbox attaches the sandbox to its private network and nothing else.
func (l *Launcher) startSandbox(ctx context.Context, sandboxID string, pair Pair, env []string) (string, error) {
	env = append(env,
		"TSI_EVENTS_URL=http://"+pair.ProxyIP.String()+":8080/v1/events",
		"TSI_EGRESS_PROXY=http://"+pair.ProxyIP.String()+":3128",
	)
	return l.start(ctx, "tsi-sandbox-"+sandboxID, "sandbox", sandboxID, env, pair.Network, map[string]*network.EndpointSettings{
		pair.Network: {IPAMConfig: &network.EndpointIPAMConfig{IPv4Address: pair.SandboxIP}},
	})
}

// start creates and starts one container with a small, locked-down runtime
// profile. Containers are still not a security boundary, see the README.
func (l *Launcher) start(ctx context.Context, name, role, sandboxID string, env []string, primaryNet string, endpoints map[string]*network.EndpointSettings) (string, error) {
	pids := int64(64)
	created, err := l.cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name: name,
		Config: &container.Config{
			Image: l.Image, Cmd: []string{role}, Env: env,
			Labels: map[string]string{labelManaged: "1", "tsi.role": role, "tsi.sandbox_id": sandboxID},
		},
		HostConfig: &container.HostConfig{
			// Spelled out, not left to the default, which is Docker's own
			// bridge with a route to the outside world. Engine 29 was seen
			// to attach only the networks listed below even without this
			// line, but isolation should not rest on how one engine version
			// reads an empty field. The integration test checks the result.
			NetworkMode:    container.NetworkMode(primaryNet),
			CapDrop:        []string{"ALL"},
			ReadonlyRootfs: true,
			SecurityOpt:    []string{"no-new-privileges"},
			Resources:      container.Resources{Memory: 64 << 20, PidsLimit: &pids},
		},
		NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: endpoints},
	})
	if err != nil {
		return "", err
	}
	_, err = l.cli.ContainerStart(ctx, created.ID, client.ContainerStartOptions{})
	return created.ID, err
}

// Down removes every container and network that carries the launch label.
func (l *Launcher) Down(ctx context.Context) error {
	managed := make(client.Filters).Add("label", labelManaged+"=1")
	containers, err := l.cli.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: managed})
	if err != nil {
		return err
	}
	for _, c := range containers.Items {
		if _, err := l.cli.ContainerRemove(ctx, c.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
			return err
		}
	}
	networks, err := l.cli.NetworkList(ctx, client.NetworkListOptions{Filters: managed})
	if err != nil {
		return err
	}
	for _, n := range networks.Items {
		if _, err := l.cli.NetworkRemove(ctx, n.ID, client.NetworkRemoveOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// Kill stops one sandbox and its proxy. It is the guard's Runtime in the
// demo: the code that knows how a pair was built also takes it down. The
// containers stay, stopped, so their logs can still be read.
func (l *Launcher) Kill(ctx context.Context, sandboxID string) error {
	// The sandbox first, so it is never running without its proxy. Both are
	// tried whatever happens to the first, and one that already stopped is
	// not an error: a sandbox that crashed must not leave its proxy running.
	var errs []error
	for _, name := range []string{"tsi-sandbox-" + sandboxID, "tsi-proxy-" + sandboxID} {
		ctx, cancel := context.WithTimeout(ctx, killEach)
		info, err := l.cli.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
		if err == nil && info.Container.State.Running {
			_, err = l.cli.ContainerKill(ctx, name, client.ContainerKillOptions{Signal: "SIGKILL"})
		}
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("kill %s: %w", name, err))
		}
	}
	return errors.Join(errs...) // nil when there were none
}
