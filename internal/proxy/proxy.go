// Package proxy is the trust boundary in front of one sandbox. It has two
// jobs: an event gateway (gateway.go) and an egress proxy (egress.go).
// Nothing the sandbox says about itself is believed past this point.
package proxy

import (
	"context"
	"maps"
	"net"
	"net/http"
	"net/http/httputil"
	"sync"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/clock"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/control"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/httpx"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

type Config struct {
	// TenantID and SandboxID are set by whoever started the proxy. They
	// are stamped on every event, whatever the sandbox claims.
	TenantID, SandboxID string
	// ListenIP is this proxy's address on the sandbox network. Listening
	// only there means nothing on the other networks can connect.
	ListenIP string
	// PeerIP is the sandbox. Any other caller is refused.
	PeerIP    string
	IngestURL string
	// GuardURL is where heartbeats go. Empty means no guard: the static
	// limits are then the only limits.
	GuardURL string
	Rate     float64 // events per second
	Burst    int
	Sites    scenario.Sites
	Clock    clock.Clock

	// Bounds. Zero means the default set in New.
	QueueLimit   int           // events buffered toward ingest
	QueueBytes   int           // and their size in memory, whichever is hit first
	ForwardBatch int           // events per request to ingest
	ForwardEvery time.Duration // how often the buffer is drained
	Heartbeat    time.Duration // how often counters go to the guard
}

const (
	maxEventBody   = 1 << 20  // bytes in one request to the event gateway
	maxEgressBody  = 8 << 20  // bytes in one outbound request body
	maxEgressReply = 32 << 20 // bytes in one outbound response body
	egressTimeout  = 30 * time.Second
	tunnelLifetime = 60 * time.Second
	dialTimeout    = 5 * time.Second
	ingestTimeout  = 2 * time.Second
	guardTimeout   = 800 * time.Millisecond // shorter than one heartbeat, so they never pile up
	drainTimeout   = 3 * time.Second        // inside the 10 s that docker stop allows
)

type Proxy struct {
	cfg     Config
	bucket  *bucket
	queue   *event.Queue
	classes map[string]string      // domain to site class
	ingest  *http.Client           // toward ingest
	guard   *http.Client           // toward the guard
	forward *httputil.ReverseProxy // toward the fake internet
	// dial opens outbound connections. Tests replace it to send every
	// host name to a local server.
	dial func(ctx context.Context, network, addr string) (net.Conn, error)

	started time.Time // when the counters began at zero, sent with every heartbeat

	mu     sync.Mutex // guards c and policy
	c      control.Counters
	policy control.Policy // the guard's last word, see heartbeat.go
}

func New(cfg Config) *Proxy {
	if cfg.QueueLimit == 0 {
		cfg.QueueLimit = 5000
	}
	if cfg.QueueBytes == 0 {
		cfg.QueueBytes = 8 << 20
	}
	if cfg.ForwardBatch == 0 {
		// 500 events of the largest size, fully JSON-escaped, still fit in
		// the request body limit of ingest.
		cfg.ForwardBatch = 500
	}
	if cfg.ForwardEvery == 0 {
		cfg.ForwardEvery = 200 * time.Millisecond
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = time.Second
	}
	p := &Proxy{
		cfg:     cfg,
		bucket:  newBucket(cfg.Clock, cfg.Rate, cfg.Burst),
		queue:   event.NewQueue(cfg.QueueLimit, cfg.QueueBytes),
		classes: cfg.Sites.Classes(),
		ingest:  &http.Client{Timeout: ingestTimeout},
		guard:   &http.Client{Timeout: guardTimeout},
		dial:    (&net.Dialer{Timeout: dialTimeout}).DialContext,
		started: cfg.Clock.Now(),
		c: control.Counters{
			Rejects: map[event.Reason]uint64{}, EgressBytes: map[string]uint64{},
			EgressByClass: map[string]uint64{}, Blocked: map[event.Reason]uint64{},
		},
	}
	p.forward = p.newForwarder()
	return p
}

// Counters returns a copy that is safe to read while the proxy keeps running.
func (p *Proxy) Counters() control.Counters {
	p.mu.Lock()
	defer p.mu.Unlock()
	c := p.c
	c.Rejects, c.Blocked = maps.Clone(p.c.Rejects), maps.Clone(p.c.Blocked)
	c.EgressBytes, c.EgressByClass = maps.Clone(p.c.EgressBytes), maps.Clone(p.c.EgressByClass)
	return c
}

// count runs fn with the counters locked.
func (p *Proxy) count(fn func(c *control.Counters)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fn(&p.c)
}

// onlyPeer refuses every caller except this proxy's own sandbox. The network
// layout already guarantees this, the check is a second line of defence.
func (p *Proxy) onlyPeer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if host, _, _ := net.SplitHostPort(r.RemoteAddr); host != p.cfg.PeerIP {
			httpx.Refuse(w, http.StatusForbidden, event.ForbiddenPeer, 0)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// EventsHandler and EgressHandler are exported so tests can serve them
// without real network addresses.
func (p *Proxy) EventsHandler() http.Handler { return p.onlyPeer(http.HandlerFunc(p.handleEvents)) }
func (p *Proxy) EgressHandler() http.Handler { return p.onlyPeer(http.HandlerFunc(p.handleEgress)) }

// Run serves both listeners and forwards the buffer until ctx is cancelled.
func (p *Proxy) Run(ctx context.Context) error {
	return p.serve(ctx, net.JoinHostPort(p.cfg.ListenIP, "8080"), net.JoinHostPort(p.cfg.ListenIP, "3128"))
}

// serve is Run with the two addresses spelled out, so a test can pick free ports.
func (p *Proxy) serve(ctx context.Context, eventsAddr, egressAddr string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	forwarding := make(chan struct{})
	// Go idiom: "go" starts a goroutine. This one ends when ctx is cancelled,
	// and closing the channel tells Run that it has.
	go func() { p.forwardLoop(ctx); close(forwarding) }()
	if p.cfg.GuardURL != "" {
		go p.heartbeatLoop(ctx)
	}
	events := &http.Server{
		Addr: eventsAddr, Handler: p.EventsHandler(),
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
	}
	// No write timeout on egress: tunnels are long lived and bounded by
	// tunnelLifetime instead.
	egress := &http.Server{
		Addr: egressAddr, Handler: p.EgressHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	err := httpx.Serve(ctx, events, egress)
	// The listeners are closed, so nothing new can arrive. Events the
	// sandbox already got a 200 for are handed over before the process ends.
	cancel()
	<-forwarding
	p.drain()
	return err
}
