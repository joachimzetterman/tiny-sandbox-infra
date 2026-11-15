package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/httpx"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

// handleEgress is a forward proxy for the sandbox's outbound calls. It
// decides by site class, then either forwards a plain HTTP request or opens
// a tunnel for HTTPS (CONNECT).
func (p *Proxy) handleEgress(w http.ResponseWriter, r *http.Request) {
	// For CONNECT the URL is just "host:port", which parses the same way.
	// DNS names ignore case and a trailing dot, so the lookup must too.
	host := strings.TrimSuffix(strings.ToLower(r.URL.Hostname()), ".")
	class, listed := p.classes[host]
	if !listed {
		class = scenario.ClassUnlisted
	}
	switch class {
	case scenario.ClassBad:
		p.block(w, http.StatusForbidden, event.SiteBad, host)
	case scenario.ClassUnlisted:
		p.block(w, http.StatusForbidden, event.SiteUnlisted, host)
	default:
		add := func(n int) {
			p.count(func(c *Counters) { c.EgressBytes[host] += uint64(n); c.EgressByClass[class] += uint64(n) })
		}
		if r.Method == http.MethodConnect {
			p.tunnel(w, r, add)
			return
		}
		if r.ContentLength > maxEgressBody {
			// Refused before a single byte leaves, and with the real reason.
			p.block(w, http.StatusRequestEntityTooLarge, event.BodyTooLarge, host)
			return
		}
		// Data can leave in the URL or a header as well as in the body.
		add(headSize(r))
		// Bound the whole exchange: request body, reply body and time.
		ctx, cancel := context.WithTimeout(r.Context(), egressTimeout)
		defer cancel()
		r.Body = counted{http.MaxBytesReader(w, r.Body, maxEgressBody), add}
		p.forward.ServeHTTP(&countedWriter{w, add, maxEgressReply}, r.WithContext(ctx))
	}
}

// block refuses an outbound call before anything leaves, and counts it.
func (p *Proxy) block(w http.ResponseWriter, status int, reason event.Reason, host string) {
	p.count(func(c *Counters) { c.Blocked[reason]++ })
	slog.Info("egress blocked", "reason", reason, "host", host)
	httpx.Refuse(w, status, reason, 0)
}

// headSize is the size of the request line and headers as the sandbox sent them.
func headSize(r *http.Request) int {
	n := len(r.Method) + len(r.URL.String())
	for name, values := range r.Header {
		for _, v := range values {
			n += len(name) + len(v)
		}
	}
	return n
}

// newForwarder builds the plain HTTP path. ReverseProxy from the standard
// library already strips hop-by-hop headers and streams bodies, so the only
// thing left to say is where requests go: wherever the sandbox asked.
func (p *Proxy) newForwarder() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(*httputil.ProxyRequest) {}, // a proxy request already carries the full target URL
		Transport: &http.Transport{
			DialContext:           func(ctx context.Context, network, addr string) (net.Conn, error) { return p.dial(ctx, network, addr) },
			ResponseHeaderTimeout: 10 * time.Second,
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Info("egress failed", "host", r.URL.Hostname(), "err", err)
			// An upload of unknown length that ran past the limit: the remote
			// host is fine, and sending the same request again cannot work.
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				p.count(func(c *Counters) { c.Blocked[event.BodyTooLarge]++ })
				httpx.Refuse(w, http.StatusRequestEntityTooLarge, event.BodyTooLarge, 0)
				return
			}
			httpx.Refuse(w, http.StatusBadGateway, event.UpstreamError, 0)
		},
	}
}

// tunnel serves CONNECT: it dials the target and copies bytes both ways.
// The proxy cannot see inside TLS, so it counts bytes and nothing else.
func (p *Proxy) tunnel(w http.ResponseWriter, r *http.Request, add func(int)) {
	up, err := p.dial(r.Context(), "tcp", r.Host)
	if err != nil {
		httpx.Refuse(w, http.StatusBadGateway, event.UpstreamError, 0)
		return
	}
	defer up.Close()
	// Hijack takes the raw TCP connection away from the HTTP server.
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		httpx.Refuse(w, http.StatusBadGateway, event.UpstreamError, 0)
		return
	}
	down, _, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer down.Close()
	// Socket deadlines are wall-clock by nature, so this is the one place
	// that reads the real time. It bounds how long a tunnel can live.
	deadline := time.Now().Add(tunnelLifetime)
	_ = up.SetDeadline(deadline)
	_ = down.SetDeadline(deadline)
	if _, err := io.WriteString(down, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(up, counted{down, add})
		_ = up.Close() // unblocks the copy below when the sandbox hangs up
		close(done)
	}()
	_, _ = io.Copy(down, counted{up, add})
	_ = down.Close() // unblocks the goroutine when the remote hangs up
	<-done           // wait, so the goroutine never outlives the request
}

// counted reports every byte read through it.
type counted struct {
	io.ReadCloser
	add func(int)
}

func (c counted) Read(b []byte) (int, error) {
	n, err := c.ReadCloser.Read(b)
	c.add(n)
	return n, err
}

// countedWriter reports reply bytes and cuts the reply off at a limit.
// Go idiom: embedding http.ResponseWriter gives this type all its methods;
// only Write is replaced.
type countedWriter struct {
	http.ResponseWriter
	add  func(int)
	left int64
}

func (c *countedWriter) Write(b []byte) (int, error) {
	if c.left -= int64(len(b)); c.left < 0 {
		return 0, io.ErrShortWrite // makes the forwarder abort the reply
	}
	c.add(len(b))
	return c.ResponseWriter.Write(b)
}
