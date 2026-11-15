package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
	"github.com/joachimzetterman/tiny-sandbox-infra/internal/scenario"
)

// egressSetup runs the egress proxy on a real socket in front of one
// upstream server. Every host name is dialled to that upstream, so the
// test can use the names from the site classes.
func egressSetup(t *testing.T, upstream *httptest.Server, sites scenario.Sites) (*Proxy, *http.Client) {
	t.Helper()
	p, _ := newProxy(func(c *Config) { c.PeerIP = "127.0.0.1"; c.Sites = sites })
	p.dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, upstream.Listener.Addr().String())
	}
	front := httptest.NewServer(p.EgressHandler())
	t.Cleanup(front.Close)
	proxyURL, _ := url.Parse(front.URL)
	return p, &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
}

var testSites = scenario.Sites{Trusted: []string{"allowed.test"}, Watched: []string{"paste.test"}, Bad: []string{"evil.test"}}

// countingUpstream answers every request and counts how many arrived.
func countingUpstream(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, "hello from "+r.Host)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestTrustedSiteIsForwardedAndCounted(t *testing.T) {
	upstream, hits := countingUpstream(t)
	p, client := egressSetup(t, upstream, testSites)
	resp, err := client.Get("http://allowed.test/page")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "hello from allowed.test" || hits.Load() != 1 {
		t.Fatalf("got %d %q with %d upstream hits", resp.StatusCode, body, hits.Load())
	}
	c := p.Counters()
	if c.EgressBytes["allowed.test"] < uint64(len(body)) || c.EgressByClass[scenario.ClassTrusted] != c.EgressBytes["allowed.test"] {
		t.Errorf("bytes not counted: %+v", c)
	}
}

func TestDataInTheURLAndHeadersIsCounted(t *testing.T) {
	upstream, _ := countingUpstream(t)
	p, client := egressSetup(t, upstream, testSites)
	req, _ := http.NewRequest(http.MethodGet, "http://paste.test/?q="+strings.Repeat("a", 100_000), nil)
	req.Header.Set("X-Data", strings.Repeat("b", 200_000))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := p.Counters().EgressByClass[scenario.ClassWatched]; got < 300_000 {
		t.Errorf("watched bytes = %d, although 300,000 left in the URL and a header", got)
	}
}

func TestHostNamesIgnoreCaseAndTrailingDot(t *testing.T) {
	upstream, hits := countingUpstream(t)
	p, client := egressSetup(t, upstream, testSites)
	for _, target := range []string{"http://Allowed.Test/", "http://allowed.test./"} {
		resp, err := client.Get(target)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: got %d, want it forwarded", target, resp.StatusCode)
		}
	}
	c := p.Counters()
	if hits.Load() != 2 || len(c.EgressBytes) != 1 || c.EgressBytes["allowed.test"] == 0 {
		t.Errorf("%d upstream hits, bytes by domain %v: want 2 hits counted under one name", hits.Load(), c.EgressBytes)
	}
}

func TestUploadOverTheLimitIsRefusedAsTooLarge(t *testing.T) {
	tooBig := strings.Repeat("x", maxEgressBody+10)
	cases := []struct {
		name        string
		body        io.Reader
		knownLength bool
	}{
		{"length known up front", strings.NewReader(tooBig), true},
		// Wrapping hides the length, so the client streams it in chunks.
		{"length unknown", io.MultiReader(strings.NewReader(tooBig)), false},
	}
	for _, c := range cases {
		upstream, hits := countingUpstream(t)
		p, client := egressSetup(t, upstream, testSites)
		// Expect: 100-continue makes the client wait for the proxy's verdict
		// before it sends the body, as a careful uploader would.
		client.Transport.(*http.Transport).ExpectContinueTimeout = 5 * time.Second
		req, _ := http.NewRequest(http.MethodPost, "http://paste.test/upload", c.body)
		req.Header.Set("Expect", "100-continue")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		reply, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge || !strings.Contains(string(reply), string(event.BodyTooLarge)) {
			t.Errorf("%s: got %d %s, want 413 body_too_large", c.name, resp.StatusCode, reply)
		}
		// When the length is declared, nothing at all may leave.
		if got := p.Counters(); c.knownLength && (hits.Load() != 0 || len(got.EgressBytes) != 0 || got.Blocked[event.BodyTooLarge] != 1) {
			t.Errorf("%s: %d upstream hits, counters %+v: want it refused before anything left", c.name, hits.Load(), got)
		}
	}
}

func TestBadAndUnlistedSitesAreBlockedAndCounted(t *testing.T) {
	cases := []struct {
		target string
		reason event.Reason
	}{
		{"http://evil.test/c2", event.SiteBad},
		{"http://EVIL.TEST/c2", event.SiteBad},  // a bad site stays bad in capitals
		{"http://evil.test./c2", event.SiteBad}, // and with a trailing dot
		{"http://never-heard-of-it.test/", event.SiteUnlisted},
		{"http://10.0.0.5:8123/", event.SiteUnlisted},          // an address is not a listed name
		{"http://allowed.test.evil.test/", event.SiteUnlisted}, // no suffix or prefix matching
	}
	for _, c := range cases {
		upstream, hits := countingUpstream(t)
		p, client := egressSetup(t, upstream, testSites)
		resp, err := client.Get(c.target)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(body), string(c.reason)) {
			t.Errorf("%s: got %d %s, want 403 %s", c.target, resp.StatusCode, body, c.reason)
		}
		if hits.Load() != 0 {
			t.Errorf("%s: the request reached the upstream", c.target)
		}
		if got := p.Counters(); got.Blocked[c.reason] != 1 || len(got.EgressBytes) != 0 {
			t.Errorf("%s: counters wrong: %+v", c.target, got)
		}
	}
}

func TestHTTPSTunnelIsAllowedByClassAndCounted(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "secret")
	}))
	defer upstream.Close()
	// The test certificate is issued for example.com.
	p, client := egressSetup(t, upstream, scenario.Sites{Trusted: []string{"example.com"}})
	pool := x509.NewCertPool()
	pool.AddCert(upstream.Certificate())
	client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{RootCAs: pool}

	resp, err := client.Get("https://example.com/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "secret" {
		t.Fatalf("tunnel returned %q", body)
	}
	client.CloseIdleConnections() // ends the tunnel, so its bytes are all counted
	if got := p.Counters().EgressBytes["example.com"]; got < 500 {
		t.Errorf("counted %d tunnel bytes, a TLS handshake alone is more than 500", got)
	}

	// A tunnel to a site in no class is refused before any connection is made.
	if _, err := client.Get("https://not-listed.test/"); err == nil || !strings.Contains(err.Error(), "Forbidden") {
		t.Errorf("tunnel to an unlisted site: err = %v, want 403 Forbidden", err)
	}
	if p.Counters().Blocked[event.SiteUnlisted] != 1 {
		t.Error("blocked tunnel was not counted")
	}
}
