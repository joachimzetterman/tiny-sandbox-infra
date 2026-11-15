package sandbox

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"strings"
	"time"
)

// ProbeResult is one line of probe output.
type ProbeResult struct {
	Name      string `json:"name"`
	Reachable bool   `json:"reachable"`
}

// Probe is used by the isolation tests. It runs where a sandbox runs and
// reports what that position can reach. Each target is "name=host:port"
// for a TCP connect, or "name=dns:host" for a name lookup.
func Probe(ctx context.Context, targets []string, out io.Writer) error {
	enc := json.NewEncoder(out)
	for _, target := range targets {
		name, addr, _ := strings.Cut(target, "=")
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var err error
		if host, isDNS := strings.CutPrefix(addr, "dns:"); isDNS {
			_, err = net.DefaultResolver.LookupHost(ctx, host)
		} else {
			var conn net.Conn
			if conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", addr); err == nil {
				conn.Close()
			}
		}
		cancel()
		if err := enc.Encode(ProbeResult{Name: name, Reachable: err == nil}); err != nil {
			return err
		}
	}
	return nil
}
