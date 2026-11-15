// Package fakenet is the pretend internet. One server answers for every
// host name, so the demo needs no real outbound traffic.
package fakenet

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/httpx"
)

const maxBody = 16 << 20

func Run(ctx context.Context, addr string) error {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read and discard the upload, so the sender's bytes really travel.
		n, _ := io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, maxBody))
		fmt.Fprintf(w, "ok %s got %d bytes\n", r.Host, n)
	})
	srv := &http.Server{Addr: addr, Handler: handler, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second}
	return httpx.Serve(ctx, srv)
}
