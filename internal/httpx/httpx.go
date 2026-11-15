// Package httpx holds the few HTTP helpers every role shares: JSON replies,
// refusals with a reason code, and running servers until shutdown.
package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/joachimzetterman/tiny-sandbox-infra/internal/event"
)

// Refusal is the body of every non-2xx reply.
type Refusal struct {
	Reason            event.Reason `json:"reason"`
	RetryAfterSeconds int          `json:"retry_after_seconds,omitempty"`
	// Rejected lists events in the request that would be refused anyway, so
	// a producer told to retry can fix them first.
	Rejected []event.Rejected `json:"rejected,omitempty"`
}

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the client hung up if this fails, nothing to do
}

// Refuse replies with a reason code. A positive retryAfter also sets the
// Retry-After header, rounded up so a client never retries too early.
func Refuse(w http.ResponseWriter, status int, reason event.Reason, retryAfter time.Duration, rejected ...event.Rejected) {
	body := Refusal{Reason: reason, Rejected: rejected}
	if retryAfter > 0 {
		body.RetryAfterSeconds = int(math.Ceil(retryAfter.Seconds()))
		w.Header().Set("Retry-After", strconv.Itoa(body.RetryAfterSeconds))
	}
	WriteJSON(w, status, body)
}

// Serve runs the servers until ctx is cancelled or one of them fails.
// Go idiom: a context carries cancellation; every blocking call that takes
// one stops when it is cancelled.
func Serve(ctx context.Context, servers ...*http.Server) error {
	// Go idiom: a channel is a typed queue between goroutines (lightweight
	// threads). The buffer lets every server report without blocking.
	errc := make(chan error, len(servers))
	for _, s := range servers {
		go func() { errc <- s.ListenAndServe() }()
	}
	var err error
	// Go idiom: select waits for whichever channel is ready first.
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	stop, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, s := range servers {
		_ = s.Shutdown(stop)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
