package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// turnStreamKeepalive is how often an idle stream emits an SSE comment.
//
// Not cosmetic. A 397B coder decodes at roughly 14 tok/s, so multi-minute gaps
// between turns are normal, and an idle connection can otherwise be dropped by
// a proxy or the client's own timeout. The comment keeps it alive and is
// ignored by every SSE client.
const turnStreamKeepalive = 30 * time.Second

// Handler serves the stream as Server-Sent Events: the replay buffer first,
// then live turns, until the client disconnects.
//
// SSE rather than a WebSocket deliberately. The traffic is one-directional,
// SSE survives proxies without an upgrade dance, and `curl -N` is a first-class
// client, which makes the endpoint useful before any UI exists.
func (s *TurnStream) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		// Proxies that buffer would defeat the entire point.
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		sub, cancel := s.subscribe(true)
		defer cancel()

		ticker := time.NewTicker(turnStreamKeepalive)
		defer ticker.Stop()

		enc := json.NewEncoder(w)
		for {
			select {
			case <-r.Context().Done():
				return
			case ev, open := <-sub.ch:
				if !open {
					return
				}
				if _, err := w.Write([]byte("data: ")); err != nil {
					return
				}
				// Encode writes a trailing newline; SSE needs a blank line to
				// terminate the frame, hence the second one below.
				if err := enc.Encode(ev); err != nil {
					return
				}
				if _, err := w.Write([]byte("\n")); err != nil {
					return
				}
				flusher.Flush()
			case ev, open := <-sub.progress:
				if !open {
					return
				}
				if _, err := w.Write([]byte("event: progress\ndata: ")); err != nil {
					return
				}
				if err := enc.Encode(ev); err != nil {
					return
				}
				if _, err := w.Write([]byte("\n")); err != nil {
					return
				}
				flusher.Flush()
			case <-ticker.C:
				if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

// TurnStreamServer serves a TurnStream over HTTP as a controller-runtime
// Runnable, so the manager owns its lifecycle and shuts it down with everything
// else. Mirrors how the audit reaper is registered.
type TurnStreamServer struct {
	// Addr is the bind address. Empty or "0" disables the server, matching
	// the convention the metrics flag already uses.
	Addr   string
	Stream *TurnStream
}

// NeedLeaderElection reports false: this serves the LOCAL agent's own run, so
// every replica must serve its own stream rather than only the leader.
func (srv *TurnStreamServer) NeedLeaderElection() bool { return false }

// Start satisfies manager.Runnable.
func (srv *TurnStreamServer) Start(ctx context.Context) error {
	if srv.Addr == "" || srv.Addr == "0" || srv.Stream == nil {
		<-ctx.Done()
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/stream", srv.Stream.Handler())

	h := &http.Server{
		Addr:    srv.Addr,
		Handler: mux,
		// No WriteTimeout: an SSE response is open for the life of the run,
		// and a write deadline would sever it mid-stream.
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := h.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return h.Shutdown(shutdownCtx)
	}
}
