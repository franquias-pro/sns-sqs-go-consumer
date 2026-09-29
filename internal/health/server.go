package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

type Pinger interface { Ping(context.Context) error }

type Handler struct {
	ready atomic.Bool
	dependency Pinger
}

func New(dependency Pinger) *Handler { return &Handler{dependency: dependency} }
func (h *Handler) SetReady(ready bool) { h.ready.Store(ready) }

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !h.ready.Load() { http.Error(w, "not ready", http.StatusServiceUnavailable); return }
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := h.dependency.Ping(ctx); err != nil {
			http.Error(w, "MongoDB unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}
