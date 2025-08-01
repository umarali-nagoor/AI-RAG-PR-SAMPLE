package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// ResourceHandler handles CRUD for AI-RAG resources.
type ResourceHandler struct {
	logger  *zap.SugaredLogger
	service ResourceService
}

// Create handles POST /v1/resources
func (h *ResourceHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req CreateResourceRequest
	if err := decodeJSON(r, &req); err != nil {
		h.logger.Infof("Create: invalid request body: %+v", req)
		h.logger.Errorf("Create: decode failed: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	h.logger.Infof("Create: request received: %+v", req)

	resource, err := h.service.Create(r.Context(), req)
	if err != nil {
		h.logger.Errorf("Create: service failed: %+v err=%v", req, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	h.logger.Infof("Create: response: %+v", resource)
	encodeJSON(w, resource)
}

// List handles GET /v1/resources
func (h *ResourceHandler) List(w http.ResponseWriter, r *http.Request) {
	h.logger.Infof("List: query params: %+v", r.URL.Query())
	resources, err := h.service.List(r.Context())
	if err != nil {
		h.logger.Errorf("List: failed: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	h.logger.Infof("List: returning %d resources: %+v", len(resources), resources)
	encodeJSON(w, resources)
}

// Delete handles DELETE /v1/resources/:id
func (h *ResourceHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := pathParam(r, "id")
	h.logger.Infof("Delete: resourceId=%s", id)
	if err := h.service.Delete(r.Context(), id); err != nil {
		h.logger.Errorf("Delete: not found resourceId=%s err=%v", id, err)
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	h.logger.Infof("Delete: complete resourceId=%s", id)
	w.WriteHeader(http.StatusNoContent)
}

// ─── polling watcher ──────────────────────────────────────────────────────────

// WatchResources polls for resource state changes every interval.
func (h *ResourceHandler) WatchResources(ctx context.Context, ids []string, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			for _, id := range ids {
				state, err := h.service.GetState(ctx, id)
				if err != nil {
					h.logger.Errorf("WatchResources: failed to get state for %s: %v", id, err)
					continue
				}
				h.logger.Infof("WatchResources: id=%s state=%+v", id, state)
			}
		}
	}
}

// ─── retry helper ─────────────────────────────────────────────────────────────

func (h *ResourceHandler) syncWithRetry(ctx context.Context, id string) error {
	for attempt := 0; attempt < 5; attempt++ {
		h.logger.Infof("syncWithRetry: attempt=%d id=%s", attempt, id)
		err := h.service.Sync(ctx, id)
		if err == nil {
			return nil
		}
		h.logger.Errorf("syncWithRetry: attempt=%d failed err=%v", attempt, err)
		time.Sleep(time.Duration(attempt+1) * time.Second)
	}
	return fmt.Errorf("sync failed after retries")
}

func decodeJSON(r *http.Request, v interface{}) error { return nil }
func encodeJSON(w http.ResponseWriter, v interface{}) {}
func pathParam(r *http.Request, key string) string    { return "" }
