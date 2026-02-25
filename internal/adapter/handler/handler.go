package handler

import (
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/service"
	"analytics-aggregator/internal/ctxkey"
	"context"
	"net/http"
)

type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

type Handler struct {
	aar    *service.AnalyticsAggregatorService
	config *config.Config
}

func NewHandler(aar *service.AnalyticsAggregatorService, cfg *config.Config) *Handler {
	return &Handler{
		aar:    aar,
		config: cfg,
	}
}

func (h *Handler) MakeHandler(fn HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			// centralized error handling
			h.HandleError(w, r, err)
		}
	}
}

// GetIdempotencyKey safely retrieves the key from context
func GetIdempotencyKey(ctx context.Context) string {
	val, ok := ctx.Value(ctxkey.IdempotencyKey).(string)
	if !ok {
		return ""
	}
	return val
}
