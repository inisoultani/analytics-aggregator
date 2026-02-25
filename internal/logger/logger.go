package logger

import (
	"analytics-aggregator/internal/ctxkey"
	"context"
	"log/slog"
	"os"

	"github.com/go-chi/chi/v5/middleware"
)

type LogContextHandler struct {
	slog.Handler
}

func (l *LogContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if reqId := middleware.GetReqID(ctx); reqId != "" {
		r.AddAttrs(slog.String(string(ctxkey.RequestID), reqId))
	}
	return l.Handler.Handle(ctx, r)
}

func NewLogger(isProduction bool) (*slog.Logger, *slog.LevelVar) {
	logLevel := &slog.LevelVar{}
	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	if isProduction {
		handler = slog.NewJSONHandler(os.Stdout, opts)
		logLevel.Set(slog.LevelInfo)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
		logLevel.Set(slog.LevelDebug)
	}

	wrapperHandler := &LogContextHandler{
		Handler: handler,
	}

	logger := slog.New(wrapperHandler)
	slog.SetDefault(logger)

	return logger, logLevel
}
