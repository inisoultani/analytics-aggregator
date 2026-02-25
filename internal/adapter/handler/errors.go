package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
)

type AppError struct {
	Code    int    // we defind HTTP Status code here
	Message string // the message FE sees
	Err     error  // actual technical issue that captured
}

func (e *AppError) Error() string {
	return e.Message
}

// we'll reuse this within handler as default return object for bad request 400
func BadRequest(msg string, internalError error) error {
	return &AppError{
		Code:    http.StatusBadRequest,
		Message: msg,
		Err:     internalError,
	}
}

// we'll reuse this within handler as default return object for internal error 500
func InternalError(msg string, internalError error) error {
	return &AppError{
		Code:    http.StatusInternalServerError,
		Message: msg,
		Err:     internalError,
	}
}

func logError(r *http.Request, msg string, err error) {
	slog.ErrorContext(r.Context(), msg,
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Any("err", err),
	)
}

func (h *Handler) HandleError(w http.ResponseWriter, r *http.Request, err error) {

	// handle predefined AppError type
	var appError *AppError
	if errors.As(err, &appError) {
		logError(r, "app_error", err)
		http.Error(w, appError.Message, appError.Code)
		return
	}

	// Check if the error string contains our custom repo prefix
	// Or check if it's a context error
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "repo_timeout") {
		logError(r, "request_timeout", err)
		http.Error(w, "Database timeout", http.StatusGatewayTimeout)
		return
	}

	switch {

	default:
		logError(r, "internal_server_error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}
