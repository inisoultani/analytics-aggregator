package port

import "errors"

var (
	ErrRateLimited   = errors.New("external service rate limit exceeded")
	ErrTransient     = errors.New("transient error, safe to retry")
	ErrUnrecoverable = errors.New("unrecoverable error, do not retry")
)
