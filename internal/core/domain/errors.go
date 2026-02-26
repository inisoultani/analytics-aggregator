package domain

import "errors"

var (
	ErrFailedToPushToPipeline = errors.New("Time out when try to push new event to the pipeline")
)
