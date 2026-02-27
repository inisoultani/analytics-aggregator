package domain

import "github.com/google/uuid"

type DeadLetterEvent struct {
	ID          uuid.UUID
	RawData     []byte
	ErrorReason string
}
