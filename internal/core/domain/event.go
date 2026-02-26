package domain

import "github.com/google/uuid"

// Event represents our core business entity.
type Event struct {
	ID           uuid.UUID // Uses standard google/uuid
	RawData      []byte    // Storing JSON as bytes
	EnrichedData []byte    // Storing JSON as bytes
	ClientIP     string

	// Transient Pipeline State (Not saved to the main DB table)
	RetryCount  int
	ErrorReason string
}
