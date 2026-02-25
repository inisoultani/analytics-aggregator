package handler

import (
	"encoding/json"
)

// ProcessEventRequest is our HTTP-specific DTO.
// Using json.RawMessage allows us to capture arbitrary, nested JSON
// in the "payload" field without needing to unmarshal it into strict structs yet.
type ProcessEventRequest struct {
	UserID    string          `json:"user_id"`
	EventType string          `json:"event_type"`
	ClientIP  string          `json:"client_ip"`
	Payload   json.RawMessage `json:"payload"`
}
