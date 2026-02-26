package handler

import (
	"analytics-aggregator/internal/core/domain"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

func (h *Handler) ProcessAndStoreEvents(w http.ResponseWriter, r *http.Request) error {
	var processEventReq ProcessEventRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&processEventReq); err != nil {
		return BadRequest(fmt.Sprintf("Invalid request payload : %v", err), err)
	}

	if processEventReq.EventType == "" {
		return BadRequest("event_type is required", errors.New("mandatory field not provided"))
	}

	rawDataBytes, err := json.Marshal(processEventReq)
	if err != nil {
		return InternalError(fmt.Sprintf("Failed to marshal raw data : %v", rawDataBytes), err)
	}
	event := domain.Event{
		ID:       uuid.New(),
		RawData:  rawDataBytes,
		ClientIP: processEventReq.ClientIP,
	}

	affectedRecs, err := h.service.ProcessAndStore(r.Context(), []domain.Event{event})
	if err != nil {
		return InternalError("pipeline ingestion failed", err)
	}

	resp := ProcessEventResponse{
		Status:  "accepted",
		Message: fmt.Sprintf("%d event queued for processing", affectedRecs),
		Id:      event.ID.String(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	return json.NewEncoder(w).Encode(resp)
}
