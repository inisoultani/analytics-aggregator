package handler

import "net/http"

func (h *Handler) ProcessAndStoreEvents(w http.ResponseWriter, r *http.Request) error {

	h.aar.ProcessAndStore(r.Context(), nil)

	return nil
}
