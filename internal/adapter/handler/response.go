package handler

type ProcessEventResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Id      string `json:"id"`
}
