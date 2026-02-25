package handler

import (
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/service"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func NewRouter(ps *service.PipelineService, cfg *config.Config) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(LoggerMiddleware)
	r.Use(middleware.RealIP)

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	r.Route("/event", func(r chi.Router) {
		h := NewHandler(ps, cfg)

		r.Post("/", h.MakeHandler(h.ProcessAndStoreEvents))
		// r.Get("/{loanID}", h.MakeHandler(h.GetLoanByID))
		// r.Get("/{loanID}/outstanding", h.MakeHandler(h.GetOutstanding))
		// r.Get("/{loanID}/payment", h.MakeHandler(h.ListPayments))
		// r.Get("/{loanID}/schedule", h.MakeHandler(h.ListSchedules))

		// r.Group(func(r chi.Router) {
		// 	r.Use(billingApiMiddleware.IdempotencyMiddleware)
		// 	r.Post("/{loanID}/payment", h.MakeHandler(h.MakePayment))
		// })
		// r.Group(func(r chi.Router) {
		// 	// later we can put specific auth middleware here
		// 	r.Post("/admin/log-level", h.MakeHandler(h.ChangeLogLevel(cfg.LogLevel)))
		// })
	})

	return r
}
