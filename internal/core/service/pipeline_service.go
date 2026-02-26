package service

import (
	"analytics-aggregator/internal/core/domain"
	"analytics-aggregator/internal/core/port"
	"context"
	"log/slog"
)

type PipelineService struct {
	txManager         port.TxManager
	enrichmentService port.EnrichmentService
}

func NewPipelineService(txManager port.TxManager, enrichmentService port.EnrichmentService) *PipelineService {
	return &PipelineService{
		txManager:         txManager,
		enrichmentService: enrichmentService,
	}
}

func (p *PipelineService) ProcessAndStore(ctx context.Context, events []domain.Event) (int64, error) {

	for i := range events {
		enrichData, err := p.enrichmentService.EnrichIp(ctx, events[i].ClientIP)
		if err != nil {
			slog.Error("Failed to fetch enrich data",
				slog.String("event_id", events[i].ID.String()),
				slog.String("client_ip", events[i].ClientIP),
				slog.Any("err", err))
		} else {
			events[i].EnrichedData = enrichData
		}
	}

	recs := int64(0)
	err := p.txManager.WithTx(ctx, func(aar port.AnalyticsAggregatorRepository) error {

		affectedRecs, err := aar.Event().CreateEvents(ctx, events)
		if err != nil {
			slog.Error("create events error", slog.Any("err", err))
			return err
		}

		recs = affectedRecs
		return nil
	})

	return recs, err
}
