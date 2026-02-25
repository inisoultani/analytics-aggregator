package service

import (
	"analytics-aggregator/internal/core/domain"
	"analytics-aggregator/internal/core/port"
	"context"
	"log/slog"
)

type PipelineService struct {
	txManager port.TxManager
}

func NewPipelineService(txManager port.TxManager) *PipelineService {
	return &PipelineService{
		txManager: txManager,
	}
}

func (a *PipelineService) ProcessAndStore(ctx context.Context, events []domain.Event) (int64, error) {
	recs := int64(0)
	err := a.txManager.WithTx(ctx, func(aar port.AnalyticsAggregatorRepository) error {

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
