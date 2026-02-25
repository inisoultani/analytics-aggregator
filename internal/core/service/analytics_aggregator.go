package service

import (
	"analytics-aggregator/internal/core/domain"
	"analytics-aggregator/internal/core/port"
	"context"
)

type AnalyticsAggregatorService struct {
	txManager port.TxManager
}

func NewAnalyticsAggregatorService(txManager port.TxManager) *AnalyticsAggregatorService {
	return &AnalyticsAggregatorService{
		txManager: txManager,
	}
}

func (a *AnalyticsAggregatorService) ProcessAndStore(ctx context.Context, events []domain.Event) (int64, error) {
	recs := int64(0)
	err := a.txManager.WithTx(ctx, func(aar port.AnalyticsAggregatorRepository) error {

		affectedRecs, err := aar.Event().CreateEvents(ctx, events)
		if err != nil {
			return err
		}

		recs = affectedRecs
		return nil
	})

	return recs, err
}
