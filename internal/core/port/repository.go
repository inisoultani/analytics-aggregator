package port

import (
	"analytics-aggregator/internal/core/domain"
	"context"
)

type EventRepository interface {
	CreateEvents(ctx context.Context, e []domain.Event) (int64, error)
}

type DeadLetterEventRepository interface {
	CreateDeadLetter(ctx context.Context, e domain.Event, reason string) (string, error)
}

type AnalyticsAggregatorRepository interface {
	Event() EventRepository
	DeadLetterEvent() DeadLetterEventRepository
}

type TxManager interface {
	// transaction
	WithTx(ctx context.Context, fn func(AnalyticsAggregatorRepository) error) error
}
