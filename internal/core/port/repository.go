package port

import (
	"analytics-aggregator/internal/core/domain"
	"context"
)

type EventRepository interface {
	CreateEvents(ctx context.Context, e []domain.Event) (int64, error)
}

type DeadLetterEventRepository interface {
	CreateDeadLetters(ctx context.Context, e []domain.Event) (int64, error)
}

type PipelineRepository interface {
	Event() EventRepository
	DeadLetterEvent() DeadLetterEventRepository
}

type TxManager interface {
	// transaction
	WithTx(ctx context.Context, fn func(PipelineRepository) error) error
}
