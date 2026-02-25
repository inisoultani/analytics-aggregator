package repository

import (
	"analytics-aggregator/internal/adapter/repository/sqlc"
	"analytics-aggregator/internal/core/domain"
	"context"
)

type PostgresDeadLetterEventRepository struct {
	queries *sqlc.Queries
}

func (r *PostgresDeadLetterEventRepository) CreateDeadLetter(ctx context.Context, e domain.Event, reason string) (string, error) {
	return "", nil
}
