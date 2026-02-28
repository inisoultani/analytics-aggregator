package repository

import (
	"analytics-aggregator/internal/adapter/repository/sqlc"
	"analytics-aggregator/internal/core/domain"
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

type PostgresDeadLetterEventRepository struct {
	queries *sqlc.Queries
}

func (p *PostgresDeadLetterEventRepository) CreateDeadLetters(ctx context.Context, events []domain.Event) (int64, error) {
	params := make([]sqlc.BulkInsertDeadLetterEventsParams, len(events))
	for i, e := range events {
		var pgUUID pgtype.UUID
		pgUUID.Bytes = e.ID // Both are [16]byte under the hood
		pgUUID.Valid = true
		params[i] = sqlc.BulkInsertDeadLetterEventsParams{
			ID:          pgUUID,
			RawData:     e.RawData,
			ErrorReason: e.ErrorReason,
		}
	}

	effectedRecs, err := p.queries.BulkInsertDeadLetterEvents(ctx, params)
	if err != nil {
		return 0, err
	}
	return effectedRecs, nil
}
