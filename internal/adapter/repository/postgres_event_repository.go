package repository

import (
	"analytics-aggregator/internal/adapter/repository/sqlc"
	"analytics-aggregator/internal/core/domain"
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

type PostgresEventRepository struct {
	queries *sqlc.Queries
}

func (r *PostgresEventRepository) CreateEvents(ctx context.Context, events []domain.Event) (int64, error) {
	return runWithTimeout(ctx, "batch_insert_events", len(events), func(ctx context.Context) (int64, error) {
		params := make([]sqlc.BulkInsertEventsParams, 0, len(events))
		for i, e := range events {
			var pgUUID pgtype.UUID
			pgUUID.Bytes = e.ID // Both are [16]byte under the hood
			pgUUID.Valid = true
			params[i] = sqlc.BulkInsertEventsParams{
				ID:           pgUUID,
				RawData:      e.RawData,
				EnrichedData: e.EnrichedData,
			}
		}

		effectedRecs, err := r.queries.BulkInsertEvents(ctx, params)
		if err != nil {
			return 0, err
		}
		return effectedRecs, nil
	})
}
