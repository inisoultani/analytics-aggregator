package repository

import (
	"analytics-aggregator/internal/adapter/repository/sqlc"
	"analytics-aggregator/internal/core/domain"
	"context"
)

type PostgresDeadLetterEventRepository struct {
	queries *sqlc.Queries
}

func (p *PostgresDeadLetterEventRepository) CreateDeadLetters(ctx context.Context, events []domain.Event) (int64, error) {
	// return runWithTimeout(ctx, "insert_dead_letter", 1, func(ctx context.Context) (int64, error) {
	// 	var pgUUID pgtype.UUID
	// 	pgUUID.Bytes = e.ID // Both are [16]byte under the hood
	// 	pgUUID.Valid = true
	// 	param := &sqlc.InsertDeadLetterParams{
	// 		ID:          pgUUID,
	// 		RawData:     e.RawData,
	// 		ErrorReason: e.ErrorReason,
	// 	}
	// 	id, err := p.queries.InsertDeadLetter(ctx, *param)
	// 	if err != nil {
	// 		return "", err
	// 	}

	// 	return id.String(), nil
	// })
	// params := make([]sqlc.BulkInsertEventsParams, len(events))
	// for i, e := range events {
	// 	var pgUUID pgtype.UUID
	// 	pgUUID.Bytes = e.ID // Both are [16]byte under the hood
	// 	pgUUID.Valid = true
	// 	params[i] = sqlc.BulkInsertEventsParams{
	// 		ID:           pgUUID,
	// 		RawData:      e.RawData,
	// 		EnrichedData: e.EnrichedData,
	// 	}
	// }

	// effectedRecs, err := p.queries.BulkInsertEvents(ctx, params)
	// if err != nil {
	// 	return 0, err
	// }
	return 123, nil
}
