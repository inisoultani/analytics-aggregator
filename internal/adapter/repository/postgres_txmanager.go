package repository

import (
	"analytics-aggregator/internal/adapter/db"
	"analytics-aggregator/internal/adapter/repository/sqlc"
	"analytics-aggregator/internal/core/port"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPostgresTxManager(pool *pgxpool.Pool) *PostgresTxManager {
	return &PostgresTxManager{
		pool: pool,
	}
}

type PostgresTxManager struct {
	pool    *pgxpool.Pool
	queries *sqlc.Queries
}

func (p *PostgresTxManager) WithTx(ctx context.Context, fn func(port.AnalyticsAggregatorRepository) error) error {
	return db.WithTx(ctx, p.pool, func(tx pgx.Tx) error {
		txRepo := &PostgresAnalyticsAggregatorRepository{
			queries: p.queries.WithTx(tx),
		}
		return fn(txRepo)
	})
}

type PostgresAnalyticsAggregatorRepository struct {
	queries *sqlc.Queries
}

func (p *PostgresAnalyticsAggregatorRepository) Event() port.EventRepository {
	return &PostgresEventRepository{
		queries: p.queries,
	}
}

func (p *PostgresAnalyticsAggregatorRepository) DeadLetterEvent() port.DeadLetterEventRepository {
	return &PostgresDeadLetterEventRepository{
		queries: p.queries,
	}
}

func getContextWithTimeout(ctx context.Context, label string, rowCount int) (context.Context, context.CancelFunc) {
	// base timeout
	timeout := 2 * time.Second

	// add 50ms per row for batch
	if rowCount > 1 {
		timeout += time.Duration(rowCount) * 50 * time.Millisecond
	}

	// cap time to 10sec
	if timeout > 10*time.Second {
		timeout = 10 * time.Second
	}

	cause := fmt.Errorf("repo-timeout : %s limit was (%v)", label, timeout)
	return context.WithTimeoutCause(ctx, timeout, cause)
}

func runWithTimeout[T any](ctx context.Context, label string, rowCount int, fn func(context.Context) (T, error)) (T, error) {
	childCtx, cancel := getContextWithTimeout(ctx, label, rowCount)
	defer cancel()

	t, err := fn(childCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			// return the custom cause as the error itself
			var zero T
			return zero, context.Cause(childCtx)
		}
		return t, err
	}
	return t, nil
}
