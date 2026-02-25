package db

import (
	"analytics-aggregator/internal/config"
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPostgresPool(ctx context.Context, config *config.Config) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		return nil, err
	}

	log.Printf("set MaxConns : %d\n", config.MaxConns)
	cfg.MaxConns = int32(config.MaxConns)

	log.Printf("set MinConns : %d\n", config.MinConns)
	cfg.MinConns = int32(config.MinConns)

	log.Printf("set MaxConnIdleTime : %d\n", config.MaxConnIdleTime)
	cfg.MaxConnIdleTime = time.Duration(config.MaxConnIdleTime) * time.Second

	log.Printf("set MaxConnLifeTime : %d\n", config.MaxConnLifeTime)
	cfg.MaxConnLifetime = time.Duration(config.MaxConnLifeTime) * time.Second

	log.Printf("set HealthCheckPeriod : %d\n", config.HealthCheckPeriod)
	cfg.HealthCheckPeriod = time.Duration(config.HealthCheckPeriod) * time.Second

	return pgxpool.NewWithConfig(ctx, cfg)
}

func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else if r := recover(); r != nil {
			tx.Rollback(ctx)
			// re trigger the panic, so tha application knows something wrong
			panic(r)
		}
	}()

	err = fn(tx)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}
