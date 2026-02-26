package main

import (
	"analytics-aggregator/internal/adapter/db"
	"analytics-aggregator/internal/adapter/extapi"
	"analytics-aggregator/internal/adapter/handler"
	"analytics-aggregator/internal/adapter/repository"
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/service"
	"analytics-aggregator/internal/logger"
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	ErrDatabaseSetupTimeout = errors.New("Initial DB setup operation timed out")
)

func main() {

	mainCtx, cancelMain := context.WithCancel(context.Background())
	defer cancelMain()

	// load config
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	// configure slog
	logger, logLevel := logger.NewLogger(cfg.AppEnv == "production")
	cfg.LogLevel = logLevel

	// setup db connection
	logger.Info("Starting server", slog.String("env", logLevel.String()))
	logger.Info("Try to estabelishing DB connections...")
	timeout := time.Duration(5) * time.Second
	dbCtx, dbCtxCancel := context.WithTimeoutCause(mainCtx, timeout, ErrDatabaseSetupTimeout)
	defer dbCtxCancel()
	pool, err := db.NewPostgresPool(dbCtx, cfg)
	if err != nil {
		logger.Error("Failed to connect to db", slog.Any("err", err))
		os.Exit(1)
	} else {
		logger.Info("Successfuly estabelishing db connection...")
	}
	defer pool.Close()

	// initate core
	txManager := repository.NewPostgresTxManager(pool)
	geoApiClient := extapi.NewGeoAPIClient(cfg.GeoAPIBaseURL)
	service := service.NewPipelineService(mainCtx, txManager, geoApiClient, cfg)

	// initiate app
	addr := ":" + cfg.ServerPort
	router := handler.NewRouter(service, cfg)
	server := &http.Server{
		Addr:    addr,
		Handler: router,
	}
	go func() {
		logger.Info("analytics-aggregator started", slog.String("port", addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Listen error", slog.Any("err", err))
			os.Exit(1)
		}
	}()

	// channel to block runtime and wait for shutdown signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	logger.Info("closing all db connections...")
	pool.Close()

	logger.Info("shutting down analytics-aggregator server...")
	ctx, cancel := context.WithTimeout(mainCtx, 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Listen error", slog.Any("err", err))
		os.Exit(1)
	}
	logger.Info("server gracefully stopped")

}
