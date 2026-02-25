package config

import (
	"log"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL        string
	PagingLimitDefault int
	PagingLimitMax     int
	ServerPort         string
	MaxConns           int
	MinConns           int
	MaxConnIdleTime    int
	MaxConnLifeTime    int
	HealthCheckPeriod  int
	AppEnv             string
	LogLevel           *slog.LevelVar
}

func Load() (*Config, error) {
	log.Println("start loading config from .env config file or current env variables")
	// Loads .env into system environment variables
	err := godotenv.Load()
	if err != nil {
		// .env is missing in Production,
		// as vars are set directly in the OS/Docker.
		log.Println("No .env file found, using system environment variables")
	}

	return &Config{
		DatabaseURL:        getEnv("DATABASE_URL", "postgres://localhost:5432/analytics_aggregator"),
		PagingLimitDefault: getEnvInt("PAGING_LIMIT_DEFAULT", 10),
		PagingLimitMax:     getEnvInt("PAGING_LIMIT_MAX", 100),
		ServerPort:         getEnv("SERVER_PORT", "8081"),
		MaxConns:           getEnvInt("DB_MAX_CONNS", 10),
		MinConns:           getEnvInt("DB_MIN_CONNS", 5),
		MaxConnIdleTime:    getEnvInt("DB_MAX_IDLE_TIME", 300),
		MaxConnLifeTime:    getEnvInt("DB_MAX_LIFE_TIME", 1800),
		HealthCheckPeriod:  getEnvInt("DB_HEALTH_CHECK_PERIOD", 60),
		AppEnv:             strings.ToLower(getEnv("APP_ENV", "development")),
	}, nil
}

// getEnv helper to get string env or return default
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	s, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return fallback
	}
	return v
}
