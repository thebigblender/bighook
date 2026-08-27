package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port              string
	DatabaseURL       string
	WorkerConcurrency int
	PollInterval      time.Duration
	ReaperInterval    time.Duration
	ReaperStaleWindow time.Duration
	HTTPTimeout       time.Duration
	MaxIdleConns      int
	MaxIdleConnsHost  int
	MaxBackoff        time.Duration
	MaxReaps          int
}

func Load() *Config {
	port := getEnv("PORT", "8080")
	if port[0] != ':' {
		port = ":" + port
	}

	return &Config{
		Port:              port,
		DatabaseURL:       getEnv("DATABASE_URL", ""),
		WorkerConcurrency: getEnvInt("WORKER_CONCURRENCY", 4),
		PollInterval:      getEnvDuration("POLL_INTERVAL", 500*time.Millisecond),
		ReaperInterval:    getEnvDuration("REAPER_INTERVAL", 1*time.Minute),
		ReaperStaleWindow: getEnvDuration("REAPER_STALE_WINDOW", 2*time.Minute),
		HTTPTimeout:       getEnvDuration("HTTP_TIMEOUT", 5*time.Second),
		MaxIdleConns:      getEnvInt("HTTP_MAX_IDLE_CONNS", 1000),
		MaxIdleConnsHost:  getEnvInt("HTTP_MAX_IDLE_CONNS_PER_HOST", 200),
		MaxBackoff:        getEnvDuration("MAX_BACKOFF", 1*time.Hour),
		MaxReaps:          getEnvInt("MAX_REAPS", 5),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
