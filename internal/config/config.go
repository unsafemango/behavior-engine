// Package config loads process configuration from environment variables.
// Every service in behavior-engine (ingestion-api, profile-consumer,
// strategy-api) shares this loader so configuration stays consistent
// and each binary only pulls the keys it actually needs.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every environment-driven setting used across the three
// services. Fields are exported so callers can read them directly;
// each binary only touches the subset it cares about.
type Config struct {
	ServiceName string
	HTTPAddr    string
	LogLevel    string

	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	PostgresDSN string
	RedisAddr   string
	RedisDB     int

	ProfileCacheTTL time.Duration

	// ActiveExperimentID is the experiment the strategy engine buckets
	// users into by default. Kept simple (single active experiment) on
	// purpose -- multi-experiment support is a documented extension.
	ActiveExperimentID string
}

// Load reads every known env var, applying sane local-dev defaults so the
// services run with zero configuration against the docker-compose stack.
func Load() Config {
	return Config{
		ServiceName: getEnv("SERVICE_NAME", "behavior-engine"),
		HTTPAddr:    getEnv("HTTP_ADDR", ":8080"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),

		KafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "behavior.events"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "profile-consumer"),

		PostgresDSN: getEnv("POSTGRES_DSN", "postgres://behavior:behavior@localhost:5432/behavior_engine?sslmode=disable"),
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		RedisDB:     getEnvInt("REDIS_DB", 0),

		ProfileCacheTTL: getEnvDuration("PROFILE_CACHE_TTL", 24*time.Hour),

		ActiveExperimentID: getEnv("ACTIVE_EXPERIMENT_ID", "checkout_button_color"),
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

// String is a redacted representation safe to log at startup.
func (c Config) String() string {
	return fmt.Sprintf(
		"service=%s http_addr=%s kafka_brokers=%v kafka_topic=%s redis_addr=%s",
		c.ServiceName, c.HTTPAddr, c.KafkaBrokers, c.KafkaTopic, c.RedisAddr,
	)
}
