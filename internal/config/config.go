package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr string

	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	WindowDuration time.Duration
	BucketCount    int

	MaxHitsPerActor int

	TopCacheRefresh time.Duration
	DefaultTopN     int
	MaxTopN         int
}

func Load() Config {
	return Config{
		HTTPAddr: env("HTTP_ADDR", ":8080"),

		KafkaBrokers: []string{env("KAFKA_BROKERS", "localhost:9092")},
		KafkaTopic:   env("KAFKA_TOPIC", "search.events"),
		KafkaGroupID: env("KAFKA_GROUP_ID", "trending-service"),

		WindowDuration: 5 * time.Minute,
		BucketCount:    envInt("BUCKET_COUNT", 30),

		MaxHitsPerActor: envInt("MAX_HITS_PER_ACTOR", 3),

		TopCacheRefresh: envDuration("TOP_CACHE_REFRESH", time.Second),
		DefaultTopN:     envInt("DEFAULT_TOP_N", 10),
		MaxTopN:         envInt("MAX_TOP_N", 100),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
