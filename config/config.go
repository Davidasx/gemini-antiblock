package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration values
type Config struct {
	UpstreamURLBase           string
	MaxConsecutiveRetries     int
	DebugMode                 bool
	RetryDelayMs              time.Duration
	SwallowThoughtsAfterRetry bool
	Port                      string
	RateLimitCount            int
	RateLimitWindowSeconds    int
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	return &Config{
		UpstreamURLBase:           getEnvString("UPSTREAM_URL_BASE", "https://generativelanguage.googleapis.com"),
		MaxConsecutiveRetries:     getEnvInt("MAX_CONSECUTIVE_RETRIES", 100),
		DebugMode:                 getEnvBool("DEBUG_MODE", true),
		RetryDelayMs:              time.Duration(getEnvInt("RETRY_DELAY_MS", 750)) * time.Millisecond,
		SwallowThoughtsAfterRetry: getEnvBool("SWALLOW_THOUGHTS_AFTER_RETRY", true),
		Port:                      getEnvString("PORT", "8080"),
		RateLimitCount:            getEnvInt("RATE_LIMIT_COUNT", 3),
		RateLimitWindowSeconds:    getEnvInt("RATE_LIMIT_WINDOW_SECONDS", 60),
	}
}

func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}
