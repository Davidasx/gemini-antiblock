package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"

	"gemini-antiblock/config"
	"gemini-antiblock/handlers"
	"gemini-antiblock/logger"
)

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	// Load configuration
	cfg := config.LoadConfig()

	// Set up logging
	logger.SetDebugMode(cfg.DebugMode)

	logger.LogInfo("=== GEMINI ANTIBLOCK PROXY STARTING ===")
	logger.LogInfo(fmt.Sprintf("Upstream URL: %s", cfg.UpstreamURLBase))
	logger.LogInfo(fmt.Sprintf("Max retries: %d", cfg.MaxConsecutiveRetries))
	logger.LogInfo(fmt.Sprintf("Debug mode: %t", cfg.DebugMode))
	logger.LogInfo(fmt.Sprintf("Retry delay: %v", cfg.RetryDelayMs))
	logger.LogInfo(fmt.Sprintf("Swallow thoughts after retry: %t", cfg.SwallowThoughtsAfterRetry))
	logger.LogInfo(fmt.Sprintf("Server port: %s", cfg.Port))

	// Create rate limiter from config
	rateLimitWindow := time.Duration(cfg.RateLimitWindowSeconds) * time.Second
	rateLimiter := handlers.NewRateLimiter(cfg.RateLimitCount, rateLimitWindow)
	logger.LogInfo(fmt.Sprintf("Rate limiting enabled: %d requests per %v per key", cfg.RateLimitCount, rateLimitWindow))

	// Create proxy handler
	proxyHandler := handlers.NewProxyHandler(cfg, rateLimiter)

	// Set up routes
	router := mux.NewRouter()

	// Health check endpoint
	router.HandleFunc("/health", handlers.HealthHandler).Methods("GET")
	router.HandleFunc("/healthz", handlers.HealthHandler).Methods("GET")

	// Handle all requests with the proxy handler
	router.PathPrefix("/").Handler(proxyHandler)

	// Start server
	logger.LogInfo(fmt.Sprintf("Starting server on port %s", cfg.Port))
	logger.LogInfo("Server ready to accept requests")

	if err := http.ListenAndServe(":"+cfg.Port, router); err != nil {
		logger.LogError("Server failed to start:", err)
		os.Exit(1)
	}
}
