package handlers

import (
	"sync"
	"time"
)

// RateLimiter controls request rates on a per-key basis.
type RateLimiter struct {
	mutex   sync.Mutex
	clients map[string][]time.Time // Map from API key to its request timestamps
	limit   int
	window  time.Duration
}

// NewRateLimiter creates a new RateLimiter.
func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		clients: make(map[string][]time.Time),
		limit:   limit,
		window:  window,
	}
}

// Wait enforces the rate limit for a given key, waiting if necessary.
func (l *RateLimiter) Wait(apiKey string) {
	// Loop to handle the case where we wake up but another goroutine gets the slot.
	for {
		l.mutex.Lock()

		now := time.Now()
		cutoff := now.Add(-l.window)

		// Get timestamps for the current key, cleaning up old ones.
		timestamps, _ := l.clients[apiKey]
		firstValidIndex := 0
		for i, ts := range timestamps {
			if !ts.Before(cutoff) {
				firstValidIndex = i
				break
			}
			if i == len(timestamps)-1 {
				firstValidIndex = i + 1
			}
		}
		timestamps = timestamps[firstValidIndex:]

		// If the limit is not reached, allow the request and record it.
		if len(timestamps) < l.limit {
			l.clients[apiKey] = append(timestamps, now)
			l.mutex.Unlock()
			return // Allowed, exit.
		}

		// If the limit is reached, calculate the necessary wait time.
		oldestTimestamp := timestamps[0]
		waitUntil := oldestTimestamp.Add(l.window)
		waitTime := time.Until(waitUntil)

		// Unlock the mutex while waiting to not block other keys.
		l.mutex.Unlock()

		if waitTime > 0 {
			time.Sleep(waitTime)
		}
		// After waiting, loop again to re-check the conditions.
	}
}
