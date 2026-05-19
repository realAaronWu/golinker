package ratelimit

import (
	"sync"
	"time"
)

// Limiter implements a token bucket rate limiter for controlling send rate.
type Limiter struct {
	mu       sync.Mutex
	rate     float64   // tokens per second
	tokens   float64   // current tokens
	maxBurst float64   // max burst size
	lastTime time.Time
}

// New creates a rate limiter. rate is in ops/sec. burst is max burst size.
// If rate is 0, the limiter is unlimited (Wait always returns immediately).
func New(rate int, burst int) *Limiter {
	if burst <= 0 {
		burst = 1
	}
	return &Limiter{
		rate:     float64(rate),
		tokens:   float64(burst),
		maxBurst: float64(burst),
		lastTime: time.Now(),
	}
}

// Wait blocks until a token is available. Returns immediately if rate is 0 (unlimited).
func (l *Limiter) Wait() {
	if l.rate == 0 {
		return // unlimited
	}
	for {
		l.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(l.lastTime).Seconds()
		l.tokens += elapsed * l.rate
		if l.tokens > l.maxBurst {
			l.tokens = l.maxBurst
		}
		l.lastTime = now

		if l.tokens >= 1.0 {
			l.tokens -= 1.0
			l.mu.Unlock()
			return
		}

		// Calculate how long to wait for one token
		waitDur := time.Duration((1.0 - l.tokens) / l.rate * float64(time.Second))
		l.mu.Unlock()
		time.Sleep(waitDur)
	}
}

// TryWait attempts to acquire a token without blocking.
// Returns true if a token was acquired.
func (l *Limiter) TryWait() bool {
	if l.rate == 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastTime).Seconds()
	l.tokens += elapsed * l.rate
	if l.tokens > l.maxBurst {
		l.tokens = l.maxBurst
	}
	l.lastTime = now

	if l.tokens >= 1.0 {
		l.tokens -= 1.0
		return true
	}
	return false
}

// Unlimited returns a limiter that never blocks.
func Unlimited() *Limiter {
	return &Limiter{rate: 0}
}
