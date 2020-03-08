package ratelimiter

import (
	"sync"
	"time"
)

type RateLimiter struct {
	maxCount int64
	interval time.Duration
	mu sync.Mutex
	currentCount int64
	lastTime time.Time
}

func New(maxCount int64,interval time.Duration) *RateLimiter{
	return &RateLimiter{
		maxCount:maxCount,
		interval:interval,
		mu:sync.Mutex{},
	}
}

func (r *RateLimiter)Allow()bool  {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.lastTime) < r.interval {
		if r.currentCount > 0 {
			r.currentCount--
			return true
		}
		return false
	}
	r.currentCount = r.maxCount-1
	r.lastTime = time.Now()
	return true
}