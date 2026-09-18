package ratelimiter

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Limiter rate-limits HTTP requests per client IP using a token bucket algorithm.
type Limiter struct {
	mu       sync.Mutex
	visitors map[string]*visitor
	rps      rate.Limit
	burst    int
	ttl      time.Duration

	stopOnce sync.Once
	stopCh   chan struct{}
}

type visitor struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// New creates a Limiter allowing rps requests per second per IP, with the given burst size.
// ttl (time to live) controls how long an idle visitor's bucket is kept before cleanup evicts it.
func New(rps rate.Limit, burst int, ttl time.Duration) *Limiter {
	return &Limiter{
		visitors: make(map[string]*visitor),
		rps:      rps,
		burst:    burst,
		ttl:      ttl,
		stopCh:   make(chan struct{}),
	}
}

// Limit wraps next, rejecting requests over the limit with 429 Too Many Requests.
func (l *Limiter) Limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		if !l.getVisitor(ip).Allow() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Too many requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func clientIP(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

func (l *Limiter) getVisitor(ip string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()

	v, exists := l.visitors[ip]
	if !exists {
		rl := rate.NewLimiter(l.rps, l.burst)
		l.visitors[ip] = &visitor{limiter: rl, lastSeen: time.Now()}
		return rl
	}
	v.lastSeen = time.Now()
	return v.limiter
}

// StartCleanup runs a background goroutine that evicts visitors idle longer than ttl,
// checking every interval. Call Stop to shut it down.
func (l *Limiter) StartCleanup(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				l.cleanup()
			case <-ctx.Done():
				return
			case <-l.stopCh:
				return
			}
		}
	}()
}

func (l *Limiter) cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for ip, v := range l.visitors {
		if time.Since(v.lastSeen) > l.ttl {
			delete(l.visitors, ip)
		}
	}
}

// Stop halts the background cleanup goroutine, if running.
func (l *Limiter) Stop() {
	l.stopOnce.Do(func() { close(l.stopCh) })
}