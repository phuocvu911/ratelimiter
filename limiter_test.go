package ratelimiter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func newTestHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func doRequest(t *testing.T, h http.Handler, remoteAddr string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = remoteAddr
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestLimit_AllowsWithinBurst(t *testing.T) {
	l := New(1, 3, time.Minute)
	h := l.Limit(newTestHandler())

	for i := 0; i < 3; i++ {
		rec := doRequest(t, h, "1.2.3.4:5555")
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: got status %d, want %d", i, rec.Code, http.StatusOK)
		}
	}
}

func TestLimit_RejectsOverBurst(t *testing.T) {
	l := New(1, 2, time.Minute)
	h := l.Limit(newTestHandler())

	// Exhaust the burst.
	doRequest(t, h, "1.2.3.4:5555")
	doRequest(t, h, "1.2.3.4:5555")

	// Next request should be rejected immediately.
	rec := doRequest(t, h, "1.2.3.4:5555")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("got status %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("expected Retry-After header to be set")
	}
}

func TestLimit_PerIPIsolation(t *testing.T) {
	l := New(1, 1, time.Minute)
	h := l.Limit(newTestHandler())

	// First IP exhausts its own bucket.
	doRequest(t, h, "1.1.1.1:1111")
	rec := doRequest(t, h, "1.1.1.1:1111")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("ip1: got status %d, want %d", rec.Code, http.StatusTooManyRequests)
	}

	// A different IP should be unaffected.
	rec2 := doRequest(t, h, "2.2.2.2:2222")
	if rec2.Code != http.StatusOK {
		t.Fatalf("ip2: got status %d, want %d", rec2.Code, http.StatusOK)
	}
}

func TestLimit_RefillsOverTime(t *testing.T) {
	l := New(rate.Every(10*time.Millisecond), 1, time.Minute)
	h := l.Limit(newTestHandler())

	doRequest(t, h, "3.3.3.3:3333")
	rec := doRequest(t, h, "3.3.3.3:3333")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected immediate second request to be rejected, got %d", rec.Code)
	}

	time.Sleep(20 * time.Millisecond)

	rec2 := doRequest(t, h, "3.3.3.3:3333")
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected request to be allowed after refill, got %d", rec2.Code)
	}
}

func TestClientIP_ParsesHostPort(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "9.9.9.9:4321"

	if got := clientIP(req); got != "9.9.9.9" {
		t.Errorf("got %q, want %q", got, "9.9.9.9")
	}
}

func TestClientIP_FallsBackOnMalformedAddr(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "not-a-valid-addr"

	if got := clientIP(req); got != "not-a-valid-addr" {
		t.Errorf("got %q, want fallback %q", got, "not-a-valid-addr")
	}
}

func TestCleanup_EvictsStaleVisitors(t *testing.T) {
	l := New(1, 1, 10*time.Millisecond)
	h := l.Limit(newTestHandler())

	doRequest(t, h, "5.5.5.5:5555")

	l.mu.Lock()
	if _, ok := l.visitors["5.5.5.5"]; !ok {
		l.mu.Unlock()
		t.Fatal("expected visitor to be tracked after request")
	}
	l.mu.Unlock()

	time.Sleep(20 * time.Millisecond)
	l.cleanup()

	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.visitors["5.5.5.5"]; ok {
		t.Error("expected stale visitor to be evicted")
	}
}

func TestStartCleanup_StopsOnStop(t *testing.T) {
	l := New(1, 1, time.Millisecond)
	l.StartCleanup(context.Background(), time.Millisecond)

	// Give the goroutine a moment to start running.
	time.Sleep(5 * time.Millisecond)
	l.Stop()

	// Calling Stop twice must not panic (sync.Once guards this).
	l.Stop()
}

func TestStartCleanup_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := New(1, 1, time.Millisecond)
	l.StartCleanup(ctx, time.Millisecond)

	cancel()
	time.Sleep(5 * time.Millisecond)
	// No assertion beyond "doesn't hang or panic" — goroutine leak would show
	// up under -race/-timeout in CI if this didn't work.
}

func TestLimit_ConcurrentAccess(t *testing.T) {
	l := New(100, 100, time.Minute)
	h := l.Limit(newTestHandler())

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ip := "10.0.0.1:1000"
			if n%2 == 0 {
				ip = "10.0.0.2:2000"
			}
			doRequest(t, h, ip)
		}(i)
	}
	wg.Wait()
}
