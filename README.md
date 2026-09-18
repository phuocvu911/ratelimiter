# ratelimiter
<div align="center">
<br />

[![MIT License](https://img.shields.io/badge/License-MIT-555555.svg?labelColor=333333&color=666666)](./LICENSE)

</div>

A lightweight Go HTTP rate limiter for per-client-IP request throttling, built on top of the token bucket algorithm from `golang.org/x/time/rate`.

It is designed for protecting HTTP handlers from abuse by limiting how many requests each remote IP can make in a given time window. Each IP gets its own limiter bucket, so one client cannot consume the quota of another client.

## Features

- Per-IP rate limiting
- Burst support via token bucket configuration
- Automatic eviction of idle visitors
- Simple middleware-style integration with Go HTTP handlers
- Safe cleanup lifecycle with `StartCleanup` and `Stop`

## Installation

```zsh
go get github.com/phuocvu911/ratelimiter
```

## Usage

The package exposes a constructor, `New`, which creates a limiter for a given request rate, burst, and idle TTL (Time To Live).

```go
limiter := ratelimiter.New(2, 5, 3*time.Minute)
limiter.StartCleanup(context.Background(), time.Minute)
defer limiter.Stop()

mux...

http.ListenAndServe(":8080", limiter.Limit(mux))

// When a client exceeds the permitted rate, the response is 429 Too Many Requests.

```

## How it works

Each client IP is tracked separately in an internal map. When a request arrives, the limiter extracts the remote IP from `r.RemoteAddr` and checks whether that IP still has tokens available.

- `rps` defines how many requests are allowed per second.
- `burst` defines how many tokens can be accumulated immediately.
- `ttl` controls how long an idle IP bucket stays in memory before cleanup removes it.

If the bucket is empty, the middleware writes:

- HTTP status: `429 Too Many Requests`
- Header: `Retry-After: 1`

## Notes

- The limiter identifies clients by the IP extracted from `RemoteAddr`.
- If `RemoteAddr` is malformed, the limiter falls back to the raw value.
- The implementation is simple and suitable for API gateways, internal services, or small web applications that need basic per-IP throttling.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
