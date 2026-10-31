// Package httputil builds http.Transports with an explicitly sized connection
// pool. A bare http.DefaultTransport clone keeps only
// DefaultMaxIdleConnsPerHost = 2 idle connections per host, which forces a
// fresh TCP + TLS handshake for all but two concurrent requests to the same
// host.
package httputil

import (
	"net/http"
	"time"
)

const (
	// DefaultMaxConns and DefaultIdleConnTimeout are applied when a caller
	// leaves the corresponding setting at zero.
	DefaultMaxConns        = 120
	DefaultIdleConnTimeout = 30 * time.Second
)

// NewPooledTransport clones http.DefaultTransport and overrides its connection
// pool. A non-positive maxConns or idleConnTimeout falls back to
// DefaultMaxConns or DefaultIdleConnTimeout, so a partially configured pool is
// still a pool. In particular maxConns must never reach the transport as 0:
// MaxIdleConnsPerHost = 0 means DefaultMaxIdleConnsPerHost = 2, which is the
// behavior this package exists to avoid.
//
// MaxConnsPerHost is deliberately left at 0 (unlimited): it is a hard limiter
// with blocking acquisition semantics, so a non-zero value would trade
// connection churn for requests timing out while queued.
func NewPooledTransport(maxConns int, idleConnTimeout time.Duration) *http.Transport {
	if maxConns <= 0 {
		maxConns = DefaultMaxConns
	}

	if idleConnTimeout <= 0 {
		idleConnTimeout = DefaultIdleConnTimeout
	}

	// Clone the default transport to keep its proxy, dial timeouts and
	// HTTP/2 configuration.
	transport := http.DefaultTransport.(*http.Transport).Clone()

	transport.IdleConnTimeout = idleConnTimeout
	transport.MaxIdleConns = maxConns
	transport.MaxIdleConnsPerHost = maxConns

	return transport
}
