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

// DefaultIdleConnTimeout is applied when a caller does not configure one.
const DefaultIdleConnTimeout = 30 * time.Second

// NewPooledTransport clones http.DefaultTransport and overrides its connection
// pool. A non-positive idleConnTimeout falls back to DefaultIdleConnTimeout.
//
// MaxConnsPerHost is deliberately left at 0 (unlimited): it is a hard limiter
// with blocking acquisition semantics, so a non-zero value would trade
// connection churn for requests timing out while queued.
func NewPooledTransport(maxConns int, idleConnTimeout time.Duration) *http.Transport {
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
