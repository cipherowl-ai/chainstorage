package httputil

import (
	"net/http"
	"testing"
	"time"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestNewPooledTransport(t *testing.T) {
	require := testutil.Require(t)

	transport := NewPooledTransport(120, 10*time.Second)
	require.Equal(120, transport.MaxIdleConns)
	require.Equal(120, transport.MaxIdleConnsPerHost)
	require.Equal(10*time.Second, transport.IdleConnTimeout)
	// The pool is sized, not capped: requests must never block waiting for a
	// connection.
	require.Equal(0, transport.MaxConnsPerHost)

	// The settings inherited from http.DefaultTransport must survive.
	require.NotNil(transport.Proxy)
	require.NotNil(transport.DialContext)
	require.True(transport.ForceAttemptHTTP2)
	// Cloning is what leaves TLSClientConfig non-nil, which callers overwrite
	// fields on.
	require.NotNil(transport.TLSClientConfig)

	// The global default must not be touched.
	require.Equal(0, http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost)
}

func TestNewPooledTransport_DefaultIdleConnTimeout(t *testing.T) {
	require := testutil.Require(t)

	transport := NewPooledTransport(8, 0)
	require.Equal(DefaultIdleConnTimeout, transport.IdleConnTimeout)
}

func TestNewPooledTransport_DefaultMaxConns(t *testing.T) {
	require := testutil.Require(t)

	// A caller that configures only the idle timeout must still get a pool:
	// MaxIdleConnsPerHost = 0 would mean DefaultMaxIdleConnsPerHost = 2.
	transport := NewPooledTransport(0, 10*time.Second)
	require.Equal(DefaultMaxConns, transport.MaxIdleConns)
	require.Equal(DefaultMaxConns, transport.MaxIdleConnsPerHost)
	require.Equal(10*time.Second, transport.IdleConnTimeout)
}
