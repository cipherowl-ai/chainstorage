package activity

import (
	"net/http"
	"testing"
	"time"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/httputil"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

// unwrapTracedTransport strips the round tripper installed by
// tracehttp.WrapClient to reach the transport the client was built with.
func unwrapTracedTransport(t *testing.T, client *http.Client) http.RoundTripper {
	require := testutil.Require(t)

	unwrapper, ok := client.Transport.(interface{ Unwrap() http.RoundTripper })
	require.True(ok)
	return unwrapper.Unwrap()
}

// Without configuration the replicator must keep sharing
// http.DefaultTransport, exactly as it did before the pool knob existed.
func TestReplicatorHTTPClient_UnconfiguredKeepsDefaultTransport(t *testing.T) {
	require := testutil.Require(t)

	client := newReplicatorHTTPClient(config.HttpPoolConfig{})
	require.Equal(timeout, client.Timeout)
	require.True(unwrapTracedTransport(t, client) == http.DefaultTransport)
}

func TestReplicatorHTTPClient_ConfiguredPool(t *testing.T) {
	require := testutil.Require(t)

	client := newReplicatorHTTPClient(config.HttpPoolConfig{MaxConns: 16})
	transport, ok := unwrapTracedTransport(t, client).(*http.Transport)
	require.True(ok)
	require.Equal(16, transport.MaxIdleConns)
	require.Equal(16, transport.MaxIdleConnsPerHost)
	require.Equal(httputil.DefaultIdleConnTimeout, transport.IdleConnTimeout)
}

// Configuring only the idle timeout must still pool, rather than being
// silently dropped on the floor.
func TestReplicatorHTTPClient_IdleConnTimeoutOnly(t *testing.T) {
	require := testutil.Require(t)

	client := newReplicatorHTTPClient(config.HttpPoolConfig{IdleConnTimeout: 90 * time.Second})
	transport, ok := unwrapTracedTransport(t, client).(*http.Transport)
	require.True(ok)
	require.Equal(90*time.Second, transport.IdleConnTimeout)
	require.Equal(httputil.DefaultMaxConns, transport.MaxIdleConns)
	require.Equal(httputil.DefaultMaxConns, transport.MaxIdleConnsPerHost)
}
