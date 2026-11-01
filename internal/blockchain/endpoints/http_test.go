package endpoints

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

// transportOf unwraps the round trippers layered on top of the client's
// transport by tracehttp.WrapClient and WrapHTTPClient.
func transportOf(t *testing.T, client *http.Client) *http.Transport {
	require := testutil.Require(t)

	rt := client.Transport
	for i := 0; i < 8; i++ {
		switch v := rt.(type) {
		case *http.Transport:
			return v
		case *roundTripper:
			rt = v.base
		case interface{ Unwrap() http.RoundTripper }:
			rt = v.Unwrap()
		default:
			require.FailNow(fmt.Sprintf("unexpected round tripper: %T", rt))
		}
	}

	require.FailNow("failed to unwrap transport")
	return nil
}

func TestHTTPClient_Default(t *testing.T) {
	require := testutil.Require(t)

	client, err := newHTTPClient()
	require.NoError(err)
	require.NotEmpty(client.Timeout)
	require.Nil(client.Jar)
}

func TestHTTPClient_StickySessionCookie(t *testing.T) {
	const (
		u     = "https://www.coinbase.com"
		key   = "hash_key"
		value = "foo_bar"
	)

	require := testutil.Require(t)

	client, err := newHTTPClient(withStickySessionCookieHash(u, key, value))
	require.NoError(err)
	require.NotEmpty(client.Timeout)

	jar := client.Jar
	require.NotNil(jar)

	pu, err := url.Parse("https://www.coinbase.com/block")
	require.NoError(err)
	expected := []*http.Cookie{
		{
			Name:  key,
			Value: value,
		},
	}
	actual := jar.Cookies(pu)
	require.Equal(1, len(actual))
	require.Equal(expected, actual)
}

func TestHTTPClient_ALBStickySession(t *testing.T) {
	require := testutil.Require(t)

	client, err := newHTTPClient(withStickySessionCookiePassive())
	require.NoError(err)
	require.NotEmpty(client.Timeout)

	require.NotNil(client.Jar)
	_, ok := client.Jar.(http.CookieJar)
	require.True(ok)
}

func TestHTTPClient_StickySessionHeader(t *testing.T) {
	require := testutil.Require(t)

	client, err := newHTTPClient(withStickySessionHeaderHash("key", "value"))
	require.NoError(err)
	require.NotEmpty(client.Timeout)
	require.Nil(client.Jar)
}

// Every path must size the connection pool. A bare http.DefaultTransport
// clone keeps only DefaultMaxIdleConnsPerHost = 2 idle connections per host,
// which churns a TCP connection per request under concurrency.
func TestHTTPClient_ConnectionPool(t *testing.T) {
	tests := []struct {
		name string
		opts []ClientOption
	}{
		{
			name: "default",
		},
		{
			name: "sticky_session_header",
			opts: []ClientOption{withStickySessionHeaderHash("key", "value")},
		},
		{
			name: "sticky_session_cookie_hash",
			opts: []ClientOption{withStickySessionCookieHash("https://www.coinbase.com", "hash_key", "foo_bar")},
		},
		{
			name: "sticky_session_cookie_passive",
			opts: []ClientOption{withStickySessionCookiePassive()},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			client, err := newHTTPClient(test.opts...)
			require.NoError(err)

			transport := transportOf(t, client)
			require.Equal(defaultMaxConns, transport.MaxIdleConns)
			require.Equal(defaultMaxConns, transport.MaxIdleConnsPerHost)
			require.Equal(defaultIdleConnTimeout, transport.IdleConnTimeout)
			// The pool is sized, not capped: requests must never block
			// waiting for a connection.
			require.Equal(0, transport.MaxConnsPerHost)
			// The settings inherited from http.DefaultTransport must survive.
			require.NotNil(transport.Proxy)
			require.NotNil(transport.DialContext)
			require.True(transport.ForceAttemptHTTP2)
		})
	}
}

func TestHTTPClient_ConnectionPoolOverride(t *testing.T) {
	require := testutil.Require(t)

	const maxConns = 7
	idleConnTimeout := 5 * time.Second

	client, err := newHTTPClient(withMaxConns(maxConns), withIdleConnTimeout(idleConnTimeout))
	require.NoError(err)

	transport := transportOf(t, client)
	require.Equal(maxConns, transport.MaxIdleConns)
	require.Equal(maxConns, transport.MaxIdleConnsPerHost)
	require.Equal(idleConnTimeout, transport.IdleConnTimeout)
}

func TestHTTPClient_TimeoutOverride(t *testing.T) {
	require := testutil.Require(t)
	timeout := 1000 * time.Second
	client, err := newHTTPClient(withOverrideRequestTimeout(timeout))
	require.NoError(err)
	require.NotEmpty(client.Timeout)
	require.Equal(timeout, client.Timeout)
}
