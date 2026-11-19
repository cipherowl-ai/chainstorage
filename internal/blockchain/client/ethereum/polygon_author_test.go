package ethereum

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestRecoverBorAuthor(t *testing.T) {
	// Expected authors are what bor_getAuthor returns for the same blocks on Polygon mainnet.
	tests := []struct {
		name     string
		fixture  string
		expected string
	}{
		{
			name:     "genesis",
			fixture:  "client/polygon/polygon_header_0.json",
			expected: `"0x0000000000000000000000000000000000000000"`,
		},
		{
			name:     "beforeJaipur",
			fixture:  "client/polygon/polygon_header_11322000.json",
			expected: `"0xf84c74dea96df0ec22e11e7c33996c73fcc2d822"`,
		},
		{
			name:     "lastBlockBeforeJaipur",
			fixture:  "client/polygon/polygon_header_23849999.json",
			expected: `"0xb79fad4ca981472442f53d16365fdf0305ffd8e9"`,
		},
		{
			name:     "firstJaipurBlock",
			fixture:  "client/polygon/polygon_header_23850000.json",
			expected: `"0xb79fad4ca981472442f53d16365fdf0305ffd8e9"`,
		},
		{
			name:     "recent",
			fixture:  "client/polygon/polygon_header_97577216.json",
			expected: `"0x9aa1e7971a94cbc400d95e593e82fb6e79fc99d8"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			header := fixtures.MustReadFile(test.fixture)
			author, err := recoverBorAuthor(header)
			require.NoError(err)
			require.Equal(test.expected, string(author))
		})
	}
}

func TestRecoverBorAuthor_SignedFieldChanged(t *testing.T) {
	require := testutil.Require(t)

	header := readPolygonHeader(t, "client/polygon/polygon_header_23850000.json")
	header["baseFeePerGas"] = "0x1"

	author, err := recoverBorAuthor(mustMarshal(t, header))
	require.NoError(err)
	require.NotEqual(`"0xb79fad4ca981472442f53d16365fdf0305ffd8e9"`, string(author))
}

func TestRecoverBorAuthor_ShortExtraData(t *testing.T) {
	require := testutil.Require(t)

	header := readPolygonHeader(t, "client/polygon/polygon_header_23850000.json")
	header["extraData"] = "0xd78301091883626f72"

	_, err := recoverBorAuthor(mustMarshal(t, header))
	require.Error(err)
	require.Contains(err.Error(), "shorter than the 65-byte signature")
}

func TestRecoverBorAuthor_InvalidSignature(t *testing.T) {
	require := testutil.Require(t)

	header := readPolygonHeader(t, "client/polygon/polygon_header_23850000.json")
	// 32 vanity bytes followed by an all-zero signature.
	header["extraData"] = "0x" + strings.Repeat("00", 32+65)

	_, err := recoverBorAuthor(mustMarshal(t, header))
	require.Error(err)
}

func TestRecoverBorAuthor_MissingNumber(t *testing.T) {
	require := testutil.Require(t)

	header := readPolygonHeader(t, "client/polygon/polygon_header_23850000.json")
	delete(header, "number")

	_, err := recoverBorAuthor(mustMarshal(t, header))
	require.Error(err)
}

func readPolygonHeader(t *testing.T, fixture string) map[string]any {
	var header map[string]any
	testutil.Require(t).NoError(json.Unmarshal(fixtures.MustReadFile(fixture), &header))
	return header
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	data, err := json.Marshal(v)
	testutil.Require(t).NoError(err)
	return data
}
