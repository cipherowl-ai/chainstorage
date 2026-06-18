package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestBlockObjectByteFields_LegacyUsesNullSentinel(t *testing.T) {
	block := testutil.MakeBlockMetadata(100, tag)

	byteOffset, byteLength, uncompressedLength := blockObjectByteFields(block)

	require.False(t, byteOffset.Valid)
	require.False(t, byteLength.Valid)
	require.False(t, uncompressedLength.Valid)
}

func TestBlockObjectByteFields_ConsolidatedFields(t *testing.T) {
	block := testutil.MakeBlockMetadata(100, tag)
	block.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
	block.ByteOffset = 4096
	block.ByteLength = 8192
	block.UncompressedLength = 8192

	byteOffset, byteLength, uncompressedLength := blockObjectByteFields(block)

	require.True(t, byteOffset.Valid)
	require.Equal(t, int64(4096), byteOffset.Int64)
	require.True(t, byteLength.Valid)
	require.Equal(t, int64(8192), byteLength.Int64)
	require.True(t, uncompressedLength.Valid)
	require.Equal(t, int64(8192), uncompressedLength.Int64)
}
