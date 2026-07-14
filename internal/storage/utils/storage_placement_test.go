package utils

import (
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestCloneBlockWithoutStoragePlacement(t *testing.T) {
	block := &api.Block{Metadata: &api.BlockMetadata{
		ObjectKeyMain:      "legacy/block.gzip",
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         10,
		ByteLength:         20,
		UncompressedLength: 30,
		Hash:               "hash",
	}}

	require.True(t, HasBlockStoragePlacement(block))
	clone := CloneBlockWithoutStoragePlacement(block)
	require.False(t, HasBlockStoragePlacement(clone))
	require.Equal(t, "hash", clone.Metadata.Hash)
	require.Equal(t, "legacy/block.gzip", block.Metadata.ObjectKeyMain)
}
