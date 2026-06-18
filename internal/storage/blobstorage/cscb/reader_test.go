package cscb

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestParseIndexAndExtractBlockPayload(t *testing.T) {
	require := testutil.Require(t)

	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)
	require.Equal(api.Compression_ZSTD, index.Header.Codec)
	require.Len(index.Blocks, 3)
	require.Len(index.Chunks, 2)

	metadata := &api.BlockMetadata{
		Tag:                7,
		Height:             101,
		Hash:               "hash-101",
		ObjectKeyMain:      object.Key,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         object.Placements[1].ByteOffset,
		ByteLength:         object.Placements[1].ByteLength,
		UncompressedLength: object.Placements[1].UncompressedLength,
	}
	block, chunk, err := index.LookupBlock(metadata)
	require.NoError(err)
	require.Equal(uint32(0), chunk.Index)

	frame := data[chunk.CompressedPayloadOffset : chunk.CompressedPayloadOffset+chunk.CompressedLength]
	reader, err := zstd.NewReader(bytes.NewReader(frame))
	require.NoError(err)
	defer reader.Close()
	chunkPayload, err := io.ReadAll(reader)
	require.NoError(err)

	require.NoError(ValidateChunkPayload(chunkPayload, chunk))
	payload, err := ExtractBlockPayload(chunkPayload, block)
	require.NoError(err)
	require.Equal([]byte("bravo-bravo"), payload)
}

func TestLookupBlockRejectsMismatchedMetadata(t *testing.T) {
	require := testutil.Require(t)

	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)

	_, _, err = index.LookupBlock(&api.BlockMetadata{
		Tag:                7,
		Height:             101,
		Hash:               "wrong-hash",
		ObjectKeyMain:      object.Key,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         object.Placements[1].ByteOffset,
		ByteLength:         object.Placements[1].ByteLength,
		UncompressedLength: object.Placements[1].UncompressedLength,
	})
	require.Error(err)
	require.Contains(err.Error(), "hash mismatch")
}
