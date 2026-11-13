package cscb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// The Python SDK (sdk/python) has its own reader for consolidated files. Both
// readers are tested against the same files, which this test owns:
//
//   - it reads each committed file with the Go reader, so the Python tests
//     read files that the Go reader accepts;
//   - it encodes the same blocks again and compares the layout, so a change
//     to the format fails here until the files are rewritten, and the Python
//     reader then has to follow.
//
// Rewrite the files with:
//
//	TEST_TYPE=unit go test ./internal/storage/blobstorage/cscb -run TestPythonSDKFixtures -update-python-sdk-fixtures
var updatePythonSDKFixtures = flag.Bool(
	"update-python-sdk-fixtures",
	false,
	"rewrite the consolidated files that the Python SDK tests read",
)

const (
	pythonSDKFixtureDir         = "../../../../sdk/python/tests/testdata"
	pythonSDKFixtureChunkBlocks = 3
)

type (
	pythonSDKFixture struct {
		File        string                  `json:"file"`
		Codec       string                  `json:"codec"`
		ChunkBlocks uint64                  `json:"chunk_blocks"`
		Blocks      []pythonSDKFixtureBlock `json:"blocks"`
	}

	// pythonSDKFixtureBlock is what the server reports about one block (the
	// BlockFile fields), plus the hash of the block payload to expect.
	pythonSDKFixtureBlock struct {
		Tag                uint32 `json:"tag"`
		Hash               string `json:"hash"`
		ParentHash         string `json:"parent_hash"`
		Height             uint64 `json:"height"`
		ParentHeight       uint64 `json:"parent_height"`
		ByteOffset         uint64 `json:"byte_offset"`
		ByteLength         uint64 `json:"byte_length"`
		UncompressedLength uint64 `json:"uncompressed_length"`
		PayloadSHA256      string `json:"payload_sha256"`
	}
)

func TestPythonSDKFixtures(t *testing.T) {
	for _, codec := range []api.Compression{api.Compression_GZIP, api.Compression_ZSTD} {
		t.Run(codec.String(), func(t *testing.T) {
			require := testutil.Require(t)
			blocks := pythonSDKFixtureBlocks()
			name := "cscb_" + strings.ToLower(codec.String())
			binPath := filepath.Join(pythonSDKFixtureDir, name+".bin")
			manifestPath := filepath.Join(pythonSDKFixtureDir, name+".json")

			payloads := make([]internal.ConsolidatedBlockPayload, len(blocks))
			serialized := make([][]byte, len(blocks))
			for i, block := range blocks {
				data, err := proto.MarshalOptions{Deterministic: true}.Marshal(block)
				require.NoError(err)
				serialized[i] = data
				payloads[i] = internal.ConsolidatedBlockPayload{
					Metadata:           block.GetMetadata(),
					MetadataID:         int64(i + 1),
					RawBlockPayload:    internal.BytesPayloadSource(data),
					UncompressedLength: uint64(len(data)),
				}
			}
			object, err := Encode(context.Background(), EncodeConfig{
				Blockchain:             blocks[0].GetBlockchain(),
				Network:                blocks[0].GetNetwork(),
				Codec:                  codec,
				CodecLevel:             3,
				MaxBlocks:              1000,
				CompressionChunkBlocks: pythonSDKFixtureChunkBlocks,
				MaxCompressedBytes:     1024 * 1024,
				MaxUncompressedBytes:   1024 * 1024,
				ShardSize:              10000,
			}, payloads)
			require.NoError(err)
			defer object.Close()
			fresh, ok := object.Bytes()
			require.True(ok)

			if *updatePythonSDKFixtures {
				manifest := pythonSDKFixture{
					File:        name + ".bin",
					Codec:       codec.String(),
					ChunkBlocks: pythonSDKFixtureChunkBlocks,
				}
				for i, placement := range object.Placements {
					metadata := blocks[i].GetMetadata()
					sum := sha256.Sum256(serialized[i])
					manifest.Blocks = append(manifest.Blocks, pythonSDKFixtureBlock{
						Tag:                metadata.GetTag(),
						Hash:               metadata.GetHash(),
						ParentHash:         metadata.GetParentHash(),
						Height:             metadata.GetHeight(),
						ParentHeight:       metadata.GetParentHeight(),
						ByteOffset:         placement.ByteOffset,
						ByteLength:         placement.ByteLength,
						UncompressedLength: placement.UncompressedLength,
						PayloadSHA256:      hex.EncodeToString(sum[:]),
					})
				}
				manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
				require.NoError(err)
				require.NoError(os.MkdirAll(pythonSDKFixtureDir, 0o755))
				require.NoError(os.WriteFile(binPath, fresh, 0o644))
				require.NoError(os.WriteFile(manifestPath, append(manifestJSON, '\n'), 0o644))
			}

			committed, err := os.ReadFile(binPath)
			require.NoError(err, "run this test with -update-python-sdk-fixtures to create the file")
			manifestJSON, err := os.ReadFile(manifestPath)
			require.NoError(err)
			var manifest pythonSDKFixture
			require.NoError(json.Unmarshal(manifestJSON, &manifest))
			require.Equal(codec.String(), manifest.Codec)
			require.Len(manifest.Blocks, len(blocks))

			// The Go reader accepts the committed file and returns the blocks.
			index, err := ParseIndex(committed)
			require.NoError(err)
			for i, want := range manifest.Blocks {
				block, chunk, err := index.LookupBlock(&api.BlockMetadata{
					Tag:                want.Tag,
					Hash:               want.Hash,
					Height:             want.Height,
					ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
					ByteOffset:         want.ByteOffset,
					ByteLength:         want.ByteLength,
					UncompressedLength: want.UncompressedLength,
				})
				require.NoError(err)
				frame := committed[chunk.CompressedPayloadOffset : chunk.CompressedPayloadOffset+chunk.CompressedLength]
				got, err := ExtractBlockPayloadsFromChunkFrame(bytes.NewReader(frame), index.Header.Codec, chunk, []*BlockDescriptor{block})
				require.NoError(err)
				require.Len(got, 1)
				sum := sha256.Sum256(got[0])
				require.Equal(want.PayloadSHA256, hex.EncodeToString(sum[:]))
				var decoded api.Block
				require.NoError(proto.Unmarshal(got[0], &decoded))
				require.True(proto.Equal(blocks[i], &decoded), "block %d differs", want.Height)
			}

			// The committed file has the layout the encoder writes today.
			// Fields that depend on the compressor's output are left out, so
			// that a compression library upgrade does not fail this test.
			freshIndex, err := ParseIndex(fresh)
			require.NoError(err)
			const update = "the format changed: rewrite the files with -update-python-sdk-fixtures and update the Python reader (sdk/python/src/chainstorage_sdk/cscb.py)"
			require.Equal(fresh[:8], committed[:8], update)
			require.Equal(pythonSDKStableHeader(freshIndex.Header), pythonSDKStableHeader(index.Header), update)
			require.Equal(freshIndex.Blocks, index.Blocks, update)
			require.Equal(pythonSDKStableChunks(freshIndex.Chunks), pythonSDKStableChunks(index.Chunks), update)
		})
	}
}

func pythonSDKFixtureBlocks() []*api.Block {
	const count = 6
	blocks := make([]*api.Block, count)
	for i := range blocks {
		height := uint64(1000 + i)
		header := make([]byte, 300+i*17)
		for j := range header {
			header[j] = byte('a' + (i+j)%26)
		}
		blocks[i] = &api.Block{
			Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
			Network:    common.Network_NETWORK_SOLANA_MAINNET,
			Metadata: &api.BlockMetadata{
				Tag:          2,
				Hash:         fmt.Sprintf("hash-%d", height),
				ParentHash:   fmt.Sprintf("hash-%d", height-1),
				Height:       height,
				ParentHeight: height - 1,
			},
			Blobdata: &api.Block_Solana{Solana: &api.SolanaBlobdata{Header: header}},
		}
	}
	return blocks
}

func pythonSDKStableHeader(header Header) Header {
	header.EnvelopeCRC32 = 0
	header.PayloadCompressedLength = 0
	header.PayloadCRC32 = 0
	return header
}

func pythonSDKStableChunks(chunks []ChunkDescriptor) []ChunkDescriptor {
	stable := make([]ChunkDescriptor, len(chunks))
	for i, chunk := range chunks {
		chunk.CompressedPayloadOffset = 0
		chunk.CompressedLength = 0
		stable[i] = chunk
	}
	return stable
}
