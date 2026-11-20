package downloader

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func (s *blockDownloaderTestSuite) newCSCBCacheTestDownloader(server *httptest.Server) *blockDownloaderImpl {
	s.app = testapp.New(
		s.T(),
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)
	impl, ok := s.downloader.(*blockDownloaderImpl)
	s.Require().True(ok)
	return impl
}

func (s *blockDownloaderTestSuite) requireCSCBBlock(fixture *cscbDownloaderFixture, blockFile *api.BlockFile, actual *api.Block) {
	expected := proto.Clone(fixture.blocks[blockFile.GetHeight()-100]).(*api.Block)
	expected.Metadata = blockFileToMetadata(blockFile)
	testutil.Require(s.T()).Equal(expected, actual)
}

func chunkRangeHeader(fixture *cscbDownloaderFixture, chunkIndex int) string {
	chunk := fixture.index.Chunks[chunkIndex]
	return rangeHeader(chunk.CompressedPayloadOffset, chunk.CompressedLength)
}

func (s *blockDownloaderTestSuite) TestDownload_CSCBSequentialHeightsFetchEachChunkOnce() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 6, 3)
	defer fixture.close()
	require.Len(fixture.index.Chunks, 2)

	server, recorder := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}
	s.newCSCBCacheTestDownloader(server)

	for _, blockFile := range fixture.blockFiles {
		rawBlock, err := s.downloader.Download(context.Background(), blockFile)
		require.NoError(err)
		s.requireCSCBBlock(fixture, blockFile, rawBlock)
	}

	// One index read and one read per chunk, where the uncached path read
	// the index and the chunk again for each of the six blocks.
	require.Equal([]string{
		"bytes=0-65535",
		chunkRangeHeader(fixture, 0),
		chunkRangeHeader(fixture, 1),
	}, recorder.rangesSnapshot())
}

func (s *blockDownloaderTestSuite) TestDownload_CSCBCacheKeyIgnoresPresignedQuery() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 2, 2)
	defer fixture.close()

	server, recorder := newRangeHTTPServer(fixture.data)
	defer server.Close()
	fixture.blockFiles[0].FileUrl = server.URL + "/base/consolidated/object.zstd?X-Amz-Signature=first"
	fixture.blockFiles[1].FileUrl = server.URL + "/base/consolidated/object.zstd?X-Amz-Signature=second"
	s.newCSCBCacheTestDownloader(server)

	for _, blockFile := range fixture.blockFiles {
		rawBlock, err := s.downloader.Download(context.Background(), blockFile)
		require.NoError(err)
		s.requireCSCBBlock(fixture, blockFile, rawBlock)
	}

	require.Equal([]string{
		"bytes=0-65535",
		chunkRangeHeader(fixture, 0),
	}, recorder.rangesSnapshot())
}

func (s *blockDownloaderTestSuite) TestDownload_CSCBObjectsDoNotShareCacheEntries() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 2, 2)
	defer fixture.close()

	server, recorder := newRangeHTTPServer(fixture.data)
	defer server.Close()
	s.newCSCBCacheTestDownloader(server)

	for _, object := range []string{"/first.zstd", "/second.zstd"} {
		blockFile := proto.Clone(fixture.blockFiles[0]).(*api.BlockFile)
		blockFile.FileUrl = server.URL + object
		rawBlock, err := s.downloader.Download(context.Background(), blockFile)
		require.NoError(err)
		s.requireCSCBBlock(fixture, blockFile, rawBlock)
	}

	require.Equal([]string{
		"bytes=0-65535",
		chunkRangeHeader(fixture, 0),
		"bytes=0-65535",
		chunkRangeHeader(fixture, 0),
	}, recorder.rangesSnapshot())
}

func (s *blockDownloaderTestSuite) TestDownload_CSCBChunkLargerThanCacheIsStreamedEachTime() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 2, 2)
	defer fixture.close()
	require.Len(fixture.index.Chunks, 1)

	server, recorder := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}
	downloader := s.newCSCBCacheTestDownloader(server)
	chunkBytes := int64(fixture.index.Chunks[0].UncompressedLength) - 1
	downloader.cscbCache = newCSCBCache(cscbIndexCacheEntries, cscbChunkCacheEntries, chunkBytes)

	for _, blockFile := range fixture.blockFiles {
		rawBlock, err := s.downloader.Download(context.Background(), blockFile)
		require.NoError(err)
		s.requireCSCBBlock(fixture, blockFile, rawBlock)
	}

	// The index is still read once; the chunk is read for each block.
	require.Equal([]string{
		"bytes=0-65535",
		chunkRangeHeader(fixture, 0),
		chunkRangeHeader(fixture, 0),
	}, recorder.rangesSnapshot())
}

func (s *blockDownloaderTestSuite) TestDownload_CSCBFailedChunkReadIsNotCached() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 2, 2)
	defer fixture.close()

	chunkRange := chunkRangeHeader(fixture, 0)
	recorder := &rangeRequestRecorder{}
	var refused atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		rangeValue := request.Header.Get("Range")
		done := recorder.record(rangeValue)
		defer done()
		if rangeValue == chunkRange && refused.CompareAndSwap(false, true) {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writeRangeResponse(writer, rangeValue, fixture.data)
	}))
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}
	s.newCSCBCacheTestDownloader(server)

	_, err := s.downloader.Download(context.Background(), fixture.blockFiles[0])
	require.Error(err)

	rawBlock, err := s.downloader.Download(context.Background(), fixture.blockFiles[0])
	require.NoError(err)
	s.requireCSCBBlock(fixture, fixture.blockFiles[0], rawBlock)

	require.Equal([]string{
		"bytes=0-65535",
		chunkRange,
		chunkRange,
	}, recorder.rangesSnapshot())
}

func (s *blockDownloaderTestSuite) TestDownload_CSCBConcurrentReadsWithEviction() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 8, 2)
	defer fixture.close()
	require.Len(fixture.index.Chunks, 4)

	server, _ := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}
	downloader := s.newCSCBCacheTestDownloader(server)
	// Fewer chunk entries than chunks, so readers evict each other's chunks.
	downloader.cscbCache = newCSCBCache(cscbIndexCacheEntries, 2, cscbChunkCacheBytes)

	const workers = 4
	results := make([][]*api.Block, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range fixture.blockFiles {
				// Each worker starts at a different height.
				blockFile := fixture.blockFiles[(i+worker*2)%len(fixture.blockFiles)]
				rawBlock, err := s.downloader.Download(context.Background(), blockFile)
				if err != nil {
					errs[worker] = xerrors.Errorf("height %d: %w", blockFile.GetHeight(), err)
					return
				}
				results[worker] = append(results[worker], rawBlock)
			}
		}()
	}
	wg.Wait()

	for worker := 0; worker < workers; worker++ {
		require.NoError(errs[worker])
		require.Len(results[worker], len(fixture.blockFiles))
		for _, rawBlock := range results[worker] {
			blockFile := fixture.blockFiles[rawBlock.GetMetadata().GetHeight()-100]
			s.requireCSCBBlock(fixture, blockFile, rawBlock)
		}
	}
}

func (s *blockDownloaderTestSuite) TestDownloadStream_CSCBReusesCachedChunk() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 2, 2)
	defer fixture.close()

	server, recorder := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}
	s.newCSCBCacheTestDownloader(server)

	rawBlock, err := s.downloader.Download(context.Background(), fixture.blockFiles[0])
	require.NoError(err)
	s.requireCSCBBlock(fixture, fixture.blockFiles[0], rawBlock)

	spooled, err := s.downloader.DownloadStream(context.Background(), fixture.blockFiles[1])
	require.NoError(err)
	defer spooled.Close()
	rc, err := spooled.Open()
	require.NoError(err)
	defer rc.Close()
	gotBytes, err := io.ReadAll(rc)
	require.NoError(err)
	var spooledBlock api.Block
	require.NoError(proto.Unmarshal(gotBytes, &spooledBlock))
	require.Equal(fixture.blocks[1], &spooledBlock)

	require.Equal([]string{
		"bytes=0-65535",
		chunkRangeHeader(fixture, 0),
	}, recorder.rangesSnapshot())
}

func TestCSCBObjectKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "drops the presigned query",
			input: "https://bucket.s3.us-east-1.amazonaws.com/base/consolidated/a.zstd?X-Amz-Date=20261120T000000Z&X-Amz-Signature=abc",
			want:  "https://bucket.s3.us-east-1.amazonaws.com/base/consolidated/a.zstd",
		},
		{
			name:  "drops an empty query and a fragment",
			input: "https://host/a.zstd?#part",
			want:  "https://host/a.zstd",
		},
		{
			name:  "keeps a link without a query",
			input: "http://127.0.0.1:8080/a/b.zstd",
			want:  "http://127.0.0.1:8080/a/b.zstd",
		},
		{
			name:  "keeps an unparsable link as it is",
			input: "http://[::1",
			want:  "http://[::1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testutil.Require(t).Equal(test.want, cscbObjectKey(test.input))
		})
	}
}

func TestLRUCache(t *testing.T) {
	t.Run("evicts the least recently used entry", func(t *testing.T) {
		require := testutil.Require(t)
		cache := newLRUCache(2, 0)
		cache.put("a", 1, 0)
		cache.put("b", 2, 0)
		_, ok := cache.get("a")
		require.True(ok)
		cache.put("c", 3, 0)

		_, ok = cache.get("b")
		require.False(ok)
		value, ok := cache.get("a")
		require.True(ok)
		require.Equal(1, value)
		value, ok = cache.get("c")
		require.True(ok)
		require.Equal(3, value)
	})

	t.Run("evicts to stay within the byte limit", func(t *testing.T) {
		require := testutil.Require(t)
		cache := newLRUCache(4, 10)
		cache.put("a", "a", 4)
		cache.put("b", "b", 4)
		cache.put("c", "c", 4)

		_, ok := cache.get("a")
		require.False(ok)
		require.Equal(int64(8), cache.bytes)
		require.Equal(2, cache.order.Len())
	})

	t.Run("does not keep an entry larger than the byte limit", func(t *testing.T) {
		require := testutil.Require(t)
		cache := newLRUCache(4, 10)
		cache.put("a", "a", 4)
		cache.put("big", "big", 11)

		_, ok := cache.get("big")
		require.False(ok)
		_, ok = cache.get("a")
		require.True(ok)
		require.Equal(int64(4), cache.bytes)
	})

	t.Run("replacing an entry replaces its size", func(t *testing.T) {
		require := testutil.Require(t)
		cache := newLRUCache(4, 10)
		cache.put("a", "first", 6)
		cache.put("a", "second", 3)

		value, ok := cache.get("a")
		require.True(ok)
		require.Equal("second", value)
		require.Equal(int64(3), cache.bytes)
		require.Equal(1, cache.order.Len())
	})

	t.Run("keeps nothing without entries", func(t *testing.T) {
		require := testutil.Require(t)
		cache := newLRUCache(0, 0)
		cache.put("a", 1, 0)

		_, ok := cache.get("a")
		require.False(ok)
	})
}

func TestDecodeCSCBChunk(t *testing.T) {
	fixture := newCSCBDownloaderFixture(t, 4, 2)
	defer fixture.close()
	codec := fixture.index.Header.Codec
	chunk := fixture.index.Chunks[1]
	frame := fixture.data[chunk.CompressedPayloadOffset : chunk.CompressedPayloadOffset+chunk.CompressedLength]

	t.Run("decodes and validates a chunk", func(t *testing.T) {
		require := testutil.Require(t)
		payload, err := decodeCSCBChunk(bytes.NewReader(frame), codec, &chunk)
		require.NoError(err)
		require.Len(payload, int(chunk.UncompressedLength))
		require.NoError(cscb.ValidateChunkPayload(payload, &chunk))
	})

	t.Run("decodes into a buffer of exactly the chunk length", func(t *testing.T) {
		require := testutil.Require(t)
		payload, err := decodeCSCBChunk(bytes.NewReader(frame), codec, &chunk)
		require.NoError(err)
		require.Equal(int(chunk.UncompressedLength), cap(payload))
	})

	t.Run("refuses a frame that inflates past the chunk length", func(t *testing.T) {
		require := testutil.Require(t)
		short := chunk
		short.UncompressedLength = 1
		_, err := decodeCSCBChunk(bytes.NewReader(frame), codec, &short)
		require.ErrorContains(err, "inflates past its length 1")
	})

	t.Run("refuses a frame shorter than the chunk length", func(t *testing.T) {
		require := testutil.Require(t)
		long := chunk
		long.UncompressedLength++
		_, err := decodeCSCBChunk(bytes.NewReader(frame), codec, &long)
		require.ErrorIs(err, io.ErrUnexpectedEOF)
	})

	t.Run("refuses a chunk whose CRC does not match", func(t *testing.T) {
		require := testutil.Require(t)
		wrong := chunk
		wrong.ChunkCRC32++
		_, err := decodeCSCBChunk(bytes.NewReader(frame), codec, &wrong)
		require.ErrorContains(err, "CSCB chunk CRC mismatch")
	})

	t.Run("refuses a truncated frame", func(t *testing.T) {
		require := testutil.Require(t)
		_, err := decodeCSCBChunk(bytes.NewReader(frame[:len(frame)/2]), codec, &chunk)
		require.Error(err)
	})
}

func TestCSCBBlockPayloadFromChunk(t *testing.T) {
	require := testutil.Require(t)
	fixture := newCSCBDownloaderFixture(t, 4, 2)
	defer fixture.close()
	chunk := fixture.index.Chunks[1]
	frame := fixture.data[chunk.CompressedPayloadOffset : chunk.CompressedPayloadOffset+chunk.CompressedLength]
	chunkPayload, err := decodeCSCBChunk(bytes.NewReader(frame), fixture.index.Header.Codec, &chunk)
	require.NoError(err)

	var inChunk []cscb.BlockDescriptor
	var otherChunk *cscb.BlockDescriptor
	for i := range fixture.index.Blocks {
		block := fixture.index.Blocks[i]
		if block.ChunkIndex == chunk.Index {
			inChunk = append(inChunk, block)
		} else {
			otherChunk = &block
		}
	}
	require.Len(inChunk, 2)
	require.NotNil(otherChunk)

	t.Run("returns each block of the chunk", func(t *testing.T) {
		require := testutil.Require(t)
		for i := range inChunk {
			block := inChunk[i]
			payload, err := cscbBlockPayloadFromChunk(chunkPayload, &chunk, &block)
			require.NoError(err)
			require.Equal(len(payload), cap(payload))
			var decoded api.Block
			require.NoError(proto.Unmarshal(payload, &decoded))
			require.Equal(fixture.blocks[block.Height-100], &decoded)
		}
	})

	t.Run("refuses a block of another chunk", func(t *testing.T) {
		_, err := cscbBlockPayloadFromChunk(chunkPayload, &chunk, otherChunk)
		testutil.Require(t).ErrorContains(err, "CSCB block chunk mismatch")
	})

	t.Run("refuses a block past the end of the chunk", func(t *testing.T) {
		block := inChunk[1]
		block.ChunkRelativeOffset = uint64(len(chunkPayload))
		_, err := cscbBlockPayloadFromChunk(chunkPayload, &chunk, &block)
		testutil.Require(t).ErrorContains(err, "exceeds chunk bounds")
	})

	t.Run("refuses a block whose end overflows", func(t *testing.T) {
		block := inChunk[1]
		block.ChunkRelativeOffset = math.MaxUint64
		_, err := cscbBlockPayloadFromChunk(chunkPayload, &chunk, &block)
		testutil.Require(t).Error(err)
	})

	t.Run("refuses a block whose CRC does not match", func(t *testing.T) {
		block := inChunk[0]
		block.PayloadCRC32++
		_, err := cscbBlockPayloadFromChunk(chunkPayload, &chunk, &block)
		testutil.Require(t).ErrorContains(err, "CSCB block CRC mismatch")
	})
}
