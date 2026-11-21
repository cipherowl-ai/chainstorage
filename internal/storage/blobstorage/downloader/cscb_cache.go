package downloader

import (
	"container/list"
	"hash/crc32"
	"io"
	"math"
	"net/url"
	"strconv"
	"sync"

	"golang.org/x/sync/singleflight"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// A single-block read of a consolidated block needs the object's index and
// the decompressed chunk that holds the block. Without a cache, a reader
// that walks consecutive heights (StreamChainEvents, for one) fetches the
// index and the whole chunk again for every block in the chunk. These limits
// match the Python SDK (sdk/python/src/chainstorage_sdk/downloader.py).
const (
	cscbIndexCacheEntries = 8
	cscbChunkCacheEntries = 4
	cscbChunkCacheBytes   = 128 * 1024 * 1024
)

type (
	// cscbCache keeps parsed CSCB indexes and decompressed, validated
	// chunks for the next read. Entries are keyed by the object, not by its
	// link: the server presigns a new link for every request, and only the
	// query string changes. A CSCB object key carries the SHA of its
	// content, so an entry is never stale.
	cscbCache struct {
		indexes *lruCache
		chunks  *lruCache
		// chunkFetches lets concurrent misses on one chunk share a single
		// fetch, so the chunk is downloaded and held once, not per caller.
		chunkFetches singleflight.Group
	}

	cscbChunkKey struct {
		object string
		chunk  uint32
	}

	// lruCache keeps the most recently used entries within an entry limit
	// and, when maxBytes is positive, a byte limit. An entry larger than
	// maxBytes is not kept. It is safe for concurrent use.
	lruCache struct {
		mu         sync.Mutex
		maxEntries int
		maxBytes   int64
		bytes      int64
		order      *list.List
		entries    map[any]*list.Element
	}

	lruEntry struct {
		key   any
		value any
		size  int64
	}
)

func newCSCBCache(indexEntries int, chunkEntries int, chunkBytes int64) *cscbCache {
	return &cscbCache{
		indexes: newLRUCache(indexEntries, 0),
		chunks:  newLRUCache(chunkEntries, chunkBytes),
	}
}

func (c *cscbCache) index(fileURL string) (*cscb.Index, bool) {
	value, ok := c.indexes.get(cscbObjectKey(fileURL))
	if !ok {
		return nil, false
	}
	index, ok := value.(*cscb.Index)
	return index, ok
}

func (c *cscbCache) putIndex(fileURL string, index *cscb.Index) {
	c.indexes.put(cscbObjectKey(fileURL), index, 0)
}

// cachesChunk reports whether a chunk of this size can be kept. A larger
// chunk is read by streaming up to the block instead, as before the cache,
// so it is never held in memory whole.
func (c *cscbCache) cachesChunk(chunk *cscb.ChunkDescriptor) bool {
	return c.chunks.maxEntries > 0 &&
		c.chunks.maxBytes > 0 &&
		chunk.UncompressedLength > 0 &&
		chunk.UncompressedLength <= uint64(c.chunks.maxBytes)
}

func (c *cscbCache) chunk(fileURL string, chunkIndex uint32) ([]byte, bool) {
	value, ok := c.chunks.get(cscbChunkKey{object: cscbObjectKey(fileURL), chunk: chunkIndex})
	if !ok {
		return nil, false
	}
	payload, ok := value.([]byte)
	return payload, ok
}

func (c *cscbCache) putChunk(fileURL string, chunkIndex uint32, payload []byte) {
	c.chunks.put(cscbChunkKey{object: cscbObjectKey(fileURL), chunk: chunkIndex}, payload, int64(len(payload)))
}

// loadChunk returns the cached chunk, or fetches it once for all concurrent
// callers and caches it. A failed fetch is not cached; callers that joined it
// get its error, including one caused by the first caller's context ending.
func (c *cscbCache) loadChunk(fileURL string, chunkIndex uint32, fetch func() ([]byte, error)) ([]byte, error) {
	if payload, ok := c.chunk(fileURL, chunkIndex); ok {
		return payload, nil
	}
	key := cscbObjectKey(fileURL) + "#" + strconv.FormatUint(uint64(chunkIndex), 10)
	value, err, _ := c.chunkFetches.Do(key, func() (any, error) {
		if payload, ok := c.chunk(fileURL, chunkIndex); ok {
			return payload, nil
		}
		payload, err := fetch()
		if err != nil {
			return nil, err
		}
		c.putChunk(fileURL, chunkIndex, payload)
		return payload, nil
	})
	if err != nil {
		return nil, err
	}
	payload, ok := value.([]byte)
	if !ok {
		return nil, xerrors.Errorf("unexpected CSCB chunk cache value %T", value)
	}
	return payload, nil
}

// cscbObjectKey identifies the object behind a presigned link by dropping
// the query string (which holds the signature) and the fragment. It assumes
// the object is named by the link's host and path, as S3 and GCS presigned
// links are.
func cscbObjectKey(fileURL string) string {
	parsed, err := url.Parse(fileURL)
	if err != nil {
		return fileURL
	}
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.RawFragment = ""
	return parsed.String()
}

func newLRUCache(maxEntries int, maxBytes int64) *lruCache {
	return &lruCache{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		order:      list.New(),
		entries:    make(map[any]*list.Element),
	}
}

func (c *lruCache) get(key any) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(element)
	return element.Value.(*lruEntry).value, true
}

func (c *lruCache) put(key any, value any, size int64) {
	if c.maxEntries <= 0 || (c.maxBytes > 0 && size > c.maxBytes) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[key]; ok {
		c.removeElement(element)
	}
	c.entries[key] = c.order.PushFront(&lruEntry{key: key, value: value, size: size})
	c.bytes += size
	for c.order.Len() > c.maxEntries || (c.maxBytes > 0 && c.bytes > c.maxBytes) {
		c.removeElement(c.order.Back())
	}
}

func (c *lruCache) removeElement(element *list.Element) {
	entry := c.order.Remove(element).(*lruEntry)
	delete(c.entries, entry.key)
	c.bytes -= entry.size
}

// decodeCSCBChunk decompresses a whole chunk frame into a buffer of exactly
// the chunk's length and validates its CRC. A frame that inflates past that
// length is refused, so the buffer, and the memory the chunk cache counts,
// is never larger than the descriptor says. The caller bounds the length
// (cachesChunk).
func decodeCSCBChunk(frame io.Reader, codec api.Compression, chunk *cscb.ChunkDescriptor) ([]byte, error) {
	if chunk.UncompressedLength > math.MaxInt32 {
		return nil, xerrors.Errorf("CSCB chunk %d length %d is too large to decode whole", chunk.Index, chunk.UncompressedLength)
	}
	decompressor, err := cscb.NewChunkDecompressor(frame, codec)
	if err != nil {
		return nil, err
	}
	defer func() { _ = decompressor.Close() }()

	payload := make([]byte, int(chunk.UncompressedLength))
	if _, err := io.ReadFull(decompressor, payload); err != nil {
		return nil, xerrors.Errorf("failed to decompress CSCB chunk %d: %w", chunk.Index, err)
	}
	var extra [1]byte
	switch _, err := io.ReadFull(decompressor, extra[:]); {
	case err == io.EOF:
	case err == nil:
		return nil, xerrors.Errorf("CSCB chunk %d inflates past its length %d", chunk.Index, chunk.UncompressedLength)
	default:
		return nil, xerrors.Errorf("failed to decompress CSCB chunk %d: %w", chunk.Index, err)
	}
	if err := cscb.ValidateChunkPayload(payload, chunk); err != nil {
		return nil, err
	}
	return payload, nil
}

// cscbBlockPayloadFromChunk returns the block's payload from a decompressed
// chunk, after checking its bounds and CRC. The result shares memory with
// chunkPayload, which may be cached, so callers must not modify it; its
// capacity ends with the block, so an append cannot reach the next one.
func cscbBlockPayloadFromChunk(chunkPayload []byte, chunk *cscb.ChunkDescriptor, block *cscb.BlockDescriptor) ([]byte, error) {
	if block.ChunkIndex != chunk.Index {
		return nil, xerrors.Errorf("CSCB block chunk mismatch at height %d: block=%d chunk=%d", block.Height, block.ChunkIndex, chunk.Index)
	}
	end, err := cscb.BlockPayloadEnd(block)
	if err != nil {
		return nil, err
	}
	if end > uint64(len(chunkPayload)) {
		return nil, xerrors.Errorf("CSCB block payload exceeds chunk bounds at height %d: end=%d chunk_length=%d", block.Height, end, len(chunkPayload))
	}
	payload := chunkPayload[block.ChunkRelativeOffset:end:end]
	if crc := crc32.ChecksumIEEE(payload); crc != block.PayloadCRC32 {
		return nil, xerrors.Errorf("CSCB block CRC mismatch at height %d: got %08x want %08x", block.Height, crc, block.PayloadCRC32)
	}
	return payload, nil
}
