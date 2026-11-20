package downloader

import (
	"container/list"
	"hash/crc32"
	"io"
	"net/url"
	"sync"

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
	return value.(*cscb.Index), true
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
	return value.([]byte), true
}

func (c *cscbCache) putChunk(fileURL string, chunkIndex uint32, payload []byte) {
	c.chunks.put(cscbChunkKey{object: cscbObjectKey(fileURL), chunk: chunkIndex}, payload, int64(len(payload)))
}

// cscbObjectKey identifies the object behind a presigned link by dropping
// the query string (which holds the signature) and the fragment.
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

// decodeCSCBChunk decompresses a whole chunk frame and validates its length
// and CRC. It reads at most one byte past the expected length, so a frame
// that inflates further is refused rather than buffered.
func decodeCSCBChunk(frame io.Reader, codec api.Compression, chunk *cscb.ChunkDescriptor) ([]byte, error) {
	decompressor, err := cscb.NewChunkDecompressor(frame, codec)
	if err != nil {
		return nil, err
	}
	defer func() { _ = decompressor.Close() }()

	limit, err := limitReaderByLength(decompressor, chunk.UncompressedLength+1)
	if err != nil {
		return nil, err
	}
	payload, err := io.ReadAll(limit)
	if err != nil {
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
