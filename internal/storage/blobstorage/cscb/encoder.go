package cscb

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	HeaderSize           = 96
	EnvelopeHeaderSize   = 80
	BlockIndexRecordSize = 96
	ChunkIndexRecordSize = 72

	formatVersion           = 1
	codecGzip               = 1
	codecZstd               = 2
	compressionScopeChunked = 2

	fileSuffixGzip = ".cscb.gzip"
	fileSuffixZstd = ".cscb.zstd"
)

var (
	magic         = [4]byte{'C', 'S', 'C', 'B'}
	envelopeMagic = [4]byte{'E', 'N', 'V', '1'}
)

type (
	EncodeConfig struct {
		Blockchain                common.Blockchain
		Network                   common.Network
		SideChain                 api.SideChain
		Codec                     api.Compression
		CodecLevel                int
		ZstdLongDistanceWindowLog *int
		MaxBlocks                 uint64
		CompressionChunkBlocks    uint64
		MaxChunkUncompressedBytes *uint64
		MaxChunkCompressedBytes   *uint64
		MaxCompressedBytes        uint64
		MaxUncompressedBytes      uint64
		ShardSize                 uint64
		MemoryBudgetBytes         *uint64
		LocalSpillDir             string
	}

	Object struct {
		Key                       string
		SHA256                    string
		Length                    uint64
		PayloadUncompressedLength uint64
		PayloadCompressedLength   uint64
		Placements                []internal.BlockPlacement

		data     []byte
		tempFile string
	}

	blockRecord struct {
		height               uint64
		logicalPayloadOffset uint64
		payloadLength        uint64
		payloadCRC32         uint32
		chunkIndex           uint32
		chunkRelativeOffset  uint64
		hashSHA256           [32]byte
		metadataID           uint64
	}

	chunkRecord struct {
		index                   uint32
		startHeight             uint64
		endHeight               uint64
		compressedPayloadOffset uint64
		compressedLength        uint64
		uncompressedOffset      uint64
		uncompressedLength      uint64
		chunkCRC32              uint32
		blockCount              uint32
	}

	chunkBuilder struct {
		index              uint32
		startHeight        uint64
		endHeight          uint64
		uncompressedOffset uint64
		blockCount         uint32
		raw                bytes.Buffer
	}
)

func Encode(ctx context.Context, cfg EncodeConfig, blocks []internal.ConsolidatedBlockPayload) (*Object, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, xerrors.New("cannot encode empty CSCB object")
	}
	if cfg.MaxBlocks > 0 && uint64(len(blocks)) > cfg.MaxBlocks {
		return nil, xerrors.Errorf("CSCB block count %d exceeds max_blocks %d", len(blocks), cfg.MaxBlocks)
	}

	first := blocks[0].Metadata
	if first == nil {
		return nil, xerrors.New("block metadata is required")
	}
	tag := first.Tag
	startHeight := first.Height
	shardStart, shardEnd := shardBounds(startHeight, cfg.ShardSize)

	payloadSink, err := newSpillBuffer(cfg.LocalSpillDir, cfg.MemoryBudgetBytes)
	if err != nil {
		return nil, err
	}
	defer payloadSink.cleanup()

	var (
		blockRecords      []blockRecord
		chunkRecords      []chunkRecord
		placements        []internal.BlockPlacement
		currentChunk      *chunkBuilder
		payloadCRC        = crc32.NewIEEE()
		logicalOffset     uint64
		prevHeight        uint64
		totalUncompressed uint64
	)

	flushChunk := func() error {
		if currentChunk == nil || currentChunk.blockCount == 0 {
			return nil
		}
		rawChunk := currentChunk.raw.Bytes()
		compressed, err := compressChunk(rawChunk, cfg.Codec, cfg.CodecLevel, cfg.ZstdLongDistanceWindowLog)
		if err != nil {
			return err
		}
		if cfg.MaxChunkCompressedBytes != nil && uint64(len(compressed)) > *cfg.MaxChunkCompressedBytes && currentChunk.blockCount > 1 {
			return xerrors.Errorf("CSCB compressed chunk length %d exceeds max_chunk_compressed_bytes %d", len(compressed), *cfg.MaxChunkCompressedBytes)
		}
		record := chunkRecord{
			index:                   currentChunk.index,
			startHeight:             currentChunk.startHeight,
			endHeight:               currentChunk.endHeight,
			compressedPayloadOffset: payloadSink.size,
			compressedLength:        uint64(len(compressed)),
			uncompressedOffset:      currentChunk.uncompressedOffset,
			uncompressedLength:      uint64(len(rawChunk)),
			chunkCRC32:              crc32.ChecksumIEEE(rawChunk),
			blockCount:              currentChunk.blockCount,
		}
		if _, err := payloadSink.Write(compressed); err != nil {
			return xerrors.Errorf("failed to write compressed CSCB chunk: %w", err)
		}
		chunkRecords = append(chunkRecords, record)
		currentChunk = nil
		return nil
	}

	for i, payload := range blocks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		metadata := payload.Metadata
		if metadata == nil {
			return nil, xerrors.Errorf("block %d metadata is required", i)
		}
		if metadata.Skipped {
			return nil, xerrors.Errorf("skipped block cannot be stored in CSCB payload (height=%d)", metadata.Height)
		}
		if metadata.Tag != tag {
			return nil, xerrors.Errorf("CSCB object cannot mix tags: got %d want %d", metadata.Tag, tag)
		}
		if i > 0 && metadata.Height <= prevHeight {
			return nil, xerrors.Errorf("CSCB blocks must be strictly increasing by height: got %d after %d", metadata.Height, prevHeight)
		}
		if metadata.Height >= shardEnd {
			return nil, xerrors.Errorf("CSCB object crosses shard boundary: height=%d shard_end=%d", metadata.Height, shardEnd)
		}
		if payload.MetadataID < 0 {
			return nil, xerrors.Errorf("CSCB metadata id hint cannot be negative (height=%d metadata_id=%d)", metadata.Height, payload.MetadataID)
		}
		prevHeight = metadata.Height

		raw, err := readPayload(ctx, payload)
		if err != nil {
			return nil, xerrors.Errorf("failed to read CSCB payload at height %d: %w", metadata.Height, err)
		}
		if len(raw) == 0 {
			return nil, xerrors.Errorf("CSCB payload cannot be empty (height=%d)", metadata.Height)
		}
		if err := validateAdd(totalUncompressed, uint64(len(raw)), uint64(len(blocks)), cfg.MaxUncompressedBytes); err != nil {
			return nil, err
		}

		if currentChunk != nil && shouldFlushChunk(currentChunk, uint64(len(raw)), cfg) {
			if err := flushChunk(); err != nil {
				return nil, err
			}
		}
		if currentChunk == nil {
			currentChunk = &chunkBuilder{
				index:              uint32(len(chunkRecords)),
				startHeight:        metadata.Height,
				uncompressedOffset: logicalOffset,
			}
		}

		chunkRelativeOffset := uint64(currentChunk.raw.Len())
		if _, err := currentChunk.raw.Write(raw); err != nil {
			return nil, xerrors.Errorf("failed to buffer CSCB chunk: %w", err)
		}
		if _, err := payloadCRC.Write(raw); err != nil {
			return nil, xerrors.Errorf("failed to compute CSCB payload crc: %w", err)
		}
		hashDigest := sha256.Sum256([]byte(metadata.Hash))
		blockRecords = append(blockRecords, blockRecord{
			height:               metadata.Height,
			logicalPayloadOffset: logicalOffset,
			payloadLength:        uint64(len(raw)),
			payloadCRC32:         crc32.ChecksumIEEE(raw),
			chunkIndex:           currentChunk.index,
			chunkRelativeOffset:  chunkRelativeOffset,
			hashSHA256:           hashDigest,
			metadataID:           uint64(payload.MetadataID),
		})
		placements = append(placements, internal.BlockPlacement{
			MetadataID:         payload.MetadataID,
			Height:             metadata.Height,
			Hash:               metadata.Hash,
			ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:         logicalOffset,
			ByteLength:         uint64(len(raw)),
			UncompressedLength: uint64(len(raw)),
		})
		logicalOffset += uint64(len(raw))
		totalUncompressed += uint64(len(raw))
		currentChunk.blockCount++
		currentChunk.endHeight = metadata.Height + 1
	}
	if err := flushChunk(); err != nil {
		return nil, err
	}

	payloadCompressedLength := payloadSink.size
	if cfg.MaxCompressedBytes > 0 && payloadCompressedLength > cfg.MaxCompressedBytes && len(blocks) > 1 {
		return nil, xerrors.Errorf("CSCB compressed payload length %d exceeds max_compressed_bytes %d", payloadCompressedLength, cfg.MaxCompressedBytes)
	}

	endHeight := blocks[len(blocks)-1].Metadata.Height + 1
	envelope, err := buildEnvelope(blockRecords, chunkRecords, startHeight, endHeight)
	if err != nil {
		return nil, err
	}
	payloadOffset := uint64(HeaderSize + len(envelope))
	applyAbsoluteChunkOffsets(envelope, len(blockRecords), payloadOffset)
	header := buildHeader(cfg.Codec, uint64(len(blockRecords)), uint64(len(chunkRecords)), startHeight, endHeight, envelope, payloadCompressedLength, totalUncompressed, payloadCRC)
	object, err := materializeObject(cfg.LocalSpillDir, cfg.MemoryBudgetBytes, header, envelope, payloadSink)
	if err != nil {
		return nil, err
	}

	key := buildObjectKey(cfg, tag, shardStart, shardEnd, startHeight, endHeight, object.SHA256)
	object.Key = key
	object.PayloadUncompressedLength = totalUncompressed
	object.PayloadCompressedLength = payloadCompressedLength
	object.Placements = placements
	for i := range object.Placements {
		object.Placements[i].ObjectKey = key
	}
	return object, nil
}

func (o *Object) Open() (io.ReadCloser, error) {
	if o.tempFile != "" {
		return os.Open(o.tempFile)
	}
	return io.NopCloser(bytes.NewReader(o.data)), nil
}

func (o *Object) Bytes() ([]byte, bool) {
	if o.tempFile != "" {
		return nil, false
	}
	return o.data, true
}

func (o *Object) Close() error {
	if o.tempFile == "" {
		return nil
	}
	err := os.Remove(o.tempFile)
	o.tempFile = ""
	return err
}

func validateConfig(cfg EncodeConfig) error {
	switch cfg.Codec {
	case api.Compression_GZIP, api.Compression_ZSTD:
	default:
		return xerrors.Errorf("unsupported CSCB codec: %v", cfg.Codec)
	}
	if cfg.CodecLevel <= 0 {
		return xerrors.New("CSCB codec_level must be positive")
	}
	if cfg.Codec == api.Compression_GZIP && cfg.CodecLevel > gzip.BestCompression {
		return xerrors.Errorf("gzip CSCB codec_level must be between 1 and %d", gzip.BestCompression)
	}
	if cfg.Codec == api.Compression_ZSTD && cfg.ZstdLongDistanceWindowLog != nil {
		windowLog := *cfg.ZstdLongDistanceWindowLog
		if windowLog < 10 || windowLog > 29 {
			return xerrors.Errorf("zstd_long_distance_window_log must be between 10 and 29, got %d", windowLog)
		}
	}
	if cfg.CompressionChunkBlocks == 0 {
		return xerrors.New("CSCB compression_chunk_blocks must be positive")
	}
	if cfg.ShardSize == 0 {
		return xerrors.New("CSCB shard_size must be positive")
	}
	return nil
}

func readPayload(ctx context.Context, payload internal.ConsolidatedBlockPayload) ([]byte, error) {
	if payload.RawBlockPayload == nil {
		return nil, xerrors.New("raw block payload source is required")
	}
	reader, err := payload.RawBlockPayload.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	raw, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	expected := payload.RawBlockPayload.Length()
	if expected != uint64(len(raw)) {
		return nil, xerrors.Errorf("payload length mismatch: read %d want %d", len(raw), expected)
	}
	if payload.UncompressedLength != 0 && payload.UncompressedLength != uint64(len(raw)) {
		return nil, xerrors.Errorf("uncompressed length mismatch: payload=%d source=%d", payload.UncompressedLength, len(raw))
	}
	return raw, nil
}

func validateAdd(current uint64, next uint64, blockCount uint64, limit uint64) error {
	if limit == 0 {
		return nil
	}
	if current+next <= limit {
		return nil
	}
	if blockCount == 1 {
		return nil
	}
	return xerrors.Errorf("CSCB uncompressed payload length %d exceeds max_uncompressed_bytes %d", current+next, limit)
}

func shouldFlushChunk(chunk *chunkBuilder, nextPayloadLength uint64, cfg EncodeConfig) bool {
	if uint64(chunk.blockCount) >= cfg.CompressionChunkBlocks {
		return true
	}
	if cfg.MemoryBudgetBytes != nil && chunk.blockCount > 0 && uint64(chunk.raw.Len())+nextPayloadLength > *cfg.MemoryBudgetBytes {
		return true
	}
	if cfg.MaxChunkUncompressedBytes == nil {
		return false
	}
	if chunk.blockCount == 0 {
		return false
	}
	return uint64(chunk.raw.Len())+nextPayloadLength > *cfg.MaxChunkUncompressedBytes
}

func compressChunk(raw []byte, codec api.Compression, level int, windowLog *int) ([]byte, error) {
	switch codec {
	case api.Compression_GZIP:
		var buf bytes.Buffer
		writer, err := gzip.NewWriterLevel(&buf, level)
		if err != nil {
			return nil, xerrors.Errorf("failed to create gzip writer: %w", err)
		}
		if _, err := writer.Write(raw); err != nil {
			return nil, xerrors.Errorf("failed to write gzip CSCB chunk: %w", err)
		}
		if err := writer.Close(); err != nil {
			return nil, xerrors.Errorf("failed to close gzip CSCB chunk: %w", err)
		}
		return buf.Bytes(), nil
	case api.Compression_ZSTD:
		opts := []zstd.EOption{
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderCRC(false),
		}
		if windowLog != nil {
			opts = append(opts, zstd.WithWindowSize(1<<uint(*windowLog)))
		}
		writer, err := zstd.NewWriter(nil, opts...)
		if err != nil {
			return nil, xerrors.Errorf("failed to create zstd writer: %w", err)
		}
		defer func() { _ = writer.Close() }()
		return writer.EncodeAll(raw, nil), nil
	default:
		return nil, xerrors.Errorf("unsupported CSCB codec: %v", codec)
	}
}

func buildEnvelope(blocks []blockRecord, chunks []chunkRecord, startHeight uint64, endHeight uint64) ([]byte, error) {
	if len(chunks) == 0 {
		return nil, xerrors.New("CSCB object must contain at least one chunk")
	}
	blockIndexLength := len(blocks) * BlockIndexRecordSize
	chunkIndexOffset := EnvelopeHeaderSize + blockIndexLength
	chunkIndexLength := len(chunks) * ChunkIndexRecordSize
	envelope := make([]byte, EnvelopeHeaderSize+blockIndexLength+chunkIndexLength)
	copy(envelope[0:4], envelopeMagic[:])
	binary.LittleEndian.PutUint64(envelope[8:16], uint64(len(blocks)))
	binary.LittleEndian.PutUint64(envelope[16:24], uint64(len(chunks)))
	binary.LittleEndian.PutUint64(envelope[24:32], startHeight)
	binary.LittleEndian.PutUint64(envelope[32:40], endHeight)
	binary.LittleEndian.PutUint64(envelope[40:48], EnvelopeHeaderSize)
	binary.LittleEndian.PutUint64(envelope[48:56], uint64(blockIndexLength))
	binary.LittleEndian.PutUint64(envelope[56:64], uint64(chunkIndexOffset))
	binary.LittleEndian.PutUint64(envelope[64:72], uint64(chunkIndexLength))

	for i, block := range blocks {
		offset := EnvelopeHeaderSize + i*BlockIndexRecordSize
		writeBlockRecord(envelope[offset:offset+BlockIndexRecordSize], block)
	}
	for i, chunk := range chunks {
		offset := chunkIndexOffset + i*ChunkIndexRecordSize
		writeChunkRecord(envelope[offset:offset+ChunkIndexRecordSize], chunk)
	}
	return envelope, nil
}

func writeBlockRecord(buf []byte, record blockRecord) {
	binary.LittleEndian.PutUint64(buf[0:8], record.height)
	binary.LittleEndian.PutUint64(buf[8:16], record.logicalPayloadOffset)
	binary.LittleEndian.PutUint64(buf[16:24], record.payloadLength)
	binary.LittleEndian.PutUint32(buf[24:28], record.payloadCRC32)
	binary.LittleEndian.PutUint32(buf[28:32], record.chunkIndex)
	binary.LittleEndian.PutUint64(buf[32:40], record.chunkRelativeOffset)
	copy(buf[48:80], record.hashSHA256[:])
	binary.LittleEndian.PutUint64(buf[80:88], record.metadataID)
}

func writeChunkRecord(buf []byte, record chunkRecord) {
	binary.LittleEndian.PutUint32(buf[0:4], record.index)
	binary.LittleEndian.PutUint64(buf[8:16], record.startHeight)
	binary.LittleEndian.PutUint64(buf[16:24], record.endHeight)
	binary.LittleEndian.PutUint64(buf[24:32], record.compressedPayloadOffset)
	binary.LittleEndian.PutUint64(buf[32:40], record.compressedLength)
	binary.LittleEndian.PutUint64(buf[40:48], record.uncompressedOffset)
	binary.LittleEndian.PutUint64(buf[48:56], record.uncompressedLength)
	binary.LittleEndian.PutUint32(buf[56:60], record.chunkCRC32)
	binary.LittleEndian.PutUint32(buf[60:64], record.blockCount)
}

func applyAbsoluteChunkOffsets(envelope []byte, blockCount int, payloadOffset uint64) {
	chunkIndexOffset := EnvelopeHeaderSize + blockCount*BlockIndexRecordSize
	chunkCount := int(binary.LittleEndian.Uint64(envelope[16:24]))
	for i := 0; i < chunkCount; i++ {
		offset := chunkIndexOffset + i*ChunkIndexRecordSize + 24
		relative := binary.LittleEndian.Uint64(envelope[offset : offset+8])
		binary.LittleEndian.PutUint64(envelope[offset:offset+8], payloadOffset+relative)
	}
}

func buildHeader(codec api.Compression, blockCount uint64, chunkCount uint64, startHeight uint64, endHeight uint64, envelope []byte, payloadCompressedLength uint64, payloadUncompressedLength uint64, payloadCRC hash.Hash32) []byte {
	header := make([]byte, HeaderSize)
	copy(header[0:4], magic[:])
	header[4] = formatVersion
	header[5] = codecID(codec)
	header[6] = compressionScopeChunked
	binary.LittleEndian.PutUint32(header[8:12], uint32(blockCount))
	binary.LittleEndian.PutUint32(header[12:16], uint32(chunkCount))
	binary.LittleEndian.PutUint64(header[16:24], startHeight)
	binary.LittleEndian.PutUint64(header[24:32], endHeight)
	binary.LittleEndian.PutUint64(header[32:40], HeaderSize)
	binary.LittleEndian.PutUint64(header[40:48], uint64(len(envelope)))
	binary.LittleEndian.PutUint32(header[48:52], crc32.ChecksumIEEE(envelope))
	binary.LittleEndian.PutUint32(header[52:56], BlockIndexRecordSize)
	binary.LittleEndian.PutUint32(header[56:60], ChunkIndexRecordSize)
	binary.LittleEndian.PutUint64(header[60:68], uint64(HeaderSize+len(envelope)))
	binary.LittleEndian.PutUint64(header[68:76], payloadCompressedLength)
	binary.LittleEndian.PutUint64(header[76:84], payloadUncompressedLength)
	binary.LittleEndian.PutUint32(header[84:88], payloadCRC.Sum32())
	return header
}

func codecID(codec api.Compression) byte {
	switch codec {
	case api.Compression_GZIP:
		return codecGzip
	case api.Compression_ZSTD:
		return codecZstd
	default:
		return 0
	}
}

func codecSuffix(codec api.Compression) string {
	switch codec {
	case api.Compression_GZIP:
		return fileSuffixGzip
	case api.Compression_ZSTD:
		return fileSuffixZstd
	default:
		return ""
	}
}

func materializeObject(dir string, memoryBudget *uint64, header []byte, envelope []byte, payload *spillBuffer) (*Object, error) {
	length := uint64(len(header)+len(envelope)) + payload.size
	if payload.inMemory() && (memoryBudget == nil || length <= *memoryBudget) {
		data := make([]byte, 0, length)
		data = append(data, header...)
		data = append(data, envelope...)
		data = append(data, payload.buf.Bytes()...)
		sum := sha256.Sum256(data)
		return &Object{
			SHA256: hex.EncodeToString(sum[:]),
			Length: length,
			data:   data,
		}, nil
	}

	file, err := createTemp(dir, "chainstorage-cscb-object-*.tmp")
	if err != nil {
		return nil, xerrors.Errorf("failed to create CSCB object temp file: %w", err)
	}
	var success bool
	defer func() {
		if !success {
			_ = file.Close()
			_ = os.Remove(file.Name())
		}
	}()

	sha := sha256.New()
	writer := io.MultiWriter(file, sha)
	if _, err := writer.Write(header); err != nil {
		return nil, xerrors.Errorf("failed to write CSCB header: %w", err)
	}
	if _, err := writer.Write(envelope); err != nil {
		return nil, xerrors.Errorf("failed to write CSCB envelope: %w", err)
	}
	payloadReader, err := payload.Open()
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(writer, payloadReader); err != nil {
		_ = payloadReader.Close()
		return nil, xerrors.Errorf("failed to write CSCB payload: %w", err)
	}
	if err := payloadReader.Close(); err != nil {
		return nil, xerrors.Errorf("failed to close CSCB payload reader: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, xerrors.Errorf("failed to close CSCB object temp file: %w", err)
	}
	success = true
	return &Object{
		SHA256:   hex.EncodeToString(sha.Sum(nil)),
		Length:   length,
		tempFile: file.Name(),
	}, nil
}

func buildObjectKey(cfg EncodeConfig, tag uint32, shardStart uint64, shardEnd uint64, startHeight uint64, endHeight uint64, sha string) string {
	prefix := fmt.Sprintf("%s/%s", cfg.Blockchain, cfg.Network)
	if cfg.SideChain != api.SideChain_SIDECHAIN_NONE {
		prefix = fmt.Sprintf("%s/%s", prefix, cfg.SideChain)
	}
	return fmt.Sprintf(
		"%s/consolidated/v=%d/shard=%020d-%020d/%020d-%020d-%s%s",
		prefix,
		tag,
		shardStart,
		shardEnd,
		startHeight,
		endHeight,
		sha,
		codecSuffix(cfg.Codec),
	)
}

func shardBounds(height uint64, shardSize uint64) (uint64, uint64) {
	start := (height / shardSize) * shardSize
	return start, start + shardSize
}

type spillBuffer struct {
	dir   string
	limit *uint64
	buf   bytes.Buffer
	file  *os.File
	size  uint64
}

func newSpillBuffer(dir string, limit *uint64) (*spillBuffer, error) {
	return &spillBuffer{
		dir:   dir,
		limit: limit,
	}, nil
}

func (b *spillBuffer) Write(p []byte) (int, error) {
	if b.file == nil && b.limit != nil && uint64(b.buf.Len()+len(p)) > *b.limit {
		file, err := createTemp(b.dir, "chainstorage-cscb-payload-*.tmp")
		if err != nil {
			return 0, xerrors.Errorf("failed to create CSCB payload temp file: %w", err)
		}
		if _, err := file.Write(b.buf.Bytes()); err != nil {
			_ = file.Close()
			_ = os.Remove(file.Name())
			return 0, xerrors.Errorf("failed to spill CSCB payload buffer: %w", err)
		}
		b.buf.Reset()
		b.file = file
	}
	var n int
	var err error
	if b.file != nil {
		n, err = b.file.Write(p)
	} else {
		n, err = b.buf.Write(p)
	}
	b.size += uint64(n)
	return n, err
}

func (b *spillBuffer) Open() (io.ReadCloser, error) {
	if b.file == nil {
		return io.NopCloser(bytes.NewReader(b.buf.Bytes())), nil
	}
	if err := b.file.Sync(); err != nil {
		return nil, xerrors.Errorf("failed to sync CSCB payload temp file: %w", err)
	}
	return os.Open(b.file.Name())
}

func (b *spillBuffer) inMemory() bool {
	return b.file == nil
}

func (b *spillBuffer) cleanup() {
	if b.file == nil {
		return
	}
	name := b.file.Name()
	_ = b.file.Close()
	_ = os.Remove(name)
}

func spillDir(dir string) string {
	if dir == "" {
		return os.TempDir()
	}
	return filepath.Clean(dir)
}

func createTemp(dir string, pattern string) (*os.File, error) {
	spillPath := spillDir(dir)
	if err := os.MkdirAll(spillPath, 0o700); err != nil {
		return nil, err
	}
	return os.CreateTemp(spillPath, pattern)
}
