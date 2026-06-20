package s3

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	storageerrors "github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BlobStorageParams struct {
		fx.In
		fxparams.Params
		Client     s3.Client
		Downloader s3.Downloader
		Uploader   s3.Uploader
	}

	blobStorageFactory struct {
		params BlobStorageParams
	}

	blobStorageImpl struct {
		logger                 *zap.Logger
		config                 *config.Config
		bucket                 string
		client                 s3.Client
		downloader             s3.Downloader
		uploader               s3.Uploader
		blobStorageMetrics     *blobStorageMetrics
		instrumentUpload       instrument.InstrumentWithResult[string]
		instrumentUploadRaw    instrument.InstrumentWithResult[string]
		instrumentDownload     instrument.InstrumentWithResult[*api.Block]
		instrumentDownloadMany instrument.InstrumentWithResult[[]*api.Block]
	}

	blobStorageMetrics struct {
		blobDownloadedSize tally.Timer
		blobUploadedSize   tally.Timer
	}

	downloadRef struct {
		index    int
		metadata *api.BlockMetadata
	}

	cscbBlockDownload struct {
		ref   downloadRef
		block *cscb.BlockDescriptor
		chunk *cscb.ChunkDescriptor
	}
)

const (
	blobUploaderScopeName   = "uploader"
	blobDownloaderScopeName = "downloader"
	blobSizeMetricName      = "blob_size"

	cscbMetadataFormat             = "chainstorage-format"
	cscbMetadataCompressionScope   = "chainstorage-compression-scope"
	cscbMetadataSHA256             = "chainstorage-sha256"
	cscbMetadataCodec              = "chainstorage-codec"
	cscbMetadataUncompressedLength = "chainstorage-uncompressed-length"

	maxSinglePutObjectSize = 5 * 1024 * 1024 * 1024

	cscbInitialIndexReadSize = 64 * 1024
)

var _ internal.BlobStorage = (*blobStorageImpl)(nil)

func NewFactory(params BlobStorageParams) internal.BlobStorageFactory {
	return &blobStorageFactory{params}
}

// Create implements BlobStorageFactory.
func (f *blobStorageFactory) Create() (internal.BlobStorage, error) {
	return New(f.params)
}

func New(params BlobStorageParams) (internal.BlobStorage, error) {
	metrics := params.Metrics.SubScope("blob_storage").Tagged(map[string]string{
		"storage_type": "s3",
	})
	return &blobStorageImpl{
		logger:                 log.WithPackage(params.Logger),
		config:                 params.Config,
		bucket:                 params.Config.AWS.Bucket,
		client:                 params.Client,
		downloader:             params.Downloader,
		uploader:               params.Uploader,
		blobStorageMetrics:     newBlobStorageMetrics(metrics),
		instrumentUpload:       instrument.NewWithResult[string](metrics, "upload"),
		instrumentUploadRaw:    instrument.NewWithResult[string](metrics, "upload_raw"),
		instrumentDownload:     instrument.NewWithResult[*api.Block](metrics, "download"),
		instrumentDownloadMany: instrument.NewWithResult[[]*api.Block](metrics, "download_many"),
	}, nil
}

func newBlobStorageMetrics(scope tally.Scope) *blobStorageMetrics {
	return &blobStorageMetrics{
		blobDownloadedSize: scope.SubScope(blobDownloaderScopeName).Timer(blobSizeMetricName),
		blobUploadedSize:   scope.SubScope(blobUploaderScopeName).Timer(blobSizeMetricName),
	}
}

func (s *blobStorageImpl) getObjectKey(blockchain common.Blockchain, sidechain api.SideChain, network common.Network, metadata *api.BlockMetadata, compression api.Compression) (string, error) {
	var key string
	var err error
	blockchainNetwork := fmt.Sprintf("%s/%s", blockchain, network)
	tagHeightHash := fmt.Sprintf("%d/%d/%s", metadata.Tag, metadata.Height, metadata.Hash)
	if s.config.Chain.Sidechain != api.SideChain_SIDECHAIN_NONE {
		key = fmt.Sprintf(
			"%s/%s/%s", blockchainNetwork, sidechain, tagHeightHash,
		)
	} else {
		key = fmt.Sprintf(
			"%s/%s", blockchainNetwork, tagHeightHash,
		)
	}
	key, err = storage_utils.GetObjectKey(key, compression)
	if err != nil {
		return "", xerrors.Errorf("failed to get object key: %w", err)
	}
	return key, nil
}

func (s *blobStorageImpl) uploadRaw(ctx context.Context, rawBlockData *internal.RawBlockData) (string, error) {
	key, err := s.getObjectKey(rawBlockData.Blockchain, rawBlockData.SideChain, rawBlockData.Network, rawBlockData.BlockMetadata, rawBlockData.BlockDataCompression)
	if err != nil {
		return "", err
	}

	// #nosec G401
	h := md5.New()
	size, err := h.Write(rawBlockData.BlockData)
	if err != nil {
		return "", xerrors.Errorf("failed to compute checksum: %w", err)
	}

	checksum := base64.StdEncoding.EncodeToString(h.Sum(nil))

	if _, err := s.uploader.Upload(ctx, &awss3.PutObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(key),
		Body:       bytes.NewReader(rawBlockData.BlockData),
		ContentMD5: aws.String(checksum),
		ACL:        awss3types.ObjectCannedACLBucketOwnerFullControl,
	}); err != nil {
		return "", xerrors.Errorf("failed to upload to s3: %w", err)
	}

	// a workaround to use timer
	s.blobStorageMetrics.blobUploadedSize.Record(time.Duration(size) * time.Millisecond)

	return key, nil
}

func (s *blobStorageImpl) UploadRaw(ctx context.Context, rawBlockData *internal.RawBlockData) (string, error) {
	return s.instrumentUploadRaw.Instrument(ctx, func(ctx context.Context) (string, error) {
		defer s.logDuration("upload", time.Now())

		// Skip the upload if the block itself is skipped.
		if rawBlockData.BlockMetadata.Skipped {
			return "", nil
		}
		return s.uploadRaw(ctx, rawBlockData)
	})
}

func (s *blobStorageImpl) Upload(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
	return s.instrumentUpload.Instrument(ctx, func(ctx context.Context) (string, error) {
		defer s.logDuration("upload", time.Now())

		// Skip the upload if the block itself is skipped.
		if block.Metadata.Skipped {
			return "", nil
		}

		data, err := proto.Marshal(block)
		if err != nil {
			return "", xerrors.Errorf("failed to marshal block: %w", err)
		}

		data, err = storage_utils.Compress(data, compression)
		if err != nil {
			return "", xerrors.Errorf("failed to compress data with type %v: %w", compression.String(), err)
		}
		return s.uploadRaw(ctx, &internal.RawBlockData{
			Blockchain:           block.Blockchain,
			SideChain:            block.SideChain,
			Network:              block.Network,
			BlockMetadata:        block.Metadata,
			BlockData:            data,
			BlockDataCompression: compression,
		})
	})
}

func (s *blobStorageImpl) UploadConsolidated(ctx context.Context, blocks []internal.ConsolidatedBlockPayload) (string, []internal.BlockPlacement, error) {
	defer s.logDuration("upload_consolidated", time.Now())

	consolidation := s.config.AWS.Storage.Consolidation
	object, err := cscb.Encode(ctx, cscb.EncodeConfig{
		Blockchain:                s.config.Chain.Blockchain,
		Network:                   s.config.Chain.Network,
		SideChain:                 s.config.Chain.Sidechain,
		Codec:                     consolidation.Codec,
		CodecLevel:                consolidation.CodecLevel,
		ZstdLongDistanceWindowLog: consolidation.ZstdLongDistanceWindowLog,
		MaxBlocks:                 consolidation.MaxBlocks,
		CompressionChunkBlocks:    consolidation.CompressionChunkBlocks,
		MaxChunkCompressedBytes:   consolidation.MaxChunkCompressedBytes,
		MaxChunkUncompressedBytes: consolidation.MaxChunkUncompressedBytes,
		MaxCompressedBytes:        consolidation.MaxCompressedBytes,
		MaxUncompressedBytes:      consolidation.MaxUncompressedBytes,
		ShardSize:                 consolidation.ShardSize,
		MemoryBudgetBytes:         consolidation.MemoryBudgetBytes,
		LocalSpillDir:             consolidation.LocalSpillDir,
		LocalSpillMaxBytes:        consolidation.LocalSpillMaxBytes,
	}, blocks)
	if err != nil {
		return "", nil, err
	}
	defer func() { _ = object.Close() }()

	found, err := s.validateExistingConsolidatedObject(ctx, object)
	if err != nil {
		return "", nil, err
	}
	if !found {
		uploaded, err := s.uploadConsolidatedObject(ctx, object, consolidation.Codec)
		if err != nil {
			return "", nil, err
		}
		if uploaded {
			if err := s.validateUploadedConsolidatedObject(ctx, object); err != nil {
				return "", nil, err
			}
			s.blobStorageMetrics.blobUploadedSize.Record(time.Duration(object.Length) * time.Millisecond)
		}
	}
	return object.Key, object.Placements, nil
}

func (s *blobStorageImpl) uploadConsolidatedObject(ctx context.Context, object *cscb.Object, compression api.Compression) (bool, error) {
	if object.Length > maxSinglePutObjectSize {
		return false, xerrors.Errorf("CSCB object length %d exceeds v1 single-put limit %d", object.Length, maxSinglePutObjectSize)
	}
	reader, err := object.Open()
	if err != nil {
		return false, err
	}
	defer func() { _ = reader.Close() }()

	contentMD5, err := computeContentMD5(object)
	if err != nil {
		return false, err
	}
	metadata := map[string]string{
		cscbMetadataFormat:             "cscb",
		cscbMetadataCompressionScope:   "batch-chunked",
		cscbMetadataSHA256:             object.SHA256,
		cscbMetadataCodec:              codecName(compression),
		cscbMetadataUncompressedLength: fmt.Sprintf("%d", object.PayloadUncompressedLength),
	}
	if _, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(object.Key),
		Body:        reader,
		ContentMD5:  aws.String(contentMD5),
		ACL:         awss3types.ObjectCannedACLBucketOwnerFullControl,
		IfNoneMatch: aws.String("*"),
		Metadata:    metadata,
	}); err != nil {
		if isObjectPreconditionFailed(err) {
			found, validateErr := s.validateExistingConsolidatedObject(ctx, object)
			if validateErr != nil {
				return false, validateErr
			}
			if found {
				return false, nil
			}
		}
		return false, xerrors.Errorf("failed to upload CSCB object to s3: %w", err)
	}
	return true, nil
}

func computeContentMD5(object *cscb.Object) (string, error) {
	reader, err := object.Open()
	if err != nil {
		return "", err
	}
	defer func() { _ = reader.Close() }()

	// #nosec G401 -- S3 Content-MD5 is a transport integrity header, not a cryptographic trust boundary.
	h := md5.New()
	if _, err := io.Copy(h, reader); err != nil {
		return "", xerrors.Errorf("failed to compute CSCB content md5: %w", err)
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

func (s *blobStorageImpl) validateExistingConsolidatedObject(ctx context.Context, object *cscb.Object) (bool, error) {
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(object.Key),
	})
	if err != nil {
		if isObjectNotFound(err) {
			return false, nil
		}
		return false, xerrors.Errorf("failed to head CSCB object (bucket=%s, key=%s): %w", s.bucket, object.Key, err)
	}
	if err := validateConsolidatedHead(out, object); err != nil {
		return false, err
	}
	return true, nil
}

func (s *blobStorageImpl) validateUploadedConsolidatedObject(ctx context.Context, object *cscb.Object) error {
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(object.Key),
	})
	if err != nil {
		return xerrors.Errorf("failed to validate uploaded CSCB object (bucket=%s, key=%s): %w", s.bucket, object.Key, err)
	}
	return validateConsolidatedHead(out, object)
}

func validateConsolidatedHead(out *awss3.HeadObjectOutput, object *cscb.Object) error {
	if out == nil {
		return xerrors.New("empty CSCB HeadObject response")
	}
	if out.ContentLength == nil {
		return xerrors.Errorf("CSCB object %s has no content length", object.Key)
	}
	if uint64(*out.ContentLength) != object.Length {
		return xerrors.Errorf("CSCB object %s length mismatch: got %d want %d", object.Key, *out.ContentLength, object.Length)
	}
	if metadataSHA := out.Metadata[cscbMetadataSHA256]; metadataSHA != object.SHA256 {
		return xerrors.Errorf("CSCB object %s sha256 metadata mismatch: got %q want %q", object.Key, metadataSHA, object.SHA256)
	}
	return nil
}

func isObjectNotFound(err error) bool {
	var notFound *awss3types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var noSuchKey *awss3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return true
		}
	}
	return false
}

func isObjectPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "412":
			return true
		}
	}
	return false
}

func codecName(compression api.Compression) string {
	switch compression {
	case api.Compression_GZIP:
		return "gzip"
	case api.Compression_ZSTD:
		return "zstd"
	default:
		return "unknown"
	}
}

func (s *blobStorageImpl) Download(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
	return s.instrumentDownload.Instrument(ctx, func(ctx context.Context) (*api.Block, error) {
		defer s.logDuration("download", time.Now())

		if metadata.Skipped {
			return s.skippedBlock(metadata), nil
		}
		if isLegacyBlockObject(metadata) {
			return s.downloadLegacy(ctx, metadata)
		}
		return s.downloadCSCBBlock(ctx, metadata)
	})
}

func (s *blobStorageImpl) DownloadMany(ctx context.Context, metadatas []*api.BlockMetadata) ([]*api.Block, error) {
	return s.instrumentDownloadMany.Instrument(ctx, func(ctx context.Context) ([]*api.Block, error) {
		defer s.logDuration("download_many", time.Now())

		result := make([]*api.Block, len(metadatas))
		legacyRefs := make([]downloadRef, 0, len(metadatas))
		cscbRefsByObject := make(map[string][]downloadRef)
		for i, metadata := range metadatas {
			if metadata.GetSkipped() {
				result[i] = s.skippedBlock(metadata)
				continue
			}
			ref := downloadRef{
				index:    i,
				metadata: metadata,
			}
			if isLegacyBlockObject(metadata) {
				legacyRefs = append(legacyRefs, ref)
				continue
			}
			key := metadata.GetObjectKeyMain()
			if key == "" {
				return nil, xerrors.Errorf("missing CSCB object key for height %d", metadata.GetHeight())
			}
			cscbRefsByObject[key] = append(cscbRefsByObject[key], ref)
		}

		group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(s.downloadWorkerLimit()))
		for _, ref := range legacyRefs {
			ref := ref
			group.Go(func() error {
				block, err := s.Download(ctx, ref.metadata)
				if err != nil {
					return xerrors.Errorf("failed to download legacy block (input={%+v}): %w", ref.metadata, err)
				}
				result[ref.index] = block
				return nil
			})
		}
		for key, refs := range cscbRefsByObject {
			key, refs := key, refs
			group.Go(func() error {
				if err := s.downloadCSCBObject(ctx, key, refs, result); err != nil {
					return xerrors.Errorf("failed to download CSCB object %s: %w", key, err)
				}
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return nil, err
		}
		return result, nil
	})
}

func (s *blobStorageImpl) skippedBlock(metadata *api.BlockMetadata) *api.Block {
	return &api.Block{
		Blockchain: s.config.Chain.Blockchain,
		Network:    s.config.Chain.Network,
		SideChain:  s.config.Chain.Sidechain,
		Metadata:   metadata,
		Blobdata:   nil,
	}
}

func isLegacyBlockObject(metadata *api.BlockMetadata) bool {
	return metadata.GetObjectFormat() != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH || metadata.GetByteLength() == 0
}

func (s *blobStorageImpl) downloadWorkerLimit() int {
	limit := int(s.config.Api.NumWorkers)
	if limit <= 0 {
		return 1
	}
	return limit
}

func (s *blobStorageImpl) downloadLegacy(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
	key := metadata.ObjectKeyMain
	buf := manager.NewWriteAtBuffer([]byte{})

	size, err := s.downloader.Download(ctx, buf, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, storageerrors.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to download from s3 (bucket=%s, key=%s): %w", s.bucket, key, err)
	}

	// a workaround to use timer
	s.blobStorageMetrics.blobDownloadedSize.Record(time.Duration(size) * time.Millisecond)

	compression := storage_utils.GetCompressionType(key)
	blockData, err := storage_utils.Decompress(buf.Bytes(), compression)
	if err != nil {
		return nil, xerrors.Errorf("failed to decompress block data with type %v: %w", compression.String(), err)
	}

	return unmarshalBlockData(s.bucket, key, metadata, blockData)
}

func (s *blobStorageImpl) downloadCSCBBlock(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
	result := make([]*api.Block, 1)
	ref := downloadRef{
		index:    0,
		metadata: metadata,
	}
	if err := s.downloadCSCBObject(ctx, metadata.GetObjectKeyMain(), []downloadRef{ref}, result); err != nil {
		return nil, err
	}
	return result[0], nil
}

func (s *blobStorageImpl) downloadCSCBObject(ctx context.Context, key string, refs []downloadRef, result []*api.Block) error {
	index, err := s.readCSCBIndex(ctx, key)
	if err != nil {
		return err
	}

	downloadsByChunk := make(map[uint32][]cscbBlockDownload)
	for _, ref := range refs {
		block, chunk, err := index.LookupBlock(ref.metadata)
		if err != nil {
			return err
		}
		downloadsByChunk[chunk.Index] = append(downloadsByChunk[chunk.Index], cscbBlockDownload{
			ref:   ref,
			block: block,
			chunk: chunk,
		})
	}

	chunkIndexes := make([]int, 0, len(downloadsByChunk))
	for chunkIndex := range downloadsByChunk {
		chunkIndexes = append(chunkIndexes, int(chunkIndex))
	}
	sort.Ints(chunkIndexes)

	for _, chunkIndex := range chunkIndexes {
		downloads := downloadsByChunk[uint32(chunkIndex)]
		chunk := downloads[0].chunk
		blocks := make([]*cscb.BlockDescriptor, len(downloads))
		for i, download := range downloads {
			blocks[i] = download.block
		}
		blockPayloads, err := s.downloadCSCBChunkPayloads(ctx, key, index.Header.Codec, chunk, blocks)
		if err != nil {
			return err
		}
		for i, download := range downloads {
			blockPayload := blockPayloads[i]
			block, err := unmarshalBlockData(s.bucket, key, download.ref.metadata, blockPayload)
			if err != nil {
				return err
			}
			result[download.ref.index] = block
		}
	}
	return nil
}

func (s *blobStorageImpl) readCSCBIndex(ctx context.Context, key string) (*cscb.Index, error) {
	first, err := s.readObjectRange(ctx, key, 0, cscbInitialIndexReadSize-1)
	if err != nil {
		return nil, err
	}
	required, err := cscb.HeaderEnvelopeLength(first)
	if err != nil {
		return nil, err
	}
	if required <= uint64(len(first)) {
		return cscb.ParseIndex(first)
	}
	remaining, err := s.readObjectRange(ctx, key, uint64(len(first)), required-1)
	if err != nil {
		return nil, err
	}
	indexData := make([]byte, 0, required)
	indexData = append(indexData, first...)
	indexData = append(indexData, remaining...)
	return cscb.ParseIndex(indexData)
}

func (s *blobStorageImpl) downloadCSCBChunkPayloads(ctx context.Context, key string, codec api.Compression, chunk *cscb.ChunkDescriptor, blocks []*cscb.BlockDescriptor) ([][]byte, error) {
	compressed, err := s.readObjectRangeByLength(ctx, key, chunk.CompressedPayloadOffset, chunk.CompressedLength)
	if err != nil {
		return nil, err
	}
	if uint64(len(compressed)) != chunk.CompressedLength {
		return nil, xerrors.Errorf("CSCB compressed chunk length mismatch: got %d want %d", len(compressed), chunk.CompressedLength)
	}
	return cscb.ExtractBlockPayloadsFromChunkFrame(bytes.NewReader(compressed), codec, chunk, blocks)
}

func (s *blobStorageImpl) readObjectRangeByLength(ctx context.Context, key string, offset uint64, length uint64) ([]byte, error) {
	if length == 0 {
		return nil, xerrors.Errorf("empty range read for key %s at offset %d", key, offset)
	}
	end, err := inclusiveRangeEnd(offset, length)
	if err != nil {
		return nil, err
	}
	return s.readObjectRange(ctx, key, offset, end)
}

func (s *blobStorageImpl) readObjectRange(ctx context.Context, key string, start uint64, end uint64) ([]byte, error) {
	if end < start {
		return nil, xerrors.Errorf("invalid range for key %s: start=%d end=%d", key, start, end)
	}
	output, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, storageerrors.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to range download from s3 (bucket=%s, key=%s, range=bytes=%d-%d): %w", s.bucket, key, start, end, err)
	}
	if output.Body == nil {
		return nil, xerrors.Errorf("empty s3 body (bucket=%s, key=%s, range=bytes=%d-%d)", s.bucket, key, start, end)
	}
	defer func() { _ = output.Body.Close() }()
	expectedLength := end - start + 1
	data, err := readExpectedRangeBody(output.Body, expectedLength, start == 0)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, storageerrors.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to read s3 range body (bucket=%s, key=%s, range=bytes=%d-%d): %w", s.bucket, key, start, end, err)
	}
	s.blobStorageMetrics.blobDownloadedSize.Record(time.Duration(len(data)) * time.Millisecond)
	return data, nil
}

func readExpectedRangeBody(body io.Reader, expectedLength uint64, allowShort bool) ([]byte, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if expectedLength >= maxInt64 {
		return nil, xerrors.Errorf("range body length %d is too large", expectedLength)
	}
	data, err := io.ReadAll(io.LimitReader(body, int64(expectedLength)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) > expectedLength || (!allowShort && uint64(len(data)) != expectedLength) {
		return nil, xerrors.Errorf("range body length mismatch: got %d want %d", len(data), expectedLength)
	}
	return data, nil
}

func inclusiveRangeEnd(offset uint64, length uint64) (uint64, error) {
	if length == 0 {
		return 0, xerrors.New("range length must be positive")
	}
	if offset > ^uint64(0)-(length-1) {
		return 0, xerrors.Errorf("range overflow: offset=%d length=%d", offset, length)
	}
	return offset + length - 1, nil
}

func unmarshalBlockData(bucket string, key string, metadata *api.BlockMetadata, blockData []byte) (*api.Block, error) {
	var block api.Block
	err := proto.Unmarshal(blockData, &block)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal data downloaded from s3 bucket %s key %s: %w", bucket, key, err)
	}

	// When metadata is loaded from meta storage,
	// the new fields, e.g. ParentHeight, may be populated with default values.
	// Overwrite metadata using the one loaded from meta storage.
	block.Metadata = metadata
	return &block, nil
}

func (s *blobStorageImpl) PreSign(ctx context.Context, objectKey string) (string, error) {
	presignClient := awss3.NewPresignClient(s.client.(*awss3.Client))
	presignResult, err := presignClient.PresignGetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.config.AWS.Bucket),
		Key:    aws.String(objectKey),
	}, awss3.WithPresignExpires(s.config.AWS.PresignedUrlExpiration))
	if err != nil {
		return "", xerrors.Errorf("failed to generate presigned url: %w", err)
	}
	return presignResult.URL, nil
}

func (s *blobStorageImpl) logDuration(method string, start time.Time) {
	s.logger.Debug(
		"blob_storage",
		zap.String("storage_type", "s3"),
		zap.String("method", method),
		zap.Duration("duration", time.Since(start)),
	)
}
