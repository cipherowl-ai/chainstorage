package s3

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501
	"encoding/base64"
	"errors"
	"fmt"
	"io"
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
		logger              *zap.Logger
		config              *config.Config
		bucket              string
		client              s3.Client
		downloader          s3.Downloader
		uploader            s3.Uploader
		blobStorageMetrics  *blobStorageMetrics
		instrumentUpload    instrument.InstrumentWithResult[string]
		instrumentUploadRaw instrument.InstrumentWithResult[string]
		instrumentDownload  instrument.InstrumentWithResult[*api.Block]
	}

	blobStorageMetrics struct {
		blobDownloadedSize tally.Timer
		blobUploadedSize   tally.Timer
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
		logger:              log.WithPackage(params.Logger),
		config:              params.Config,
		bucket:              params.Config.AWS.Bucket,
		client:              params.Client,
		downloader:          params.Downloader,
		uploader:            params.Uploader,
		blobStorageMetrics:  newBlobStorageMetrics(metrics),
		instrumentUpload:    instrument.NewWithResult[string](metrics, "upload"),
		instrumentUploadRaw: instrument.NewWithResult[string](metrics, "upload_raw"),
		instrumentDownload:  instrument.NewWithResult[*api.Block](metrics, "download"),
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

func (s *blobStorageImpl) UploadConsolidated(ctx context.Context, blocks []internal.ConsolidatedBlockPayload, compression api.Compression) (string, []internal.BlockPlacement, error) {
	defer s.logDuration("upload_consolidated", time.Now())

	consolidation := s.config.AWS.Storage.Consolidation
	object, err := cscb.Encode(ctx, cscb.EncodeConfig{
		Blockchain:                s.config.Chain.Blockchain,
		Network:                   s.config.Chain.Network,
		SideChain:                 s.config.Chain.Sidechain,
		Codec:                     compression,
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
		if err := s.uploadConsolidatedObject(ctx, object, compression); err != nil {
			return "", nil, err
		}
		if err := s.validateUploadedConsolidatedObject(ctx, object); err != nil {
			return "", nil, err
		}
		s.blobStorageMetrics.blobUploadedSize.Record(time.Duration(object.Length) * time.Millisecond)
	}
	return object.Key, object.Placements, nil
}

func (s *blobStorageImpl) uploadConsolidatedObject(ctx context.Context, object *cscb.Object, compression api.Compression) error {
	if object.Length > maxSinglePutObjectSize {
		return xerrors.Errorf("CSCB object length %d exceeds v1 single-put limit %d", object.Length, maxSinglePutObjectSize)
	}
	reader, err := object.Open()
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	contentMD5, err := computeContentMD5(object)
	if err != nil {
		return err
	}
	metadata := map[string]string{
		cscbMetadataFormat:             "cscb",
		cscbMetadataCompressionScope:   "batch-chunked",
		cscbMetadataSHA256:             object.SHA256,
		cscbMetadataCodec:              codecName(compression),
		cscbMetadataUncompressedLength: fmt.Sprintf("%d", object.PayloadUncompressedLength),
	}
	if _, err := s.uploader.Upload(ctx, &awss3.PutObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(object.Key),
		Body:       reader,
		ContentMD5: aws.String(contentMD5),
		ACL:        awss3types.ObjectCannedACLBucketOwnerFullControl,
		Metadata:   metadata,
	}, singlePutUploadOption(object.Length)); err != nil {
		return xerrors.Errorf("failed to upload CSCB object to s3: %w", err)
	}
	return nil
}

func singlePutUploadOption(objectLength uint64) func(*manager.Uploader) {
	return func(u *manager.Uploader) {
		partSize := int64(objectLength + 1)
		if partSize < manager.MinUploadPartSize {
			partSize = manager.MinUploadPartSize
		}
		u.PartSize = partSize
	}
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
			// No blob data is available when the block is skipped.
			return &api.Block{
				Blockchain: s.config.Chain.Blockchain,
				Network:    s.config.Chain.Network,
				SideChain:  s.config.Chain.Sidechain,
				Metadata:   metadata,
				Blobdata:   nil,
			}, nil
		}

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

		var block api.Block
		err = proto.Unmarshal(blockData, &block)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal data downloaded from s3 bucket %s key %s: %w", s.bucket, key, err)
		}

		// When metadata is loaded from meta storage,
		// the new fields, e.g. ParentHeight, may be populated with default values.
		// Overwrite metadata using the one loaded from meta storage.
		block.Metadata = metadata
		return &block, nil
	})
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
