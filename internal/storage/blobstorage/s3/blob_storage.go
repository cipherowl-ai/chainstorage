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
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
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
		buf := &writeAtBuffer{}

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

// writeAtBuffer is a simple io.WriterAt implementation that wraps a byte slice.
type writeAtBuffer struct {
	buf []byte
}

func (w *writeAtBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, io.ErrShortWrite
	}
	end := int(off) + len(p)
	if end > len(w.buf) {
		newBuf := make([]byte, end)
		copy(newBuf, w.buf)
		w.buf = newBuf
	}
	copy(w.buf[off:], p)
	return len(p), nil
}

func (w *writeAtBuffer) Bytes() []byte {
	return w.buf
}
