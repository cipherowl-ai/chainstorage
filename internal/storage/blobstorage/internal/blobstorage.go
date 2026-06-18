package internal

import (
	"bytes"
	"context"
	"io"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	RawBlockData struct {
		Blockchain           common.Blockchain
		SideChain            api.SideChain
		Network              common.Network
		BlockMetadata        *api.BlockMetadata
		BlockData            []byte
		BlockDataCompression api.Compression
	}

	PayloadSource interface {
		Open(ctx context.Context) (io.ReadCloser, error)
		Length() uint64
	}

	BytesPayloadSource []byte

	ConsolidatedBlockPayload struct {
		Metadata           *api.BlockMetadata
		MetadataID         int64
		RawBlockPayload    PayloadSource
		UncompressedLength uint64
	}

	BlockPlacement struct {
		MetadataID         int64
		Height             uint64
		Hash               string
		ObjectKey          string
		ObjectFormat       api.BlockObjectFormat
		ByteOffset         uint64
		ByteLength         uint64
		UncompressedLength uint64
	}

	BlobStorage interface {
		Upload(ctx context.Context, block *api.Block, compression api.Compression) (string, error)
		UploadRaw(ctx context.Context, rawBlockData *RawBlockData) (string, error)
		UploadConsolidated(ctx context.Context, blocks []ConsolidatedBlockPayload) (string, []BlockPlacement, error)
		Download(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error)
		DownloadMany(ctx context.Context, metadata []*api.BlockMetadata) ([]*api.Block, error)
		PreSign(ctx context.Context, objectKey string) (string, error)
	}

	BlobStorageFactory interface {
		Create() (BlobStorage, error)
	}

	BlobStorageFactoryParams struct {
		fx.In
		fxparams.Params
		S3  BlobStorageFactory `name:"blobstorage/s3"`
		GCS BlobStorageFactory `name:"blobstorage/gcs"`
	}
)

func (s BytesPayloadSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s)), nil
}

func (s BytesPayloadSource) Length() uint64 {
	return uint64(len(s))
}

func WithBlobStorageFactory(params BlobStorageFactoryParams) (BlobStorage, error) {
	var factory BlobStorageFactory
	storageType := params.Config.StorageType.BlobStorageType
	switch storageType {
	case config.BlobStorageType_UNSPECIFIED, config.BlobStorageType_S3:
		factory = params.S3
	case config.BlobStorageType_GCS:
		factory = params.GCS
	}
	if factory == nil {
		return nil, xerrors.Errorf("blob storage type is not implemented: %v", storageType)
	}
	result, err := factory.Create()
	if err != nil {
		return nil, xerrors.Errorf("failed to create blob storage of type %v: %w", storageType, err)
	}
	return result, nil
}
