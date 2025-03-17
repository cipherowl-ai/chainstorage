package compress

import (
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	GzipFileSuffix = ".gzip"
	ZstdFileSuffix = ".zstd"
)

type (
	CompressProxy struct {
		compressorMap     map[api.Compression]Compressor
		compressType      api.Compression
		currentCompressor Compressor
	}

	Compressor interface {
		Compress(data []byte) ([]byte, error)
		Decompress(data []byte) ([]byte, error)
		GetObjectKey(key string) string
	}

	CompressorFactory interface {
		Create() (Compressor, error)
	}

	CompressorFactoryParams struct {
		fx.In
		fxparams.Params

		GZIP CompressorFactory `name:"gzip"`
		ZSTD CompressorFactory `name:"zstd"`
	}
)

func NewCompressProxy(config *config.Config, s3Uploader s3.Uploader, s3Downloader s3.Downloader) (*CompressProxy, error) {
	currentCompressType := config.AWS.Storage.DataCompression
	zstdCompressor, err := NewZstdCompressor(config, s3Uploader, s3Downloader)
	if err != nil {
		return nil, xerrors.Errorf("failed to initiate zstd reader: %w", err)
	}
	gzipCompressor := NewGzipCompressor()
	var currentCompressor Compressor
	switch currentCompressType {
	case api.Compression_GZIP:
		currentCompressor = gzipCompressor
	case api.Compression_ZSTD:
		currentCompressor = zstdCompressor
	}
	if currentCompressType == api.Compression_GZIP {
		currentCompressor = gzipCompressor
	}
	compressorMap := map[api.Compression]Compressor{
		api.Compression_ZSTD: zstdCompressor,
		api.Compression_GZIP: gzipCompressor,
	}
	return &CompressProxy{
		compressType:      currentCompressType,
		currentCompressor: currentCompressor,
		compressorMap:     compressorMap,
	}, nil
}

func (c CompressProxy) Compress(data []byte) ([]byte, error) {
	return c.currentCompressor.Compress(data)
}

func (c CompressProxy) Decompress(data []byte, compression api.Compression) ([]byte, error) {
	compressor, exists := c.compressorMap[compression]
	if !exists {
		return nil, xerrors.Errorf("unsupported compression type: %v", compression)
	}
	return compressor.Decompress(data)
}

func (c CompressProxy) GetObjectKey(key string, compression api.Compression) (string, error) {
	compressor, exists := c.compressorMap[compression]
	if !exists {
		return "", xerrors.Errorf("failed to Get Object Key with unsupport compress")
	}
	return compressor.GetObjectKey(key), nil
}
