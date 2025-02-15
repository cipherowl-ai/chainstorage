package utils

import (
	"fmt"

	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func Compress(data []byte, compression api.Compression) ([]byte, error) {
	if compression == api.Compression_NONE {
		return data, nil
	}

	compressor, err := CompressorFactory(compression)
	if err != nil {
		return nil, err
	}
	coded, err := compressor.Compress(data)
	if err != nil {
		return nil, err
	}
	return coded, nil
}

func Decompress(data []byte, compression api.Compression) ([]byte, error) {
	if compression == api.Compression_NONE {
		return data, nil
	}
	compressor, err := CompressorFactory(compression)
	if err != nil {
		return nil, err
	}
	decoded, err := compressor.Decompress(data)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func GetObjectKey(key string, compression api.Compression) (string, error) {

	switch compression {
	case api.Compression_NONE:
		return key, nil
	case api.Compression_GZIP:
		return fmt.Sprintf("%s%s", key, GzipFileSuffix), nil
	case api.Compression_ZSTD:
		return fmt.Sprintf("%s%s", key, ZstdFileSuffix), nil
	default:
		return "", xerrors.Errorf("failed to get object key with unsupported type %v", compression.String())
	}
}
