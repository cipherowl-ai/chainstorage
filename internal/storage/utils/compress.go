package utils

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io/ioutil"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	GzipFileSuffix = ".gzip"
	ZstdFileSuffix = ".zstd"
)

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

func GetCompressionType(fileURL string) api.Compression {
	ext := filepath.Ext(fileURL)
	switch ext {
	case GzipFileSuffix:
		return api.Compression_GZIP
	case ZstdFileSuffix:
		return api.Compression_ZSTD
	}
	return api.Compression_NONE
}

func CompressorFactory(compressionType api.Compression) (Compressor, error) {
	switch compressionType {
	case api.Compression_GZIP:
		return &GzipCompressor{}, nil
	case api.Compression_ZSTD:
		return &ZstdCompressor{}, nil
	default:
		return nil, errors.New("unsupported compression type")
	}
}

// ------ GZIP ------
type GzipCompressor struct{}

func (c *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	if _, err := writer.Write(data); err != nil {
		return nil, xerrors.Errorf("failed to write compressed data with gzip: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, xerrors.Errorf("failed to close gzip writer: %w", err)
	}

	return buf.Bytes(), nil
}

func (c *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, xerrors.Errorf("failed to initiate gzip reader: %w", err)
	}
	decoded, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, xerrors.Errorf("failed to read data: %w", err)
	}
	if err := reader.Close(); err != nil {
		return nil, xerrors.Errorf("failed to close gzip reader: %w", err)
	}
	return decoded, nil
}

// ------ ZSTD ------
type ZstdCompressor struct{}

func (c *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	writer, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, xerrors.Errorf("failed to write compressed data with zstd: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, xerrors.Errorf("failed to close zstd writer: %w", err)
	}
	return writer.EncodeAll(data, nil), nil
}

func (c *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to initiate zstd reader: %w", err)
	}
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to read data with zstd: %w", err)
	}
	return decoded, nil
}
