package compress

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	GzipCompressorParams struct {
		fx.In
		fxparams.Params
	}

	gzipCompressorFactory struct {
		params GzipCompressorParams
	}

	GzipCompressor struct{}
)

func NewGzipCompressorFactory(params GzipCompressorParams) CompressorFactory {
	return &gzipCompressorFactory{params}
}

func (f *gzipCompressorFactory) Create() (Compressor, error) {
	return NewGzipCompressor(), nil
}

func NewGzipCompressor() Compressor {
	return &GzipCompressor{}
}

func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
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

func (g *GzipCompressor) Decompress(data []byte) ([]byte, error) {
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

func (g *GzipCompressor) GetObjectKey(key string) string {
	return fmt.Sprintf("%s%s", key, GzipFileSuffix)
}
