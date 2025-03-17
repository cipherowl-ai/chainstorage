package compress

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	ZstdCompressorParams struct {
		fx.In
		fxparams.Params
		DictVersion string        `optional:"true"`
		Downloader  s3.Downloader `optional:"true"`
		Uploader    s3.Uploader   `optional:"true"`
	}

	zstdCompressorFactory struct {
		params ZstdCompressorParams
	}

	ZstdCompressor struct {
		dictCache         map[string][]byte
		versionDecoderMap map[string]*zstd.Decoder // only decompression use other version

		versionHash          string
		currentVersionHeader []byte
		// alway use configured verison dictionary for compression
		// it can be with dictionary of configured version or without dictionary as default
		currentEncoder *zstd.Encoder
		defaultDecoder *zstd.Decoder // default decoder without dictionary

		dictBucket     string
		dictS3Path     string
		dictDownloader s3.Downloader
		dictUploader   s3.Uploader

		mu sync.RWMutex
	}
)

const (
	dictVersionHeaderMarker = 0x01
	dictVersionLength       = 32
	dictVersionHeaderSize   = 1 + dictVersionLength
)

func NewZstdCompressorFactory(params ZstdCompressorParams) CompressorFactory {
	return &zstdCompressorFactory{params}
}

func (f *zstdCompressorFactory) Create() (Compressor, error) {
	return NewZstdCompressorWithParams(f.params)
}

func LoadZstdDict(dictDir, version string) ([]byte, error) {
	if version == "" {
		return nil, nil
	}
	dictPath := fmt.Sprintf("%v/%v", dictDir, version)
	dictData, err := os.ReadFile(dictPath)
	if err != nil {
		return nil, xerrors.Errorf("failed to read zstd dictionary file %v: %w", dictPath, err)
	}

	return dictData, nil
}

func NewZstdCompressorWithParams(params ZstdCompressorParams) (*ZstdCompressor, error) {
	dictUploader := params.Uploader
	dictDownloader := params.Downloader
	return NewZstdCompressor(params.Config, dictUploader, dictDownloader)
}

func NewZstdCompressor(config *config.Config, dictUploader s3.Uploader, dictDownloader s3.Downloader) (*ZstdCompressor, error) {
	defaultDecoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to initiate zstd reader: %w", err)
	}
	blockchainName := config.Chain.Blockchain.GetName()
	networkName := strings.TrimPrefix(config.Chain.Network.GetName(), blockchainName+"-")
	dictS3Path := fmt.Sprintf("%s/%s", blockchainName, networkName)

	z := &ZstdCompressor{
		dictCache:         make(map[string][]byte),
		versionDecoderMap: make(map[string]*zstd.Decoder),
		defaultDecoder:    defaultDecoder,
		dictBucket:        config.AWS.Bucket,
		dictS3Path:        dictS3Path,
		dictUploader:      dictUploader,
		dictDownloader:    dictDownloader,
	}

	var eopts []zstd.EOption = []zstd.EOption{
		zstd.WithEncoderLevel(zstd.SpeedDefault),
	}
	dictVersion := config.AWS.Storage.ZstdDictVersion
	if dictVersion != "" {
		dictData, err := LoadZstdDict(config.GetConfigDir(), dictVersion)
		if err != nil {
			return nil, xerrors.Errorf("failed to upload dictionary: %w", err)
		}
		versionHash, err := z.UploadDict(dictS3Path, dictData)
		if err != nil {
			return nil, xerrors.Errorf("failed to upload dictionary: %w", err)
		}
		z.versionHash = versionHash
		// precompute version header data
		header := make([]byte, dictVersionHeaderSize)
		header[0] = dictVersionHeaderMarker
		copy(header[1:], []byte(versionHash))
		z.currentVersionHeader = header

		eopts = append(eopts, zstd.WithEncoderDict(dictData))
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dictData))
		if err != nil {
			return nil, xerrors.Errorf("failed to initiate zstd reader: %w", err)
		}
		z.versionDecoderMap[versionHash] = decoder
		z.dictCache[versionHash] = dictData
	}
	encoder, err := zstd.NewWriter(nil, eopts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to write compressed data with zstd: %w", err)
	}
	z.currentEncoder = encoder

	return z, nil
}

func (z *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	compressed := z.currentEncoder.EncodeAll(data, nil)
	return z.setDictVersionHashForData(compressed), nil
}

func (z *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	versionHash, payload := z.getVersionHashFromData(data)

	var decoder *zstd.Decoder
	if versionHash != "" {
		versioDecoder, err := z.GetVersionDecoder(versionHash)
		if err != nil {
			return nil, xerrors.Errorf("failed to get decoder: %w", err)
		}
		decoder = versioDecoder
	} else {
		decoder = z.defaultDecoder
	}
	decoded, err := decoder.DecodeAll(payload, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to read data with zstd: %w", err)
	}
	return decoded, nil
}

func (z *ZstdCompressor) GetObjectKey(key string) string {
	return fmt.Sprintf("%s%s", key, ZstdFileSuffix)
}

func (z *ZstdCompressor) calculateDictHash(data []byte) string {
	hasher := md5.New()
	hasher.Write(data)
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (z *ZstdCompressor) setDictVersionHashForData(data []byte) []byte {
	if z.versionHash == "" {
		return data
	}
	result := make([]byte, dictVersionHeaderSize+len(data))
	copy(result, z.currentVersionHeader)
	copy(result[dictVersionHeaderSize:], data)
	return result
}

func (z *ZstdCompressor) getVersionHashFromData(data []byte) (string, []byte) {
	if data[0] != dictVersionHeaderMarker {
		return "", data
	}
	versionHash := string(data[1:dictVersionHeaderSize])
	return versionHash, data[dictVersionHeaderSize:]
}

func (z *ZstdCompressor) GetVersionDecoder(versionHash string) (*zstd.Decoder, error) {
	z.mu.RLock()
	decoder, exists := z.versionDecoderMap[versionHash]
	z.mu.RUnlock()
	if exists {
		return decoder, nil
	}
	// Double-Check Locking
	z.mu.Lock()
	defer z.mu.Unlock()
	if decoder, exists := z.versionDecoderMap[versionHash]; exists {
		return decoder, nil
	}
	// download dict
	dictData, err := z.DownloadDict(versionHash)
	if err != nil {
		return nil, xerrors.Errorf("failed to get dict: %w", err)
	}
	// verify the dict data with hash
	if z.calculateDictHash(dictData) != versionHash {
		return nil, xerrors.New("dictionary hash mismatch")
	}

	decoder, err = zstd.NewReader(nil, zstd.WithDecoderDicts(dictData))
	if err != nil {
		return nil, xerrors.Errorf("create decoder failed: %w", err)
	}

	z.versionDecoderMap[versionHash] = decoder
	z.dictCache[versionHash] = dictData
	return decoder, nil
}

func (z *ZstdCompressor) UploadDict(key string, data []byte) (string, error) {
	dictVersionHash := z.calculateDictHash(data)
	key = fmt.Sprintf("%s/%s", key, dictVersionHash)

	if _, err := z.dictUploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(z.dictBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}); err != nil {
		return "", xerrors.Errorf("failed to upload to s3: %w", err)
	}
	return dictVersionHash, nil
}

func (z *ZstdCompressor) DownloadDict(versionHash string) ([]byte, error) {

	buf := aws.NewWriteAtBuffer([]byte{})
	key := fmt.Sprintf("%s/%s", z.dictS3Path, versionHash)

	_, err := z.dictDownloader.Download(buf, &awss3.GetObjectInput{
		Bucket: aws.String(z.dictBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to download dictionary: %w", err)
	}
	return buf.Bytes(), nil
}
