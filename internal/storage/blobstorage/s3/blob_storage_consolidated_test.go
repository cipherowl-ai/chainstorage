package s3

import (
	"context"
	"crypto/md5" // #nosec G501
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/s3"
	s3mocks "github.com/coinbase/chainstorage/internal/s3/mocks"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestBlobStorage_UploadConsolidated_NewObject(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)

	var uploadedKey string
	var uploadedLength int64
	var uploadedSHA string
	gomock.InOrder(
		client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
				require.NotNil(input.Bucket)
				require.NotEmpty(*input.Bucket)
				require.NotNil(input.Key)
				require.True(strings.HasPrefix(*input.Key, "BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/consolidated/v=7/shard=00000000000000000000-00000000000000010000/00000000000000000100-00000000000000000102-"))
				return nil, &awss3types.NotFound{}
			}),
		uploader.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, input *awss3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
				require.NotNil(input.Bucket)
				require.NotEmpty(*input.Bucket)
				require.NotNil(input.Key)
				require.NotNil(input.Body)
				require.NotNil(input.ContentMD5)
				require.Equal(awss3types.ObjectCannedACLBucketOwnerFullControl, input.ACL)
				require.Equal("cscb", input.Metadata[cscbMetadataFormat])
				require.Equal("batch-chunked", input.Metadata[cscbMetadataCompressionScope])
				require.Equal("zstd", input.Metadata[cscbMetadataCodec])
				require.Equal("11", input.Metadata[cscbMetadataUncompressedLength])

				body, err := io.ReadAll(input.Body)
				require.NoError(err)
				var mgr manager.Uploader
				for _, opt := range opts {
					opt(&mgr)
				}
				require.Greater(mgr.PartSize, int64(len(body)))
				sum := sha256.Sum256(body)
				md5Sum := md5.Sum(body)
				uploadedKey = *input.Key
				uploadedLength = int64(len(body))
				uploadedSHA = fmt.Sprintf("%x", sum[:])
				require.Equal(uploadedSHA, input.Metadata[cscbMetadataSHA256])
				require.Contains(uploadedKey, uploadedSHA)
				require.Equal(base64.StdEncoding.EncodeToString(md5Sum[:]), *input.ContentMD5)
				return &manager.UploadOutput{}, nil
			}),
		client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
				require.Equal(uploadedKey, *input.Key)
				return &awss3.HeadObjectOutput{
					ContentLength: aws.Int64(uploadedLength),
					Metadata: map[string]string{
						cscbMetadataSHA256: uploadedSHA,
					},
				}, nil
			}),
	)

	var storage internal.BlobStorage
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(New),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	objectKey, placements, err := storage.UploadConsolidated(context.Background(), testS3Payloads(), api.Compression_ZSTD)
	require.NoError(err)
	require.Equal(uploadedKey, objectKey)
	require.Len(placements, 2)
	require.Equal(uploadedKey, placements[0].ObjectKey)
	require.Equal(uint64(0), placements[0].ByteOffset)
	require.Equal(uint64(5), placements[0].ByteLength)
	require.Equal(uint64(5), placements[1].ByteOffset)
	require.Equal(uint64(6), placements[1].ByteLength)
}

func TestBlobStorage_UploadConsolidated_AcceptsExistingExactObject(t *testing.T) {
	require := testutil.Require(t)

	expected, err := cscb.Encode(context.Background(), testS3EncodeConfig(), testS3Payloads())
	require.NoError(err)
	defer expected.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	uploader.EXPECT().Upload(gomock.Any(), gomock.Any()).Times(0)
	client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			require.Equal(expected.Key, *input.Key)
			return &awss3.HeadObjectOutput{
				ContentLength: aws.Int64(int64(expected.Length)),
				Metadata: map[string]string{
					cscbMetadataSHA256: expected.SHA256,
				},
			}, nil
		})

	var storage internal.BlobStorage
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(New),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	objectKey, placements, err := storage.UploadConsolidated(context.Background(), testS3Payloads(), api.Compression_ZSTD)
	require.NoError(err)
	require.Equal(expected.Key, objectKey)
	require.Len(placements, 2)
	require.Equal(expected.Key, placements[0].ObjectKey)
}

func TestBlobStorage_UploadConsolidated_RejectsExistingDifferentObject(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	uploader.EXPECT().Upload(gomock.Any(), gomock.Any()).Times(0)
	client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{
				ContentLength: aws.Int64(1),
				Metadata: map[string]string{
					cscbMetadataSHA256: "different",
				},
			}, nil
		})

	var storage internal.BlobStorage
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(New),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	_, _, err := storage.UploadConsolidated(context.Background(), testS3Payloads(), api.Compression_ZSTD)
	require.Error(err)
	require.Contains(err.Error(), "length mismatch")
}

func testS3EncodeConfig() cscb.EncodeConfig {
	return cscb.EncodeConfig{
		Blockchain:             common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:                common.Network_NETWORK_SOLANA_MAINNET,
		Codec:                  api.Compression_ZSTD,
		CodecLevel:             6,
		MaxBlocks:              1000,
		CompressionChunkBlocks: 10,
		MaxCompressedBytes:     2147483648,
		MaxUncompressedBytes:   137438953472,
		ShardSize:              10000,
	}
}

func testS3Payloads() []internal.ConsolidatedBlockPayload {
	return []internal.ConsolidatedBlockPayload{
		{
			Metadata: &api.BlockMetadata{
				Tag:    7,
				Height: 100,
				Hash:   "hash-100",
			},
			MetadataID:         1001,
			RawBlockPayload:    internal.BytesPayloadSource([]byte("alpha")),
			UncompressedLength: 5,
		},
		{
			Metadata: &api.BlockMetadata{
				Tag:    7,
				Height: 101,
				Hash:   "hash-101",
			},
			MetadataID:         1002,
			RawBlockPayload:    internal.BytesPayloadSource([]byte("bravo!")),
			UncompressedLength: 6,
		},
	}
}
