package cscbrepair_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/config"
	chains3 "github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/cscbrepair"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestIntegrationCSCBRepairFullLifecycle(t *testing.T) {
	if os.Getenv("TEST_TYPE") != "integration" {
		t.Skip("integration test")
	}
	require := require.New(t)
	ctx := context.Background()
	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_700_000_000 + unique%100_000_000)
	height := uint64(9_500_000_000 + unique%100_000_000)
	bucket := fmt.Sprintf("chainstorage-cscb-repair-%d", unique)

	cfg, err := config.New(
		config.WithEnvironment(config.EnvLocal),
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	cfg.StorageType.BlobStorageType = config.BlobStorageType_S3
	cfg.StorageType.MetaStorageType = config.MetaStorageType_POSTGRES
	cfg.AWS.Bucket = bucket
	cfg.AWS.IsLocalStack = true
	cfg.AWS.IsResetLocal = true
	localStackEndpoint := configureRepairTestEnvironment(t, cfg.AWS.Postgres)
	cfg.Chain.BlockStartHeight = height
	cfg.AWS.Storage.Consolidation.Enabled = true
	cfg.AWS.Storage.Consolidation.Mode = config.ConsolidationModeHistoricalBackfill

	db, err := openRepairDB(ctx, cfg.AWS.Postgres)
	require.NoError(err)
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	var (
		meta                metastorage.MetaStorage
		blob                blobstorage.BlobStorage
		singleBlockUploader blobstorage.SingleBlockUploader
		s3Session           *chains3.S3
	)
	app := testapp.New(
		t,
		testapp.WithIntegration(),
		testapp.WithConfig(cfg),
		chains3.Module,
		metastorage.Module,
		blobstorage.Module,
		fx.Replace(newRepairLocalStackConfig(ctx, localStackEndpoint)),
		fx.Populate(&meta, &blob, &singleBlockUploader, &s3Session),
	)
	defer app.Close()
	require.NotNil(meta)
	require.NotNil(blob)
	require.NotNil(singleBlockUploader)
	require.NotNil(s3Session)

	rawS3 := awss3.NewFromConfig(s3Session.Config)
	_, err = rawS3.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &awss3types.VersioningConfiguration{
			Status: awss3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(err)
	defer cleanupRepairBucket(t, rawS3, bucket)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          tag,
			Height:       height,
			Hash:         fmt.Sprintf("repair-hash-%d", unique),
			ParentHash:   fmt.Sprintf("repair-parent-%d", unique),
			ParentHeight: height - 1,
			Timestamp:    timestamppb.New(time.Now().UTC().Truncate(time.Second)),
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{Header: []byte(`{"slot":9500000000,"transactions":["repair"]}`)},
		},
	}
	singleBlockKey, err := singleBlockUploader.Upload(ctx, block, api.Compression_GZIP)
	require.NoError(err)
	block.Metadata.ObjectKeyMain = singleBlockKey
	require.NoError(meta.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block.Metadata}, nil))

	records, err := meta.GetBlocksMissingConsolidationShadow(ctx, tag, height, height+1, 1)
	require.NoError(err)
	require.Len(records, 1)
	blockMetadataID := records[0].ID
	var repairID int64
	defer cleanupRepairMetadata(t, db, blockMetadataID, &repairID)

	downloaded, err := blob.Download(ctx, records[0].Metadata)
	require.NoError(err)
	dirtyBlock := proto.Clone(downloaded).(*api.Block)
	dirtyBlock.Metadata.ObjectKeyMain = singleBlockKey
	dirtyBlock.Metadata.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
	require.True(storageutils.HasBlockStoragePlacement(dirtyBlock))
	dirtyPayload, err := proto.Marshal(dirtyBlock)
	require.NoError(err)
	dirtyKey, dirtyPlacements, err := blob.UploadConsolidated(ctx, []blobstorage.ConsolidatedBlockPayload{{
		Metadata:           records[0].Metadata,
		MetadataID:         blockMetadataID,
		RawBlockPayload:    blobstorage.BytesPayloadSource(dirtyPayload),
		UncompressedLength: uint64(len(dirtyPayload)),
	}})
	require.NoError(err)
	require.Len(dirtyPlacements, 1)
	dirtyPlacement := dirtyPlacements[0]
	require.NoError(meta.PersistBlockConsolidationShadows(ctx, []*metastorage.ConsolidationShadowPlacement{{
		BlockMetadataID:           blockMetadataID,
		Tag:                       tag,
		Height:                    height,
		Hash:                      block.Metadata.Hash,
		SingleBlockObjectKeyMain:  singleBlockKey,
		ConsolidatedObjectKeyMain: dirtyKey,
		ObjectFormat:              dirtyPlacement.ObjectFormat,
		ByteOffset:                dirtyPlacement.ByteOffset,
		ByteLength:                dirtyPlacement.ByteLength,
		UncompressedLength:        dirtyPlacement.UncompressedLength,
	}}))
	promotion, err := meta.PromoteBlockConsolidationShadows(ctx, tag, height, height+1, 1, 72*time.Hour)
	require.NoError(err)
	require.Equal(uint64(1), promotion.Blocks)

	store := retirement.NewS3ObjectStore(rawS3)
	repairer := cscbrepair.NewRepairer(cscbrepair.NewPostgresRepository(db), store, bucket)
	executionKey := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	manifest, err := repairer.PrepareNext(ctx, executionKey, tag, height, height+1, 1, nil)
	require.NoError(err)
	require.NotNil(manifest)
	repairID = manifest.ID
	require.Equal(cscbrepair.StatePrepared, manifest.State)
	require.Equal(dirtyKey, manifest.OldConsolidatedObjectKey)
	require.Len(manifest.Blocks, 1)
	require.Len(manifest.Blocks[0].PayloadSHA256, 64)

	uploadGuard, err := meta.AcquireSingleBlockUploadGuard(ctx, tag, height, block.Metadata.Hash)
	require.NoError(err)
	require.True(uploadGuard.RetirementFenced(), "active repair must fence single-block uploads")
	require.NoError(uploadGuard.Release())
	_, err = db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_delete_after = clock_timestamp() - INTERVAL '1 second'
		WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	err = retirement.NewPostgresRepository(db).PrepareRetirement(ctx, retirement.RetirementManifest{
		BlockMetadataID:                blockMetadataID,
		Tag:                            tag,
		Height:                         height,
		Hash:                           block.Metadata.Hash,
		State:                          retirement.RetirementStateEligible,
		Bucket:                         bucket,
		SingleBlockObjectKey:           singleBlockKey,
		SingleBlockObjectKeySHA256:     repairSHA256(singleBlockKey),
		SingleBlockObjectVersionIDs:    []string{manifest.Blocks[0].SingleBlockObjectVersion.VersionID},
		SingleBlockObjectETag:          manifest.Blocks[0].SingleBlockObjectVersion.ETag,
		SingleBlockObjectBytes:         manifest.Blocks[0].SingleBlockObjectVersion.Bytes,
		ConsolidatedObjectKey:          dirtyKey,
		ConsolidatedObjectVersionID:    manifest.OldConsolidatedObjectVersion.VersionID,
		ConsolidatedObjectETag:         manifest.OldConsolidatedObjectVersion.ETag,
		ConsolidatedByteOffset:         manifest.Blocks[0].OldByteOffset,
		ConsolidatedByteLength:         manifest.Blocks[0].OldByteLength,
		ConsolidatedUncompressedLength: manifest.Blocks[0].OldUncompressedLength,
		PayloadSHA256:                  manifest.Blocks[0].PayloadSHA256,
		PreparedAt:                     time.Now().UTC(),
	})
	require.ErrorContains(err, "failed to lock canonical retirement metadata")

	manifest, err = repairer.Restore(ctx, manifest.ID, nil)
	require.NoError(err)
	require.Equal(cscbrepair.StateRestored, manifest.State)
	require.NotNil(manifest.RestoredAt)
	activeSingleBlock, err := meta.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(singleBlockKey, activeSingleBlock.ObjectKeyMain)
	require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK, activeSingleBlock.ObjectFormat)
	require.Zero(activeSingleBlock.ByteLength)
	restoredBlock, err := blob.Download(ctx, activeSingleBlock)
	require.NoError(err)
	require.True(proto.Equal(storageutils.CloneBlockWithoutStoragePlacement(block), storageutils.CloneBlockWithoutStoragePlacement(restoredBlock)))
	_, err = db.ExecContext(ctx, `UPDATE block_metadata SET object_key_main = $2 WHERE id = $1`, blockMetadataID, dirtyKey)
	require.ErrorContains(err, "cannot reference a pinned old CSCB object")
	_, err = db.ExecContext(ctx, `
		INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, single_block_object_key_main,
			consolidated_object_key_main, object_format, byte_offset, byte_length,
			uncompressed_length, validated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, clock_timestamp())`,
		blockMetadataID,
		tag,
		height,
		block.Metadata.Hash,
		singleBlockKey,
		dirtyKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		manifest.Blocks[0].OldByteOffset,
		manifest.Blocks[0].OldByteLength,
		manifest.Blocks[0].OldUncompressedLength,
	)
	require.ErrorContains(err, "cannot reference a pinned old CSCB object")

	records, err = meta.GetBlocksMissingConsolidationShadow(ctx, tag, height, height+1, 1)
	require.NoError(err)
	require.Len(records, 1)
	cleanBlock, err := blob.Download(ctx, records[0].Metadata)
	require.NoError(err)
	cleanPayload, err := proto.Marshal(storageutils.CloneBlockWithoutStoragePlacement(cleanBlock))
	require.NoError(err)
	cleanKey, cleanPlacements, err := blob.UploadConsolidated(ctx, []blobstorage.ConsolidatedBlockPayload{{
		Metadata:           records[0].Metadata,
		MetadataID:         blockMetadataID,
		RawBlockPayload:    blobstorage.BytesPayloadSource(cleanPayload),
		UncompressedLength: uint64(len(cleanPayload)),
	}})
	require.NoError(err)
	require.NotEqual(dirtyKey, cleanKey)
	require.Len(cleanPlacements, 1)
	cleanPlacement := cleanPlacements[0]
	require.NoError(meta.PersistBlockConsolidationShadows(ctx, []*metastorage.ConsolidationShadowPlacement{{
		BlockMetadataID:           blockMetadataID,
		Tag:                       tag,
		Height:                    height,
		Hash:                      block.Metadata.Hash,
		SingleBlockObjectKeyMain:  singleBlockKey,
		ConsolidatedObjectKeyMain: cleanKey,
		ObjectFormat:              cleanPlacement.ObjectFormat,
		ByteOffset:                cleanPlacement.ByteOffset,
		ByteLength:                cleanPlacement.ByteLength,
		UncompressedLength:        cleanPlacement.UncompressedLength,
	}}))
	promotion, err = meta.PromoteBlockConsolidationShadows(ctx, tag, height, height+1, 1, 72*time.Hour)
	require.NoError(err)
	require.Equal(uint64(1), promotion.Blocks)
	var retentionStartedAt, singleBlockDeleteAfter time.Time
	require.NoError(db.QueryRowContext(ctx, `
		SELECT single_block_retention_started_at, single_block_delete_after
		FROM block_consolidation_shadow
		WHERE block_metadata_id = $1`, blockMetadataID).Scan(&retentionStartedAt, &singleBlockDeleteAfter))
	require.False(retentionStartedAt.Before(*manifest.RestoredAt))
	require.WithinDuration(retentionStartedAt.Add(72*time.Hour), singleBlockDeleteAfter, time.Microsecond)

	manifest, err = repairer.VerifyRebuilt(ctx, manifest.ID, nil)
	require.NoError(err)
	require.Equal(cscbrepair.StateVerified, manifest.State)
	require.Equal(cleanKey, manifest.NewConsolidatedObjectKey)
	require.NotZero(manifest.NewConsolidatedObjectVersion.Bytes)
	require.NotNil(manifest.VerifiedAt)

	manifest, err = repairer.DeleteOldObject(ctx, manifest.ID)
	require.NoError(err)
	require.Equal(cscbrepair.StateCompleted, manifest.State)
	require.NotNil(manifest.OldObjectDeletedAt)
	require.NotNil(manifest.CompletedAt)
	dirtyTopology, err := store.ListObjectVersions(ctx, bucket, dirtyKey)
	require.NoError(err)
	require.Empty(dirtyTopology.Versions)
	require.Empty(dirtyTopology.DeleteMarkers)
	cleanTopology, err := store.ListObjectVersions(ctx, bucket, cleanKey)
	require.NoError(err)
	require.Len(cleanTopology.Versions, 1)
	rawClean, err := retirement.NewPinnedPayloadVerifier(store).InspectConsolidated(ctx, retirement.Candidate{
		Bucket:             bucket,
		ConsolidatedKey:    cleanKey,
		CSCBVersionID:      cleanTopology.Versions[0].VersionID,
		Tag:                tag,
		Height:             height,
		Hash:               block.Metadata.Hash,
		ByteOffset:         cleanPlacement.ByteOffset,
		ByteLength:         cleanPlacement.ByteLength,
		UncompressedLength: cleanPlacement.UncompressedLength,
	})
	require.NoError(err)
	require.False(rawClean.HasStoragePlacement)
	singleBlockTopology, err := store.ListObjectVersions(ctx, bucket, singleBlockKey)
	require.NoError(err)
	require.Len(singleBlockTopology.Versions, 1)

	activeClean, err := meta.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(cleanKey, activeClean.ObjectKeyMain)
	readClean, err := blob.Download(ctx, activeClean)
	require.NoError(err)
	// Normal downloads overlay the active database placement after parsing; raw
	// CSCB neutrality is asserted independently above.
	require.True(storageutils.HasBlockStoragePlacement(readClean))
	require.True(proto.Equal(
		storageutils.CloneBlockWithoutStoragePlacement(block),
		storageutils.CloneBlockWithoutStoragePlacement(readClean),
	))

	resumed, err := repairer.DeleteOldObject(ctx, manifest.ID)
	require.NoError(err)
	require.Equal(cscbrepair.StateCompleted, resumed.State)
	retried, err := repairer.PrepareNext(ctx, executionKey, tag, height, height+1, 1, nil)
	require.NoError(err)
	require.Equal(manifest.ID, retried.ID)
	require.Equal(cscbrepair.StateCompleted, retried.State)
	noCandidateExecutionKey := "123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0"
	next, err := repairer.PrepareNext(
		ctx,
		noCandidateExecutionKey,
		tag,
		height,
		height+1,
		1,
		nil,
	)
	require.NoError(err)
	require.Nil(next)
	next, err = repairer.PrepareNext(ctx, noCandidateExecutionKey, tag, height, height+1, 1, nil)
	require.NoError(err)
	require.Nil(next)
}

func configureRepairTestEnvironment(t *testing.T, postgres *config.PostgresConfig) string {
	t.Helper()
	switch postgres.Host {
	case "localhost", "127.0.0.1", "::1":
		postgres.Port = 5433
		postgres.Database = "chainstorage_solana_mainnet"
		postgres.User = "cs_solana_mainnet_worker"
		postgres.Password = "worker_password"
		postgres.SSLMode = "require"
		return "http://localhost:4566"
	case "postgres":
		return "http://localstack:4566"
	default:
		t.Fatalf("refusing to run CSCB repair lifecycle test against PostgreSQL host %q", postgres.Host)
		return ""
	}
}

func newRepairLocalStackConfig(ctx context.Context, endpoint string) aws.Config {
	cfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(string, string, ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint, HostnameImmutable: true}, nil
			}),
		),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to configure LocalStack: %v", err))
	}
	return cfg
}

func repairSHA256(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func openRepairDB(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host,
		cfg.Port,
		cfg.Database,
		cfg.User,
		cfg.Password,
		cfg.SSLMode,
	)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func cleanupRepairMetadata(t *testing.T, db *sql.DB, blockMetadataID int64, repairID *int64) {
	t.Helper()
	ctx := context.Background()
	if repairID != nil && *repairID != 0 {
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_execution DISABLE TRIGGER cscb_repair_execution_delete_trigger`)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_block DISABLE TRIGGER cscb_repair_block_delete_trigger`)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_manifest DISABLE TRIGGER cscb_repair_manifest_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM cscb_repair_execution WHERE repair_id = $1`, *repairID)
		_, _ = db.ExecContext(ctx, `DELETE FROM cscb_repair_block WHERE repair_id = $1`, *repairID)
		_, _ = db.ExecContext(ctx, `DELETE FROM cscb_repair_manifest WHERE id = $1`, *repairID)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_manifest ENABLE TRIGGER cscb_repair_manifest_delete_trigger`)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_block ENABLE TRIGGER cscb_repair_block_delete_trigger`)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_execution ENABLE TRIGGER cscb_repair_execution_delete_trigger`)
	}
	_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE block_metadata_id = $1`, blockMetadataID)
	_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE block_metadata_id = $1`, blockMetadataID)
	_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, blockMetadataID)
}

func cleanupRepairBucket(t *testing.T, client *awss3.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	versions, err := client.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err == nil {
		for _, version := range versions.Versions {
			_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: version.Key, VersionId: version.VersionId})
		}
		for _, marker := range versions.DeleteMarkers {
			_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: marker.Key, VersionId: marker.VersionId})
		}
	}
	_, _ = client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(bucket)})
}
