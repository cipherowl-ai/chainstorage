package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
)

type retirementFlags struct {
	tag                     uint32
	startHeight             uint64
	endHeight               uint64
	limit                   uint64
	gracePeriod             time.Duration
	approveChain            string
	approveStartHeight      uint64
	approveEndHeight        uint64
	clientMigrationApproved bool
	fallbackErrorCount      uint64
	execute                 bool
	reportFile              string
}

var legacyRetirementFlags retirementFlags

func newRetirementCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retirement",
		Short: "plan and execute guarded storage retirement operations",
	}
	cmd.AddCommand(newLegacyRetirementPlanCommand())
	return cmd
}

func newLegacyRetirementPlanCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plan-legacy-single-blocks",
		Short: "dry-run legacy single-block object retirement after CSCB validation",
		Long: `Plan retirement for legacy single-block S3 objects whose active metadata has already been promoted to validated CSCB metadata.

The command is dry-run by default. It emits one auditable JSON report containing the bucket,
legacy key, version id when available, height, hash, legacy bytes, consolidated key,
validated_at, retired_at, eligible_at, action, and skip reason for every scanned canonical row.

Production deletion is disabled. Non-production execution deletes only an explicit S3 object
version; rows without a version id are skipped to avoid delete-marker-only cleanup.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLegacyRetirementPlan(cmd.Context(), legacyRetirementFlags)
		},
	}

	cmd.Flags().Uint32Var(&legacyRetirementFlags.tag, "tag", 0, "block tag; default zero resolves to the configured stable tag")
	cmd.Flags().Uint64Var(&legacyRetirementFlags.startHeight, "start-height", 0, "inclusive block start height")
	cmd.Flags().Uint64Var(&legacyRetirementFlags.endHeight, "end-height", 0, "exclusive block end height")
	cmd.Flags().Uint64Var(&legacyRetirementFlags.limit, "limit", 0, "maximum rows to scan; default scans the full range")
	cmd.Flags().DurationVar(&legacyRetirementFlags.gracePeriod, "grace-period", 0, "fallback minimum age after validated_at when legacy_object_retire_after is not set; default uses aws.storage.consolidation.legacy_object_retention")
	cmd.Flags().StringVar(&legacyRetirementFlags.approveChain, "approve-chain", "", "explicit chain approval, e.g. solana-mainnet")
	cmd.Flags().Uint64Var(&legacyRetirementFlags.approveStartHeight, "approve-start-height", 0, "explicit approved start height")
	cmd.Flags().Uint64Var(&legacyRetirementFlags.approveEndHeight, "approve-end-height", 0, "explicit approved end height")
	cmd.Flags().BoolVar(&legacyRetirementFlags.clientMigrationApproved, "client-migration-approved", false, "confirm known file clients are migrated or out of scope")
	cmd.Flags().Uint64Var(&legacyRetirementFlags.fallbackErrorCount, "fallback-read-errors", 0, "active fallback/read error count from the operator's observation window")
	cmd.Flags().BoolVar(&legacyRetirementFlags.execute, "execute", false, "non-production only: delete eligible legacy object versions")
	cmd.Flags().StringVar(&legacyRetirementFlags.reportFile, "report-file", "", "write JSON report to this file instead of stdout")

	_ = cmd.MarkFlagRequired("start-height")
	_ = cmd.MarkFlagRequired("end-height")
	return cmd
}

func runLegacyRetirementPlan(ctx context.Context, flags retirementFlags) error {
	if flags.endHeight <= flags.startHeight {
		return xerrors.Errorf("end height must be greater than start height: start=%d end=%d", flags.startHeight, flags.endHeight)
	}
	if flags.execute && strings.EqualFold(commonFlags.env, string(config.EnvProduction)) {
		return xerrors.New("production deletion is disabled; run without --execute for a dry-run report")
	}

	var deps struct {
		fx.In
		S3Client s3.Client
	}
	app := startApp(
		aws.Module,
		s3.Module,
		fx.Populate(&deps),
	)
	defer app.Close()
	cfg := app.Config()
	if cfg.StorageType.MetaStorageType != config.MetaStorageType_POSTGRES {
		return xerrors.Errorf("legacy retirement planner requires Postgres meta storage, got %v", cfg.StorageType.MetaStorageType)
	}
	if cfg.StorageType.BlobStorageType != config.BlobStorageType_UNSPECIFIED && cfg.StorageType.BlobStorageType != config.BlobStorageType_S3 {
		return xerrors.Errorf("legacy retirement planner requires S3 blob storage, got %v", cfg.StorageType.BlobStorageType)
	}
	if cfg.AWS.Postgres == nil {
		return xerrors.New("postgres config is required")
	}

	tag := cfg.GetEffectiveBlockTag(flags.tag)
	gracePeriod := flags.gracePeriod
	if gracePeriod == 0 {
		gracePeriod = cfg.AWS.Storage.Consolidation.LegacyObjectRetention
	}
	targetChain := approvalChainFromFlags()
	logger.Info("planning legacy single-block retirement",
		zap.String("environment", string(cfg.Env())),
		zap.String("chain", targetChain),
		zap.String("bucket", cfg.AWS.Bucket),
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", flags.startHeight),
		zap.Uint64("end_height", flags.endHeight),
		zap.Bool("execute", flags.execute),
	)

	db, err := openReadOnlyPostgres(ctx, cfg.AWS.Postgres)
	if err != nil {
		return xerrors.Errorf("failed to open read-only postgres connection: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	planner := retirement.NewPlanner(
		retirement.NewPostgresRepository(db),
		retirement.NewS3ObjectStore(deps.S3Client),
	)
	req := retirement.PlanRequest{
		Environment:             string(cfg.Env()),
		Blockchain:              commonFlags.blockchain,
		Network:                 commonFlags.network,
		Sidechain:               commonFlags.sidechain,
		Bucket:                  cfg.AWS.Bucket,
		Tag:                     tag,
		StartHeight:             flags.startHeight,
		EndHeight:               flags.endHeight,
		Limit:                   flags.limit,
		Now:                     time.Now().UTC(),
		GracePeriod:             gracePeriod,
		Execute:                 flags.execute,
		ClientMigrationApproved: flags.clientMigrationApproved,
		FallbackErrorCount:      flags.fallbackErrorCount,
		Approval: retirement.Approval{
			Chain:       flags.approveChain,
			StartHeight: flags.approveStartHeight,
			EndHeight:   flags.approveEndHeight,
		},
	}

	report, err := planner.Plan(ctx, req)
	if err != nil {
		return xerrors.Errorf("failed to plan legacy retirement: %w", err)
	}
	if flags.execute {
		if err := planner.Apply(ctx, req, report); err != nil {
			return xerrors.Errorf("failed to execute legacy retirement: %w", err)
		}
	}
	if err := writeRetirementReport(flags.reportFile, report); err != nil {
		return err
	}
	return nil
}

func openReadOnlyPostgres(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)
	if cfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(cfg.ConnectTimeout.Seconds()))
	}
	dsn += " options='-c default_transaction_read_only=on'"

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if cfg.MaxConnections > 0 {
		db.SetMaxOpenConns(cfg.MaxConnections)
	}
	if cfg.MinConnections > 0 {
		db.SetMaxIdleConns(cfg.MinConnections)
	}
	db.SetConnMaxLifetime(cfg.MaxLifetime)
	db.SetConnMaxIdleTime(cfg.MaxIdleTime)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func writeRetirementReport(path string, report *retirement.Report) error {
	if path == "" {
		return retirement.WriteReportJSON(os.Stdout, report)
	}
	file, err := os.Create(path)
	if err != nil {
		return xerrors.Errorf("failed to create report file %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()
	if err := retirement.WriteReportJSON(file, report); err != nil {
		return xerrors.Errorf("failed to write report file %s: %w", path, err)
	}
	return nil
}

func approvalChainFromFlags() string {
	parts := []string{commonFlags.blockchain, commonFlags.network}
	if commonFlags.sidechain != "" {
		parts = append(parts, commonFlags.sidechain)
	}
	return strings.Join(parts, "-")
}

func init() {
	rootCmd.AddCommand(newRetirementCommand())
}
