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
	approveChain            string
	approveStartHeight      uint64
	approveEndHeight        uint64
	clientMigrationApproved bool
	fallbackErrorCount      uint64
	execute                 bool
	confirmProductionDelete bool
	reportFile              string
}

var (
	legacyRetirementFlags          retirementFlags
	legacyRetirementReconcileFlags retirementFlags
)

func newRetirementCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retirement",
		Short: "plan and execute guarded storage retirement operations",
	}
	cmd.AddCommand(newLegacyRetirementPlanCommand())
	cmd.AddCommand(newLegacyRetirementReconcileCommand())
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

Execution persists a write-ahead manifest, independently parses and compares the pinned legacy
version with the pinned CSCB payload, revalidates immediately before deleting exactly one S3
version, verifies the key has no remaining versions, transactionally clears the legacy path,
then performs a fresh CSCB range read before marking the retirement verified.
The command also verifies the live bucket policy denies every unconditional write and every API
delete to each CSCB key, so no newer version or delete marker can replace the pinned payload.
Production execution requires both --execute and --confirm-production-delete.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLegacyRetirementPlan(cmd.Context(), legacyRetirementFlags)
		},
	}

	addLegacyRetirementFlags(cmd, &legacyRetirementFlags)
	return cmd
}

func newLegacyRetirementReconcileCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reconcile-legacy-single-blocks",
		Short: "inspect or resume durable legacy object retirements",
		Long: `Inspect write-ahead retirement manifests left in eligible, deleting, or deleted-pending-verification state.

The command is report-only by default. With explicit execution gates, it can safely resume a
pre-delete manifest, record an already-completed S3 deletion while clearing the legacy path, or
finish fresh CSCB verification against the persisted digest after the path has been cleared.
Execution remains blocked unless the live CSCB write-once bucket policy is verifiable.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLegacyRetirementReconcile(cmd.Context(), legacyRetirementReconcileFlags)
		},
	}
	addLegacyRetirementFlags(cmd, &legacyRetirementReconcileFlags)
	return cmd
}

func addLegacyRetirementFlags(cmd *cobra.Command, flags *retirementFlags) {
	cmd.Flags().Uint32Var(&flags.tag, "tag", 0, "block tag; default zero resolves to the configured stable tag")
	cmd.Flags().Uint64Var(&flags.startHeight, "start-height", 0, "inclusive block start height")
	cmd.Flags().Uint64Var(&flags.endHeight, "end-height", 0, "exclusive block end height")
	cmd.Flags().Uint64Var(&flags.limit, "limit", 0, "maximum rows to scan; default scans the full range")
	cmd.Flags().StringVar(&flags.approveChain, "approve-chain", "", "explicit chain approval, e.g. solana-mainnet")
	cmd.Flags().Uint64Var(&flags.approveStartHeight, "approve-start-height", 0, "explicit approved start height")
	cmd.Flags().Uint64Var(&flags.approveEndHeight, "approve-end-height", 0, "explicit approved end height")
	cmd.Flags().BoolVar(&flags.clientMigrationApproved, "client-migration-approved", false, "confirm known file clients are migrated or out of scope")
	cmd.Flags().Uint64Var(&flags.fallbackErrorCount, "fallback-read-errors", 0, "active fallback/read error count from the operator's observation window")
	cmd.Flags().BoolVar(&flags.execute, "execute", false, "execute guarded retirement state transitions and exact-version deletion")
	cmd.Flags().BoolVar(&flags.confirmProductionDelete, "confirm-production-delete", false, "second explicit gate required with --execute in production")
	cmd.Flags().StringVar(&flags.reportFile, "report-file", "", "write JSON report to this file instead of stdout")

	_ = cmd.MarkFlagRequired("start-height")
	_ = cmd.MarkFlagRequired("end-height")
}

func runLegacyRetirementPlan(ctx context.Context, flags retirementFlags) error {
	if flags.endHeight <= flags.startHeight {
		return xerrors.Errorf("end height must be greater than start height: start=%d end=%d", flags.startHeight, flags.endHeight)
	}
	if flags.execute && isProductionEnvironment(commonFlags.env) && !flags.confirmProductionDelete {
		return xerrors.New("production execution requires --execute and --confirm-production-delete")
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

	db, err := openRetirementPostgres(ctx, cfg.AWS.Postgres, !flags.execute)
	if err != nil {
		return xerrors.Errorf("failed to open retirement postgres connection: %w", err)
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
		Execute:                 flags.execute,
		ProductionDeleteEnabled: flags.confirmProductionDelete,
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
		err = planner.Apply(ctx, req, report)
	}
	if reportErr := writeRetirementReport(flags.reportFile, report); reportErr != nil {
		if err != nil {
			return xerrors.Errorf("legacy retirement failed (%v) and report write failed: %w", err, reportErr)
		}
		return reportErr
	}
	if err != nil {
		return xerrors.Errorf("failed to execute one or more legacy retirements: %w", err)
	}
	return nil
}

func runLegacyRetirementReconcile(ctx context.Context, flags retirementFlags) error {
	if flags.endHeight <= flags.startHeight {
		return xerrors.Errorf("end height must be greater than start height: start=%d end=%d", flags.startHeight, flags.endHeight)
	}
	if flags.execute && isProductionEnvironment(commonFlags.env) && !flags.confirmProductionDelete {
		return xerrors.New("production reconciliation requires --execute and --confirm-production-delete")
	}

	var deps struct {
		fx.In
		S3Client s3.Client
	}
	app := startApp(aws.Module, s3.Module, fx.Populate(&deps))
	defer app.Close()
	cfg := app.Config()
	if cfg.StorageType.MetaStorageType != config.MetaStorageType_POSTGRES || cfg.AWS.Postgres == nil {
		return xerrors.New("legacy retirement reconciler requires Postgres meta storage")
	}
	if cfg.StorageType.BlobStorageType != config.BlobStorageType_UNSPECIFIED && cfg.StorageType.BlobStorageType != config.BlobStorageType_S3 {
		return xerrors.Errorf("legacy retirement reconciler requires S3 blob storage, got %v", cfg.StorageType.BlobStorageType)
	}

	tag := cfg.GetEffectiveBlockTag(flags.tag)
	db, err := openRetirementPostgres(ctx, cfg.AWS.Postgres, !flags.execute)
	if err != nil {
		return xerrors.Errorf("failed to open retirement postgres connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	planner := retirement.NewPlanner(retirement.NewPostgresRepository(db), retirement.NewS3ObjectStore(deps.S3Client))
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
		Execute:                 flags.execute,
		ProductionDeleteEnabled: flags.confirmProductionDelete,
		ClientMigrationApproved: flags.clientMigrationApproved,
		FallbackErrorCount:      flags.fallbackErrorCount,
		Approval: retirement.Approval{
			Chain:       flags.approveChain,
			StartHeight: flags.approveStartHeight,
			EndHeight:   flags.approveEndHeight,
		},
	}
	report, reconcileErr := planner.Reconcile(ctx, req)
	if report == nil {
		return xerrors.Errorf("failed to reconcile legacy retirements: %w", reconcileErr)
	}
	if reportErr := writeRetirementReport(flags.reportFile, report); reportErr != nil {
		if reconcileErr != nil {
			return xerrors.Errorf("legacy retirement reconciliation failed (%v) and report write failed: %w", reconcileErr, reportErr)
		}
		return reportErr
	}
	if reconcileErr != nil {
		return xerrors.Errorf("failed to reconcile one or more legacy retirements: %w", reconcileErr)
	}
	return nil
}

func openRetirementPostgres(ctx context.Context, cfg *config.PostgresConfig, readOnly bool) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)
	if cfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(cfg.ConnectTimeout.Seconds()))
	}
	if readOnly {
		dsn += " options='-c default_transaction_read_only=on'"
	}

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

func isProductionEnvironment(value string) bool {
	return strings.EqualFold(value, string(config.EnvProduction)) || strings.EqualFold(value, "prod")
}

func init() {
	rootCmd.AddCommand(newRetirementCommand())
}
