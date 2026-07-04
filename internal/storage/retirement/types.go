package retirement

import (
	"context"
	"time"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Repository interface {
		ListMetadataRows(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit uint64) ([]MetadataRow, error)
	}

	ObjectStore interface {
		HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error)
		CurrentObjectVersion(ctx context.Context, bucket string, key string) (ObjectVersion, error)
		DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string) error
	}

	MetadataRow struct {
		BlockMetadataID           int64
		Tag                       uint32
		Height                    uint64
		Hash                      string
		Skipped                   bool
		PrimaryObjectKey          string
		LegacyObjectKey           string
		PrimaryObjectFormat       api.BlockObjectFormat
		PrimaryByteOffset         uint64
		PrimaryByteLength         uint64
		PrimaryUncompressedLength uint64
		Shadow                    *ConsolidationShadow
	}

	ConsolidationShadow struct {
		Tag                     uint32
		Height                  uint64
		Hash                    string
		LegacyObjectKey         string
		ConsolidatedObjectKey   string
		ObjectFormat            api.BlockObjectFormat
		ByteOffset              uint64
		ByteLength              uint64
		UncompressedLength      uint64
		ValidatedAt             *time.Time
		LegacyObjectRetiredAt   *time.Time
		LegacyObjectRetireAfter *time.Time
		FormatVersion           int
	}

	ObjectHead struct {
		Exists     bool
		Bytes      uint64
		VersionID  string
		Metadata   map[string]string
		DeleteMark bool
	}

	ObjectVersion struct {
		Exists              bool
		CurrentDeleteMarker bool
		VersionID           string
		Bytes               uint64
		LastModified        *time.Time
	}

	PlanRequest struct {
		Environment             string
		Blockchain              string
		Network                 string
		Sidechain               string
		Bucket                  string
		Tag                     uint32
		StartHeight             uint64
		EndHeight               uint64
		Limit                   uint64
		Now                     time.Time
		Execute                 bool
		ClientMigrationApproved bool
		FallbackErrorCount      uint64
		Approval                Approval
	}

	Approval struct {
		Chain       string `json:"chain"`
		StartHeight uint64 `json:"start_height"`
		EndHeight   uint64 `json:"end_height"`
	}

	Report struct {
		GeneratedAt time.Time   `json:"generated_at"`
		DryRun      bool        `json:"dry_run"`
		Environment string      `json:"environment"`
		Blockchain  string      `json:"blockchain"`
		Network     string      `json:"network"`
		Sidechain   string      `json:"sidechain,omitempty"`
		Bucket      string      `json:"bucket"`
		Tag         uint32      `json:"tag"`
		StartHeight uint64      `json:"start_height"`
		EndHeight   uint64      `json:"end_height"`
		Approval    Approval    `json:"approval"`
		SafetyGates SafetyGates `json:"safety_gates"`
		Summary     Summary     `json:"summary"`
		Items       []Candidate `json:"items"`
	}

	SafetyGates struct {
		ClientMigrationApproved bool   `json:"client_migration_approved"`
		FallbackReadErrors      uint64 `json:"fallback_read_errors"`
		VersionedDeleteMode     string `json:"versioned_delete_mode"`
		ProductionDeleteEnabled bool   `json:"production_delete_enabled"`
	}

	Summary struct {
		TotalRows        int    `json:"total_rows"`
		EligibleRows     int    `json:"eligible_rows"`
		SkippedRows      int    `json:"skipped_rows"`
		LegacyBytes      uint64 `json:"legacy_bytes"`
		EligibleBytes    uint64 `json:"eligible_bytes"`
		DeleteMarkerRows int    `json:"delete_marker_rows"`
	}

	Candidate struct {
		Bucket          string     `json:"bucket"`
		Key             string     `json:"key"`
		VersionID       string     `json:"version_id,omitempty"`
		Height          uint64     `json:"height"`
		Hash            string     `json:"hash"`
		LegacyBytes     uint64     `json:"legacy_bytes"`
		ConsolidatedKey string     `json:"consolidated_key"`
		ValidatedAt     *time.Time `json:"validated_at"`
		RetiredAt       *time.Time `json:"retired_at"`
		EligibleAt      *time.Time `json:"eligible_at"`
		Action          string     `json:"action"`
		SkipReason      string     `json:"skip_reason"`
	}
)

const (
	ActionSkip                 = "skip"
	ActionReportOnly           = "report_only"
	ActionDeleteObjectVersion  = "delete_object_version"
	ActionDeletedObjectVersion = "deleted_object_version"

	SkipSkippedBlock               = "skipped_block"
	SkipMissingLegacyKey           = "missing_legacy_key"
	SkipMissingConsolidationShadow = "missing_consolidation_shadow"
	SkipValidationNotPassed        = "validation_not_passed"
	SkipActiveMetadataStillLegacy  = "active_metadata_still_legacy"
	SkipMissingRetirementMarker    = "missing_retirement_marker"
	SkipInvalidMetadataReference   = "invalid_metadata_reference"
	SkipRetentionPeriodActive      = "retention_period_active"
	SkipChainRangeNotApproved      = "chain_range_not_approved"
	SkipActiveFallbackOrReadErrors = "active_fallback_or_read_errors"
	SkipFileClientsNotApproved     = "file_clients_not_approved"
	SkipMissingCSCBObject          = "missing_cscb_object"
	SkipLegacyObjectMissing        = "legacy_object_missing"
	SkipLegacyCurrentDeleteMarker  = "legacy_current_delete_marker"
	SkipLegacyVersionIDUnavailable = "legacy_version_id_unavailable"
	SkipProductionDeletionDisabled = "production_deletion_disabled"
	SkipObjectInspectionFailed     = "object_inspection_failed"
	SkipVersionedDeleteFailed      = "versioned_delete_failed"
)
