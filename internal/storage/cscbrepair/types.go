package cscbrepair

import (
	"context"
	"time"
)

type (
	State string

	Progress func(stage string, completed int, total int, height uint64)

	ObjectVersion struct {
		VersionID string
		ETag      string
		Bytes     uint64
	}

	Block struct {
		BlockMetadataID           int64
		Canonical                 bool
		Tag                       uint32
		Height                    uint64
		Hash                      string
		Skipped                   bool
		RetirementFenced          bool
		RetirementManifestExists  bool
		SingleBlockObjectKey      string
		SingleBlockObjectDeleted  bool
		SingleBlockObjectVersion  ObjectVersion
		PayloadSHA256             string
		OldConsolidatedObjectKey  string
		OldByteOffset             uint64
		OldByteLength             uint64
		OldUncompressedLength     uint64
		ActiveObjectKey           string
		ActiveObjectFormat        int32
		NewConsolidatedObjectKey  string
		NewByteOffset             uint64
		NewByteLength             uint64
		NewUncompressedLength     uint64
		NewValidatedAt            *time.Time
		NewRetentionStartedAt     *time.Time
		NewSingleBlockDeleteAfter *time.Time
	}

	Manifest struct {
		ID                           int64
		Tag                          uint32
		State                        State
		Bucket                       string
		OldConsolidatedObjectKey     string
		OldConsolidatedObjectVersion ObjectVersion
		StartHeight                  uint64
		EndHeight                    uint64
		CanonicalBlockCount          uint64
		TotalBlockCount              uint64
		RowSetSHA256                 string
		NewConsolidatedObjectKey     string
		NewConsolidatedObjectVersion ObjectVersion
		Outcome                      string
		PreparedAt                   time.Time
		RestoredAt                   *time.Time
		VerifiedAt                   *time.Time
		OldObjectDeletedAt           *time.Time
		CompletedAt                  *time.Time
		Blocks                       []Block
	}

	Repository interface {
		FindPending(ctx context.Context, tag uint32) (*Manifest, error)
		FindNextCandidate(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) (*Manifest, error)
		Prepare(ctx context.Context, manifest *Manifest) (*Manifest, error)
		Get(ctx context.Context, repairID int64) (*Manifest, error)
		RestoreToSingleBlock(ctx context.Context, repairID int64, validate func(*Manifest) error) (*Manifest, error)
		GetRebuilt(ctx context.Context, repairID int64) (*Manifest, error)
		RecordVerified(ctx context.Context, repairID int64, objectKey string, object ObjectVersion) (*Manifest, error)
		Complete(ctx context.Context, repairID int64, outcome string) (*Manifest, error)
	}

	Repairer interface {
		PrepareNext(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, maxBlocks uint64, progress Progress) (*Manifest, error)
		Restore(ctx context.Context, repairID int64, progress Progress) (*Manifest, error)
		Get(ctx context.Context, repairID int64) (*Manifest, error)
		VerifyRebuilt(ctx context.Context, repairID int64, progress Progress) (*Manifest, error)
		DeleteOldObject(ctx context.Context, repairID int64) (*Manifest, error)
	}
)

const (
	StatePrepared  State = "prepared"
	StateRestored  State = "restored"
	StateVerified  State = "verified"
	StateCompleted State = "completed"
)

func reportProgress(progress Progress, stage string, completed int, total int, height uint64) {
	if progress != nil {
		progress(stage, completed, total, height)
	}
}
