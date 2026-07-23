package retirement

import (
	"context"
	"time"

	"golang.org/x/xerrors"
)

const MaxRetentionCohortsPerWorkflow = 250

type (
	RetentionCohort struct {
		ConsolidatedObjectKey string    `json:"consolidated_object_key"`
		StartHeight           uint64    `json:"start_height"`
		EndHeight             uint64    `json:"end_height"`
		RowCount              uint64    `json:"row_count"`
		EligibleAt            time.Time `json:"eligible_at"`
		Pending               bool      `json:"pending"`
	}

	CohortRepository interface {
		ListPendingRetentionCohorts(
			ctx context.Context,
			tag uint32,
			startHeight uint64,
			endHeight uint64,
			limit int,
		) ([]RetentionCohort, error)
		ListDueRetentionCohorts(
			ctx context.Context,
			tag uint32,
			startHeight uint64,
			endHeight uint64,
			limit int,
		) ([]RetentionCohort, error)
	}

	Selector struct {
		repo CohortRepository
	}
)

func NewSelector(repo CohortRepository) *Selector {
	return &Selector{repo: repo}
}

func (s *Selector) Select(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	limit int,
) ([]RetentionCohort, bool, error) {
	if s == nil || s.repo == nil {
		return nil, false, xerrors.New("retention cohort repository is required")
	}
	if limit <= 0 || limit > MaxRetentionCohortsPerWorkflow {
		return nil, false, xerrors.Errorf(
			"retention cohort limit must be between 1 and %d: %d",
			MaxRetentionCohortsPerWorkflow,
			limit,
		)
	}
	if endHeight == 0 && startHeight != 0 {
		return nil, false, xerrors.New("retention selection end height is required when start height is set")
	}
	if endHeight != 0 && endHeight <= startHeight {
		return nil, false, xerrors.Errorf("invalid retention selection range [%d, %d)", startHeight, endHeight)
	}

	queryLimit := limit + 1
	pending, err := s.repo.ListPendingRetentionCohorts(ctx, tag, startHeight, endHeight, queryLimit)
	if err != nil {
		return nil, false, xerrors.Errorf("failed to list pending retention cohorts: %w", err)
	}
	due, err := s.repo.ListDueRetentionCohorts(ctx, tag, startHeight, endHeight, queryLimit)
	if err != nil {
		return nil, false, xerrors.Errorf("failed to list due retention cohorts: %w", err)
	}

	result := make([]RetentionCohort, 0, queryLimit)
	positions := make(map[string]int, queryLimit)
	appendOrMerge := func(cohort RetentionCohort) error {
		if cohort.ConsolidatedObjectKey == "" || cohort.EndHeight <= cohort.StartHeight || cohort.RowCount == 0 || cohort.EligibleAt.IsZero() {
			return xerrors.Errorf("invalid retention cohort: %+v", cohort)
		}
		if position, ok := positions[cohort.ConsolidatedObjectKey]; ok {
			existing := &result[position]
			if cohort.StartHeight < existing.StartHeight {
				existing.StartHeight = cohort.StartHeight
			}
			if cohort.EndHeight > existing.EndHeight {
				existing.EndHeight = cohort.EndHeight
			}
			if cohort.RowCount > existing.RowCount {
				existing.RowCount = cohort.RowCount
			}
			if cohort.EligibleAt.After(existing.EligibleAt) {
				existing.EligibleAt = cohort.EligibleAt
			}
			existing.Pending = existing.Pending || cohort.Pending
			return nil
		}
		if len(result) >= queryLimit {
			return nil
		}
		positions[cohort.ConsolidatedObjectKey] = len(result)
		result = append(result, cohort)
		return nil
	}

	for _, cohort := range pending {
		cohort.Pending = true
		if err := appendOrMerge(cohort); err != nil {
			return nil, false, err
		}
	}
	for _, cohort := range due {
		if err := appendOrMerge(cohort); err != nil {
			return nil, false, err
		}
	}
	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	return result, hasMore, nil
}
