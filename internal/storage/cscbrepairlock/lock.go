package cscbrepairlock

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

// AcquireTag serializes repair transitions with metadata placement writers.
// All callers must acquire this lock before locking block metadata or shadow rows.
func AcquireTag(ctx context.Context, tx *sql.Tx, tag uint32) error {
	key := fmt.Sprintf("cscb_repair_tag/%d", tag)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 2))`, key); err != nil {
		return fmt.Errorf("failed to acquire CSCB repair tag lock: %w", err)
	}
	return nil
}

// AcquireTags locks tags in ascending order so transactions spanning multiple
// tags cannot invert their advisory-lock order.
func AcquireTags(ctx context.Context, tx *sql.Tx, tags []uint32) error {
	unique := make(map[uint32]struct{}, len(tags))
	ordered := make([]uint32, 0, len(tags))
	for _, tag := range tags {
		if _, ok := unique[tag]; ok {
			continue
		}
		unique[tag] = struct{}{}
		ordered = append(ordered, tag)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, tag := range ordered {
		if err := AcquireTag(ctx, tx, tag); err != nil {
			return err
		}
	}
	return nil
}
