# Debug: CSCB auto consolidation scheduler

**Date:** 2026-07-01
**Reporter:** INF-1046
**Environment:** local code review plus prod observations supplied in the task
**Method:** debug-hypothesis

## Scope

**Services involved:**
- `internal/cron/` - schedules automatic batch consolidation workflows.
- `internal/workflow/` - executes `historical_backfill` and `promote_finalized` modes.
- `internal/storage/metastorage/postgres/` - finds missing or promotable consolidation metadata.

**Dependencies confirmed with user:** yes

## Observations

- The cron task only accepted `promote_finalized`, looked up already-created shadows, and started `auto_promote_finalized`.
- Solana default reads already use consolidation sidecar metadata through `read_shadow_first`, so normal catch-up needs sidecar creation, not primary metadata promotion.
- The cron range end could be capped at `promotion_gate_height`, which is an upper gate and blocks ongoing ranges above the old cap.
- The cron range end could include a partial tail shorter than `consolidation.max_blocks`.
- The promotable-shadow lookup drove from canonical blocks before filtering validated shadows, which is a poor shape for the known timeout path.

## Hypotheses

1. The normal scheduler stalls because it promotes existing sidecars instead of creating missing sidecars.
   - Supports: cron requires `promote_finalized` and calls `GetFirstPromotableBlockConsolidationShadow`.
   - Conflicts: manual historical backfills work.
2. Partial object creation happens because cron caps by safe end without flooring to full object windows.
   - Supports: old `batchConsolidatorCronRangeEnd` only applied `max_range_blocks`, then min-capped at safe end.
   - Conflicts: workflow can technically process arbitrary ranges.
3. Promotion lookup can time out because its query cannot start from the selective validated-shadow candidate set.
   - Supports: old query starts with `canonical_blocks` and then filters shadow validity.
   - Conflicts: small local test ranges do not reproduce prod-scale timeouts.

## Experiments

- Added cron unit tests for full-window scheduling, residual waiting, promotion gate non-blocking behavior, and open-workflow skip behavior.
- Added storage coverage for first-missing-shadow lookup on the existing Postgres integration fixture.
- Ran focused cron/workflow/storage package tests.

## Root Cause

The cron was still modeled as a finalized-shadow promotion scheduler, while Solana's working default-read path needs automatic creation of missing consolidation sidecar rows over complete 1000-block windows.

## Fix

- Schedule `historical_backfill` workflows from the first missing consolidation sidecar row.
- Floor scheduled ranges to complete `consolidation.max_blocks` windows and leave residual blocks waiting.
- Stop using `promotion_gate_height` as an upper cap for sidecar creation.
- Rewrite missing/promotable lookup queries to start from the more selective indexed tables and add a partial promotable-shadow index.
