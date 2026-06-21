# Debug: batch consolidator activity pending after committed side effects

**Date:** 2026-06-20
**Reporter:** user
**Environment:** prod
**Method:** debug-hypothesis

## Scope

**Services involved:**
- `internal/workflow/activity/batch_consolidator.go` - activity that scans unconsolidated blocks, uploads CSCB objects, and persists shadow metadata.
- `internal/workflow/batch_consolidator.go` - workflow loop that retries the activity and advances height windows.
- `src/helm/chainstorage` in monorepo - prod batch-worker resource sizing and deployment drift.

## Observations

- Prod Solana canary on image `69573b9` successfully uploaded CSCB objects and persisted shadow rows.
- Activity `51` logged successful consolidation for range `427819004-427819504`, but the workflow did not log `processed shadow object`.
- Temporal showed activity `51` still `PENDING_ACTIVITY_STATE_STARTED` with the last heartbeat at `batch_consolidator.block_downloaded`.
- The activity has committed external side effects before returning, so completion delivery failure leaves the workflow waiting until heartbeat timeout before a retry can reconcile state.

## Hypotheses

### H1: Temporal completion delivery failed after S3 and DB side effects (ROOT HYPOTHESIS)
- Supports: S3/DB success log exists, workflow did not receive the activity response, Temporal still showed the activity pending.
- Conflicts: none; this matches the exact boundary between activity side effects and workflow progress.
- Test: make the activity heartbeat continuously and shorten heartbeat timeout so pending committed work retries quickly.

### H2: Heartbeats stop during encode/upload/persist, making activity liveness stale
- Supports: last heartbeat details were from block download, not upload/persist.
- Conflicts: the activity had not timed out yet when inspected.
- Test: add stage heartbeats after payload build, upload, and shadow persist.

### H3: Batch worker scheduling pressure caused worker disruption
- Supports: rollout events showed insufficient CPU, memory, and ephemeral-storage before the pod scheduled.
- Conflicts: pod ultimately stayed running with zero restarts.
- Test: reduce over-reserved Solana batch-worker memory/ephemeral requests while staying above measured usage and spill needs.

## Experiments

- Code inspection confirmed batch consolidator only heartbeats during per-block download, not while encoding/uploading/persisting or while waiting for completion delivery.

## Root Cause

The batch consolidator committed S3/DB side effects before Temporal recorded activity completion, and the activity had no continuous/stage heartbeat after block downloads, so the workflow could sit pending for the full heartbeat timeout before retrying idempotently.

## Fix

- Heartbeat throughout the activity and at commit-stage boundaries.
- Shorten the batch-consolidator heartbeat timeout so completion transport failures retry quickly.
- Reduce Solana batch-worker resource requests separately in the deployment chart to improve scheduling headroom.
