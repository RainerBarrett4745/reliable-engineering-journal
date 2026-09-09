# Object Storage Image Replace: Authorization Before You Delete Old Thumbnails

Short answer: delete superseded thumbnail generations with an authorization-scoped manifest and an immediate batch cleanup after the new image is committed; keep a one-day lifecycle rule only as a delayed safety net. For an edtech platform, that extra manifest is worth the delivery complexity because a broad prefix list can cross a tenant boundary, while lifecycle expiration cannot prove that a selected backup snapshot remains restorable.

I've been paged by missed jobs and duplicate deliveries. The lesson that carries into image replacement is plain: a scheduled cleanup is an at-least-once workflow, not a clock you can trust as the source of truth. Every delete must be safe to repeat, and every candidate must be tied to the tenant and image generation that authorized it.

One invariant governs the design: publishing generation N+1 may make generation N eligible for deletion, but the cleanup worker may delete only objects recorded for N, never whatever happens to match a mutable current prefix.

That is the safety case.

## What does an object storage image replace incident teach about cleanup?

Treat image replacement as a small state machine rather than a list-then-delete convenience script. First, write the original and derived thumbnails under an immutable generation key such as `tenants/{tenantID}/media/{mediaID}/generations/{generationID}/...`. Second, record the exact object keys and their tenant owner in a manifest. Third, commit the database pointer to the new generation. Only after that commit may a cleanup task for the previous generation enter the queue.

The order matters because object storage and the application database do not share a transaction. If thumbnail generation fails, the visible pointer still names the prior complete set. If the pointer commit succeeds but queue delivery is delayed, users see the new set and old bytes remain temporarily. That is a leak to measure, not a reason to weaken the delete predicate. A retry reads the same immutable manifest, checks that the generation is no longer current and is not protected by a backup snapshot, then issues bounded batch deletes.

Duplicate delivery becomes boring.

Do not discover deletion candidates from `tenants/{tenantID}/media/{mediaID}/` and subtract the current keys in memory. It looks simple, but it turns concurrent replacements into a race: worker N can list while N+2 is being written, mistake partially written keys for debris, and erase data that another request is about to publish. Prefix enumeration still has a role in reconciliation, where it compares stored manifests with actual objects, but it should not define authority in the live replacement path.

Access control must be checked twice — when the replacement request creates the cleanup task and when the worker consumes it. The queued payload should carry stable identifiers, not a caller-supplied prefix. The worker derives the namespace from trusted tenant and media records, loads the manifest from that namespace, and rejects any key outside it. In a shared bucket, a missing tenant condition is a security event, not an empty list.

Fail closed.

## Runbook: enforce generation ownership before batch delete

The core can stay provider-neutral. This Go example deliberately puts listing behind a reconciliation interface and makes the normal cleanup path consume an immutable manifest. `DeleteBatch` reports per-key outcomes because a successful request does not necessarily mean every object reached the desired terminal state.

```go
package cleanup

import (
    "context"
    "errors"
    "fmt"
    "strings"
)

type Manifest struct {
    TenantID   string
    MediaID    string
    Generation string
    ObjectKeys []string
}

type DeleteResult struct {
    Key string
    Err error
}

type Store interface {
    DeleteBatch(ctx context.Context, keys []string) ([]DeleteResult, error)
}

type Catalog interface {
    CurrentGeneration(ctx context.Context, tenantID, mediaID string) (string, error)
    ProtectedBySnapshot(ctx context.Context, tenantID, mediaID, generation string) (bool, error)
    MarkDeleted(ctx context.Context, tenantID, mediaID, generation string) error
}

func DeleteGeneration(ctx context.Context, store Store, catalog Catalog, m Manifest) error {
    expectedPrefix := fmt.Sprintf("tenants/%s/media/%s/generations/%s/",
        m.TenantID, m.MediaID, m.Generation)
    if m.TenantID == "" || m.MediaID == "" || m.Generation == "" {
        return errors.New("cleanup manifest has an empty identity field")
    }
    for _, key := range m.ObjectKeys {
        if !strings.HasPrefix(key, expectedPrefix) {
            return fmt.Errorf("object key is outside the authorized generation: %q", key)
        }
    }

    current, err := catalog.CurrentGeneration(ctx, m.TenantID, m.MediaID)
    if err != nil {
        return fmt.Errorf("read current generation: %w", err)
    }
    if current == m.Generation {
        return errors.New("refusing to delete the current generation")
    }

    protected, err := catalog.ProtectedBySnapshot(ctx, m.TenantID, m.MediaID, m.Generation)
    if err != nil {
        return fmt.Errorf("check snapshot protection: %w", err)
    }
    if protected {
        return nil
    }

    const batchSize = 500
    for start := 0; start < len(m.ObjectKeys); start += batchSize {
        end := start + batchSize
        if end > len(m.ObjectKeys) {
            end = len(m.ObjectKeys)
        }
        results, err := store.DeleteBatch(ctx, m.ObjectKeys[start:end])
        if err != nil {
            return fmt.Errorf("submit delete batch: %w", err)
        }
        for _, result := range results {
            if result.Err != nil {
                return fmt.Errorf("delete %q: %w", result.Key, result.Err)
            }
        }
    }

    return catalog.MarkDeleted(ctx, m.TenantID, m.MediaID, m.Generation)
}
```

A production worker should checkpoint completed batches or safely replay the entire manifest. The second choice is often easier if deletion of an absent key has the same desired outcome, but verify that contract for the selected object store. Don't mark the manifest deleted until every per-key result is accounted for. Retries need exponential backoff, a cap, and a dead-letter path with tenant, media, generation, attempt count, and oldest-event age exposed to the runbook; raw object names may contain sensitive identifiers, so keep them out of broad logs.

I'm not sure what retention period a given school contract or regulatory policy requires. That answer must come from the data owner and compliance review, then become a tested snapshot-protection rule rather than an integer hidden in worker code. NIST SP 800-66 Rev. 2 frames risk analysis and safeguards for electronic protected health information; it does not choose a thumbnail retention period for you.

## Restore drill: prove snapshot pins before erasure

The application scenario complicates deletion: each tenant has backups, and an operator must be able to restore a selected snapshot. A snapshot that stores only the database pointer is incomplete if its referenced object generation has already been erased. Either the backup copies media objects into a snapshot-owned namespace, or the retention catalog pins every referenced generation until the last dependent snapshot expires. There isn't a safe third option where the restore procedure hopes yesterday's lifecycle policy left the bytes behind.

Use a restore drill to prove the contract. Create tenant A and tenant B, publish two generations for the same media record in A, take a snapshot between them, run cleanup more than once, and restore that snapshot into an isolated target. The drill passes only if A's selected generation and manifest return, B's objects are untouched, and the current production pointer never changes. Add a concurrent replacement during cleanup and inject a partial batch outcome. These tests exercise the boundary that unit tests around prefix formatting miss.

Object versioning can add recovery depth because overwriting an object creates another version and deletion commonly creates a delete marker rather than immediately erasing prior versions. It does not replace application manifests or snapshot pins: versions still need retention policy, permissions, and restore logic, and a request aimed at a specific version can have different consequences.

Versioning is recovery depth, not cleanup authority.

This is where delivery simplicity loses. A single mutable prefix plus a lifecycle rule has fewer moving parts; a manifest catalog, snapshot pins, and a queue require schema changes, metrics, and drills. For multi-tenant education records, that cost buys an auditable answer to two questions: who authorized deletion, and which restore points still depend on the data?

## Decision table: lifecycle simplicity versus access control

If the storage service has a one-day minimum lifecycle age, use that rule to collect objects abandoned before their manifest commit, not to remove previous thumbnails synchronously. A one-day delay is visible stale-data exposure, and lifecycle evaluation has no knowledge of an application transaction or a selected backup snapshot. Give temporary uploads their own narrow namespace and tag or timestamp them before publication, so the safety-net rule cannot match committed generations.

| Mechanism | Authority source | Best use | Boundary |
|---|---|---|---|
| Manifest batch cleanup | Tenant-scoped generation record | Immediate post-replace deletion | Adds catalog and queue operations |
| Lifecycle expiration | Object age and narrow namespace | Abandoned temporary uploads | Cannot see application commits or snapshot pins |
| Prefix reconciliation | Manifest-to-inventory comparison | Detecting drift | Must not authorize live deletion by itself |
| Object versioning | Stored object versions | Recovery depth | Still needs retention and restore logic |

Watch four signals: age of the oldest eligible manifest, count of failed object results, bytes retained beyond policy, and restore-drill success. Alert on age rather than queue depth alone; ten giant manifests can represent more risk than ten thousand tiny ones. Record cleanup task identity and generation in an audit trail, and make the runbook distinguish an authorization rejection from a retryable delivery failure. No delete should require an operator to infer tenant ownership from a string during an incident.

The catch is that manifests are not suitable when objects are intentionally shared across tenants or when external writers can create variants without registering them. Use reference counting or content-addressed ownership for shared objects, and use periodic inventory reconciliation for uncontrolled writers. Stick with a simple lifecycle-only design when every object is disposable, no snapshot must restore it, deletion latency of at least one day meets policy, and the namespace is isolated enough that a broad rule cannot cross an access-control boundary.

For the edtech case, I would accept the extra catalog and queue. The decision is driven by tenant isolation and provable restores, not by how few API calls fit in the replacement handler.

## References

- https://csrc.nist.gov/pubs/sp/800/66/r2/final
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html
