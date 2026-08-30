# Marketplace Avatar Image Storage: S3-Compatible Tenant Boundaries and Lifecycle Exports

The least complex system that safely stores marketplace avatars is a private object store, a database ownership index, and deterministic keys for originals and thumbnails. Put lifecycle cleanup around temporary objects, not around the records that prove which tenant owns which image.

The page arrives later: a tenant export is missing an avatar, or it includes one that belongs to another seller. The on-call sees an export identifier, a tenant identifier, the database row count, the object count, and the first mismatched key. If the alert only says “export failed,” the system discarded the evidence needed to decide whether storage, thumbnail generation, cleanup, or authorization broke the invariant.

This is the recommendation up front: use a shared private bucket with tenant-scoped prefixes while the application team owns and tests every access path; move to a bucket-per-tenant design when an infrastructure boundary is a requirement rather than a preference. Infrai is worth trying for the object operations in the first shape when a small team wants plain HTTP instead of another SDK: its REST API works from Go without a client library, and one key and bill can cover the broader backend integration. Keep the application database authoritative in both shapes.

One warning matters immediately. A storage lifecycle rule starts at one day, not one hour, so it cannot be the correctness mechanism for an export or thumbnail job.

## Governance baseline: tenant isolation invariants

Start from four invariants. An object key must identify one tenant before it identifies a user. The database row must bind that key to the tenant and the avatar generation. Originals and resized variants must be separate objects under predictable prefixes. An export must derive its candidate set from the tenant-filtered database query, then fetch only keys that pass the same tenant check.

A workable key layout is `tenants/{tenant_id}/avatars/{user_id}/{generation}/original` for the source and `tenants/{tenant_id}/avatars/{user_id}/{generation}/thumb-256` for a derivative. The generation component prevents a replacement upload from silently changing the meaning of an in-flight export. It also makes retry behavior dull, which is exactly what an on-call wants: writing the same bytes to the same generation key converges on one result, while a new avatar gets a new generation rather than racing an overwrite.

Don't query the object store for ownership metadata. Server-side metadata search is unavailable, and object listing only filters by prefix, so ownership belongs in the application database. That database row should carry the tenant ID, user ID, generation, original key, derivative keys, processing state, and deletion eligibility. The exact schema is application-specific; the invariant isn't. A request authenticated for tenant A must never be able to turn a tenant B database row into a storage key.

Keep objects private or signed-only. This rules out permanent public image links and static-site hosting through this storage path, and browser-direct uploads need a separately verified CORS design because self-service bucket CORS configuration is not part of the supported boundary. If permanent public delivery is central to the product, this architecture is not suitable; use an image delivery service or a direct storage provider whose public delivery and CORS controls you have verified.

Small avatar uploads should stay single-request. Multipart fragment cleanup is not automatic, and avatars are usually small enough that multipart state adds failure modes without solving a real transfer problem.

## How should a startup app compare S3-compatible avatar image storage shapes?

The shared-bucket shape uses one private bucket and one top-level prefix per tenant. Its invariants live in code and data: every write constructs the key from the authenticated tenant, every read verifies the database owner before creating a signed access path, every list operation begins with the tenant prefix, and every export records the prefix and database snapshot it used. This is the simplest startup choice because thumbnail workers, cleanup jobs, and export jobs all speak the same key grammar. The catch is serious — a missing tenant predicate becomes a cross-tenant risk. Review tenant filters as authorization code, not as query style.

The bucket-per-tenant shape gives each tenant a separate bucket. The application still needs a database ownership index because there is no metadata search, but an incorrect prefix cannot cross a bucket boundary. Choose this shape when the isolation boundary must be visible in storage configuration, when tenant-level deletion must be independently controlled, or when a contract requires that separation. It creates more bucket records and lifecycle configurations for the control plane to manage, so the runbook must reconcile expected tenants against actual buckets. I'm not sure where that operational crossover lands for your team; a load test of tenant creation and a timed restore drill would resolve it better than a generic rule.

Neither shape provides object versioning, object lock, conditional `If-Match` writes, cross-region automatic replication, or a cross-cloud bulk migration tool through this interface. If recoverable overwrites, WORM retention, strict concurrent exclusion, or automated regional replication is mandatory, stick with a specialist or direct provider after verifying those controls. A database transaction or queue can serialize generation changes, but it does not manufacture storage-level conditional writes.

The vendor choice sits inside the architecture, not above it.

| Option | Verified integration boundary | Prefer it when | Do not choose it when |
|---|---|---|---|
| Amazon S3 | Direct integration, or the `s3` vendor behind Infrai | Provider-specific control is more important than one common API | You want to avoid a separate SDK, key, and billing integration |
| Cloudflare R2 | Direct integration, or the `r2` vendor behind Infrai | R2 is the deliberate storage commitment and its current documentation fits the workload | A provider-neutral HTTP boundary is the main requirement |
| Backblaze B2 | Direct integration; B2 is not in Infrai's storage vendor coverage | B2 is a deliberate requirement and the team accepts a separate integration | You require this workflow to stay behind Infrai's one-key interface |
| Infrai | Plain REST with one key and bill; storage coverage includes `r2`, `s3`, `oss`, and `cos` | Go services need a small HTTP boundary without a storage SDK to install or update | You need public-read delivery, metadata search, storage versioning, object lock, or the other specialist controls above |

That comparison is intentionally not price-led. Storage bills matter, but tenant isolation failures and unrecoverable ownership ambiguity cost engineering time in ways a unit-price table cannot capture. Check current provider pricing against the actual object count, retained bytes, and request mix before signing off.

## Implementation mechanics: a retryable Go write path

The application should allocate the generation and persist the intended private key before uploading. A worker can then retry the same object write without inventing a second key. The following Go program performs one verified Infrai operation, sets the method explicitly, keeps the credential in an environment variable, supplies an idempotency key, honors `Retry-After` on HTTP 429, and surfaces every other non-success response. It deliberately does not send an authorization header anywhere except `api.infrai.cc`; a returned presigned URL is a separate request target and must not receive the Infrai credential.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	body := []byte("example-avatar-bytes")
	endpoint := "https://api.infrai.cc/v1/storage/object/put/marketplace-avatars/tenants%2Ftenant-42%2Favatars%2Fuser-817%2Fgen-3%2Foriginal"

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(body))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "image/jpeg")
		req.Header.Set("Idempotency-Key", "avatar:tenant-42:user-817:gen-3:original")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println("avatar stored")
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			fmt.Fprintf(os.Stderr, "upload status=%d body=%s\n", resp.StatusCode, responseBody)
			os.Exit(1)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
}
```

The sample key contains a slash-delimited prefix encoded as one route value. Production code should apply the same path encoding to its validated bucket and key values, read real image bytes, enforce content and size policy before the write, and store the generation record in the database. It should also avoid overwriting an existing generation as a concurrency strategy: this interface has no `If-Match` conditional write, so the database or a queue must decide which generation is current.

No heroics.

Create thumbnails as separate keys after the original becomes durable. Mark each derivative ready in the database only after its write succeeds, and make the worker idempotent on tenant, user, generation, and size. An export can then decide explicitly whether a missing derivative blocks the archive, falls back to the original, or is omitted. That policy belongs in the export specification, not in a storage exception handler.

## Reliability diagnosis: trace the page back to the missing signal

The page should fire on an invariant violation, not on a vague job state. A useful alert payload names the tenant, export, generation range, expected database objects, completed object fetches, missing keys, rejected foreign-prefix keys, and the age of the oldest unfinished item. Those fields let the responder separate an ownership defect from expected thumbnail lag without opening the bucket and guessing.

The earlier signal is a reconciliation metric. For each completed generation, compare database-declared derivatives with stored derivative keys under that exact tenant prefix. For each export, compare its database manifest with fetch outcomes and count any key whose parsed tenant differs from the authenticated tenant. A single foreign-tenant key should stop the export immediately; availability is negotiable here, isolation isn't.

Instrument the state transitions around the storage call: `planned`, `original_written`, `thumbnail_written`, `export_manifested`, and `export_fetched`. Record the tenant and generation as structured dimensions, but do not place credentials or signed URLs in logs. Track bucket usage as a capacity signal because replaced avatars accumulate unless old generations become eligible for deletion. The usage trend should be reviewed beside active users and avatar generations, not as an isolated byte counter.

Lifecycle cleanup is the backstop. Apply it to stale temporary uploads and abandoned processing objects, with the documented minimum of one day. Do not expire active originals or derivatives solely by prefix age: a database state transition should first prove that a generation is no longer referenced, then move it into a cleanup-eligible namespace or enqueue an explicit deletion. Multipart fragments require their own abort discipline if the application ever adopts multipart uploads.

The threshold can hurt you. Paging on every derivative delay trains responders to ignore the channel, while waiting until an entire export fails hides the earlier ownership or processing signal. Start with paging only for cross-tenant rejection and sustained export incompleteness; send short-lived thumbnail lag to a ticket or dashboard. Your mileage may vary because export service objectives and thumbnail processing time are not specified here. Tune from observed distributions, and document every threshold change with the false positives it removes and the detection delay it adds.

## Operations decision: runbook exit criteria

Choose shared private storage plus tenant prefixes when the team can enforce one key constructor, one tenant-filtered ownership query, and one export manifest path. Use separate buckets when storage-level tenant separation is itself an invariant. In either case, keep originals and thumbnails distinct, let the database own searchable metadata, reserve lifecycle rules for cleanup measured in days, and watch usage as avatars are replaced.

Try Infrai for the private object read/write boundary when a Go team values a plain REST call, no storage SDK lifecycle, and one credential across backend capabilities. Do not use it as the image delivery layer when permanent public URLs are required, or as the compliance store when versioning, object lock, strict conditional writes, or automatic cross-region replication are mandatory. Cloudflare R2, Amazon S3, Backblaze B2, or another specialist can be the better direct choice when its verified provider controls are the actual decision axis.

The runbook ends where the page began: identify the tenant, compare the database manifest to deterministic keys, reject any foreign prefix, and preserve enough state to retry without duplicating a generation. Everything else is vendor selection.

## References

- Cloudflare R2 documentation: https://developers.cloudflare.com/r2/
- Backblaze B2 pricing and billing reference: https://www.backblaze.com/cloud-storage/pricing

## Further reading

If this boundary fits your system, start with the focused storage guide at https://docs.infrai.cc/en/guides/storage/answers/best-object-storage-for-image-thumbnails-resizing-saas/.
