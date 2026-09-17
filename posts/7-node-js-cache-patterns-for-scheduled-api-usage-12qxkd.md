# 7 Node.js Cache Patterns for Scheduled API Usage Fetches — Dashboard Stale Timestamps

When a logistics API key must rotate during a dispatch shift, the least complex safe option is a dual-key window: issue the replacement, make consumers accept both values, switch traffic, then revoke the old value after observed use reaches zero. A usage dashboard helps prove that the old credential is quiet, but only if its data has a trustworthy timestamp.

Short answer: schedule small, idempotent usage fetches into your own store, label freshness separately from the event time, and use the resulting chart as evidence for key cutover rather than as the cutover mechanism.

I've been paged for missed jobs and duplicate deliveries. The page rarely says \"the key is stale.\" It says a carrier update is late, or a queue depth crossed a threshold. The useful signal appeared earlier in the usage record; we just had not collected it reliably.

## 1. Start with the page, then trace backward

Picture the on-call view at 02:17: a route-planning worker is healthy, but the carrier's update endpoint reports unauthorized requests. A key rotation was scheduled for 02:00. The dashboard still shows the old key making calls at 01:55 because the last successful fetch ran at 01:40. That number is not proof of current traffic. It is a stale observation.

The first instrumentation change is to store two times for every sample. `observed_at` is when the provider says the call occurred; `collected_at` is when your fetcher received and committed the record. Add a `source_cursor` or request window so a retry cannot silently create a second count. Then graph event totals by `observed_at`, with a visible freshness badge derived from `collected_at`.

This distinction catches a common false positive. A delayed fetch can make a quiet key look active, while a fast but failed fetch can make a busy key look quiet. Alert on age of the collection job and on the age of the newest event separately.

Short page.

The threshold has a cost, too. Set it at five minutes and a carrier's normal reporting lag may page the team every shift. Set it at an hour and a bad rotation can hide until a driver is waiting at a dock. Pick the threshold from measured reporting latency, and record why it changed in the runbook.

## 2. Can cache, scheduled fetch, and a Node.js dashboard share one timestamp contract?

They can, but only if each layer names the clock it owns. The scheduler owns `run_started_at` and `run_finished_at`. The cache owns an expiry deadline. The store owns commit time. The chart owns the event-time window. Mixing those values produces a convincing picture that cannot answer, \"How fresh is this?\"

For a Node.js service, keep the fetch job boring: acquire a lease, request one bounded time window, upsert by `(account_id, observed_at, request_id)`, and publish a metric for rows accepted. A cache is useful for the chart query, not for deciding whether a credential can be revoked. Cache keys should include the account and the exact window; a five-minute cache over a 24-hour window is still a five-minute-old view of old events.

Use a monotonic job identity. If the scheduler retries after a timeout, the same identity should produce the same write. A unique constraint in the store is the final guardrail, because application-level \"check then insert\" races under load.

Your mileage may vary on polling interval. Some providers expose minute-level usage, others publish hourly aggregates. When the source only gives coarse buckets, show that granularity in the chart instead of inventing precision.

## 3. Make the key rotation a state machine, not a deploy surprise

Seven practical states are enough for most logistics integrations: `planned`, `issued`, `dual-read`, `new-primary`, `draining`, `revoked`, and `verified`. Each transition needs an observable condition. For example, move from `draining` to `revoked` only after the usage chart shows no old-key events for two full source windows and the fetch job is current.

The worker should read the active key set at request time, or from a short-lived in-process cache with an explicit refresh. Do not bake a secret into a container image or a long-lived environment snapshot. The OWASP Secrets Management Cheat Sheet recommends controlled lifecycle operations, least privilege, and auditable access; those controls matter more than the particular secret store.

Here is the shape of an idempotent collector. It is Go because the collector's contract is easier to review when the side effects are explicit; the same contract can sit behind a Node.js scheduler.

```go
type UsageSample struct {
    AccountID    string
    RequestID    string
    ObservedAt   time.Time
    CollectedAt  time.Time
    Count        int64
}

func collect(ctx context.Context, account string, from, to time.Time) error {
    requestID := account + ":" + from.UTC().Format(time.RFC3339)
    samples, err := fetchUsage(ctx, account, from, to, requestID)
    if err != nil {
        return err
    }
    now := time.Now().UTC()
    for i := range samples {
        samples[i].CollectedAt = now
        samples[i].RequestID = requestID
        if err := upsertSample(ctx, samples[i]); err != nil {
            return err
        }
    }
    return nil
}
```

The important part is not the language. It is the stable request ID, bounded window, and upsert. Log the key version as a label, never the secret itself.

## 4. Test the failure modes your chart can hide

A useful test matrix includes delayed source data, a scheduler retry, a cache serving an expired value, a store write that commits after the HTTP timeout, and a rotation that overlaps a deploy. Inject each condition independently. Verify that the chart marks freshness as unknown when collection age exceeds the policy, rather than painting a zero.

For duplicate delivery, assert that two runs with the same request ID produce one logical sample. For clock skew, compare UTC timestamps and reject events outside a permitted window into a quarantine table. A quarantine row is safer than silently shifting an event into the current bucket.

Postmortems should answer three questions: which signal fired, which earlier signal was available, and which owner changes the threshold or runbook. That turns a dashboard from a screenshot into an operational control. During one review, we replayed a 24-hour carrier outage with the collector paused, resumed it twice, and compared the chart with raw access logs; the exercise exposed that our alert was measuring collection success instead of source freshness, so the runbook now names both clocks and the replay is part of release checks.

## 5. Know when this design is the wrong fit

The catch is operational overhead. If the provider offers no usage detail and your service makes only a handful of calls, a full charting pipeline can cost more maintenance than it returns. In that case, use a simple dual-key procedure with access logs and a manual verification query.

This pattern is also unsuitable when policy forbids retaining usage records, even in a private store. Stick with short-lived credentials and provider-side audit exports when retention is constrained. And if the source reports only monthly totals, do not promise a minute-by-minute stale timestamp; document the coarser guarantee and choose rotation windows accordingly.

A small runbook beats a clever dashboard

Before rotation, name the old and new key versions, set the overlap deadline, and confirm the collector's last successful commit. During rotation, watch authorization failures, queue latency, and the old-key series together. After revocation, keep the fetch job running long enough to catch delayed reports, then close the change with the observed freshness and any false-positive pages.

The dashboard is evidence. The runbook is the decision.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control
- https://opentelemetry.io/docs/specs/otel/logs/data-model/

## Further reading

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control
- https://opentelemetry.io/docs/specs/otel/logs/data-model/
