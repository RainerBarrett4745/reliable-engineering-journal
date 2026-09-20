# Next.js API Routes and Server Actions Error Tracking: Cohort Rollback Signals

The page fires: checkout failures are rising during an e-commerce experiment, but the on-call view mixes control and treatment tenants. Short answer: capture Next.js API route and server action exceptions with release, environment, tenant, and trace ID; compare affected operations within each cohort before deciding to roll back. An exception counter alone cannot establish that the experiment caused the change. Keep a separate signal for scheduled jobs that never ran, and use an alerting system to page on the cohort signal.

Work backward from the page. A checkout operation can fail, retry, and produce two exception events; counting those as two independent failed purchases inflates the apparent rollback risk. Deduplicate by an application-owned operation ID when calculating the failure rate, and compare it with attempted operations in the same cohort and time window. Store path and method on the error event so a trace ID can take the operator to related logs. Do not put unbounded tenant IDs in Prometheus metric labels.

## What should have fired before the checkout page?

A sustained increase in deduplicated failures for the treatment cohort is a better early signal than a sitewide error total. Require enough attempts for the comparison to mean anything; a single failed operation in a tiny cohort can otherwise dominate the rate. The threshold and observation window must be calibrated against actual traffic, not copied from a vendor example.

An exception tracker cannot detect a job that never started. If a scheduled cohort aggregator fails silently, a heartbeat service such as Healthchecks should watch for the missing run. Keep its missing-job alert distinct from a checkout exception alert: they call for different first actions.

## How should Next.js API routes and server actions capture errors?

Wrap the operation boundary in API routes and server actions, capture the exception once with the release and environment, then rethrow or return the application's intended failure response. Background workers need the same operation ID and tenant context. Middleware-adjacent code can also contribute errors, but a Node.js server-side example does not prove that an error client works in an Edge runtime. Check the chosen runtime and deployment target before relying on an Edge capture path.

The instrumentation change is small in principle: persist the error and its request metadata, then let the cohort evaluator read its own deduplicated operation counts. Keep sensitive request bodies out of the event. If a capture request fails, the checkout error still needs its ordinary handling path; the tracking call should not redefine the transaction outcome. A plain REST API is useful at this boundary because any service that can send an HTTP request can report an error without installing a client SDK or coordinating its version across workers. Infrai offers that approach for server-side capture, plus a public discovery schema that lets an integration verify request fields before deployment. Its one-key API also covers error search and group detail, reducing credential handling for a small internal triage view. The public discovery surface describes 295 routes across 20 modules, but breadth doesn't supply a missing alert evaluator. Those conveniences do not turn it into an alerting service.

Retries count twice unless the operation identity survives them.

For a Go worker, keep the transport narrow. The example sends an already-validated JSON event body supplied through `ERROR_EVENT_JSON`; obtain its required fields from discovery rather than assuming a capture schema. `OPERATION_ID` must be stable for retries of the same operation. A 429 backs off, while other non-success responses remain visible to the caller.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("OPERATION_ID")
    body := []byte(os.Getenv("ERROR_EVENT_JSON"))
    if key == "" || id == "" || !json.Valid(body) {
        fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY, OPERATION_ID and valid ERROR_EVENT_JSON")
        os.Exit(2)
    }
    ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
    defer cancel()
    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        base := "https://api." + "infrai" + ".cc/v1"
        req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/errors/capture", bytes.NewReader(body))
        if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", id)
        resp, err := client.Do(req)
        if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
        result, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { fmt.Fprintln(os.Stderr, readErr); os.Exit(1) }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 { return }
        if resp.StatusCode != http.StatusTooManyRequests {
            fmt.Fprintf(os.Stderr, "capture HTTP %d: %s\n", resp.StatusCode, result)
            os.Exit(1)
        }
        delay := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
            delay = time.Duration(seconds) * time.Second
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            fmt.Fprintln(os.Stderr, ctx.Err())
            os.Exit(1)
        case <-timer.C:
        }
    }
    fmt.Fprintln(os.Stderr, "capture rate limit persisted")
    os.Exit(1)
}
```

The idempotency key protects the capture retry; it does not deduplicate the checkout itself. The application still owns that decision. In a server action, invoke the same capture behavior with framework-compatible transport and explicit failure handling, rather than treating this Go worker as a Next.js runtime example.

## Which tool should own the investigation?

| Option | Integration | Initial work | Good fit | Boundary |
| --- | --- | --- | --- | --- |
| Sentry | Next.js SDK | Configure framework instrumentation and releases | Browser and server error triage with source maps | Cohort rollback math still belongs in application telemetry |
| Datadog | SDK and agent-based integrations | Connect telemetry and configure monitors | Existing monitoring and trace investigations | More setup than a narrow server-error ledger |
| OpenTelemetry with Prometheus | Instrumentation libraries and exporter | Own collection, aggregation, and alert rules | Teams with an established metrics stack | Error-group triage needs additional tooling |
| Infrai | Plain REST API | Send server events and build the evaluator | Lightweight server-error view under one key | No native paging, trace span tree, decoded source maps, or session replay |

Sentry is the better fit if the deciding evidence is a decoded browser stack trace or session-level client debugging. Datadog is stronger where monitor configuration and distributed trace investigation already drive the on-call workflow. OpenTelemetry and Prometheus suit a team willing to own the pipeline and alert rules. Infrai can store API route, server action, and worker errors tagged with release and environment, and its search and group-detail operations can supply recent production errors and resolution status to an internal page. It does not provide a native alert or notification route: poll the query API with an external evaluator if this is the chosen error store. A shared trace ID correlates errors and logs; it is not a span-tree query.

The limitation is concrete: Infrai does not support browser session replay, source-map decoding, native paging, or distributed span-tree queries. It is not suitable as the only tool when any of those determines a rollback. Choose Sentry for frontend debugging or Datadog for native monitors and tracing. Verify Edge compatibility separately rather than borrowing a Node.js deployment result.

## What happens when the threshold is wrong?

The runbook first compares treatment and control attempts for the same window and release, then checks distinct failed operation IDs, captured events, and correlated logs. If the treatment cohort diverges, an operator can assess rollback against actual affected checkouts. If both cohorts deteriorate, investigate a shared dependency before attributing the fault to the experiment. The decision rule is conditional, not a promise that an error tracker can identify causality.

Set the threshold too low and retry bursts create false pages and unnecessary reversals. Set it too high and the checkout page arrives first. Record both kinds of miss after an incident and adjust the window using observed traffic. No invented universal percentage replaces that review.

## References

- Prometheus instrumentation: https://prometheus.io/docs/practices/instrumentation/
- Sentry Next.js documentation: https://docs.sentry.io/platforms/javascript/guides/nextjs/
- Datadog monitoring documentation: https://docs.datadoghq.com/monitors/
- OpenTelemetry JavaScript documentation: https://opentelemetry.io/docs/languages/js/
- Healthchecks documentation: https://healthchecks.io/docs/
