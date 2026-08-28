# Ledger-Safe Node.js Webhook Queues for Minute-Based Cron Processing

Short answer: use the cron trigger only to admit one bounded reconciliation batch per minute, then let a durable queue and rate-limited workers do the payment-provider work. The public webhook should be small, authenticated, and idempotent. It should not wait for reconciliation to finish.

That decision rule matters in fintech because a missed payment record is bad, but a duplicate settlement or refund can be worse. The scheduler tells you when to look. It does not prove that the previous look finished, and it does not make an at-least-once queue behave like exactly-once delivery.

## The incident lesson: admission and completion are different clocks

The bounded production scenario is a nightly payment reconciliation that is released by a minute-based schedule. Each tick asks a public HTTPS endpoint to admit the next slice of provider records. The endpoint authenticates the caller, creates a stable interval ID, writes jobs durably, and returns. Workers then pace requests against the provider's limit and record the outcome.

I have been paged for both sides of this boundary: a missed job and a duplicate delivery. The useful invariant is that a retry may repeat transport, but it must not create a new business operation. A job ID such as `reconcile:20260811T0300Z:invoice-1842` must survive the webhook retry, queue redelivery, worker restart, and provider retry.

Short version: clocks release work; queues hold work; idempotency protects effects.

Do not infer completion from a successful HTTP response. A `202 Accepted` means the batch was admitted, not that a provider record matched or that a ledger entry was posted. Store an admission record with a uniqueness constraint on the interval and operation IDs. A worker claims a job, checks the operation record, calls the provider with that same idempotency key when supported, and marks the result only after the side effect is durably recorded.

## How should a cron trigger feed a public Node.js webhook queue every minute?

Start with the provider budget, not the cron expression. If the provider permits 60 requests per minute, a tick may admit 60 small units only when the worker pool has a shared limiter and enough queue capacity. Two invocations can overlap near a minute boundary; the worker must therefore enforce pacing across instances. A timestamp in the scheduler payload is not a limiter.

The admission endpoint needs a narrow contract:

1. Reject anything except authenticated `POST` requests.
2. Derive the interval from UTC and insert it atomically.
3. Enqueue records with stable operation IDs.
4. Return `202` after durable admission, or return an idempotent success for a repeated interval.

Here is the control boundary in Go. The channel and maps are deliberately small stand-ins for a durable queue and database uniqueness constraints; the important behavior is the identity carried through the retry path.

```go
package main

import (
	"crypto/subtle"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type job struct {
	OperationID string
}

var (
	queue    = make(chan job, 120)
	mu       sync.Mutex
	interval = map[string]bool{}
	done     = map[string]bool{}
)

func admit(secret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		want := "Bearer " + secret
		got := r.Header.Get("Authorization")
		if len(got) != len(want) || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		batchID := "reconcile:" + time.Now().UTC().Format("20060102T1504Z")
		mu.Lock()
		if interval[batchID] {
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			return
		}
		interval[batchID] = true
		mu.Unlock()

		for i := 0; i < 60; i++ {
			queue <- job{OperationID: fmt.Sprintf("%s:%02d", batchID, i)}
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func worker() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for item := range queue {
		<-ticker.C
		mu.Lock()
		if done[item.OperationID] {
			mu.Unlock()
			continue
		}
		log.Printf("reconcile operation_id=%s", item.OperationID)
		done[item.OperationID] = true
		mu.Unlock()
	}
}

func main() {
	secret := os.Getenv("CRON_SHARED_SECRET")
	if secret == "" {
		log.Fatal("CRON_SHARED_SECRET is required")
	}
	go worker()
	http.HandleFunc("/reconcile/admit", admit(secret))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

The example is not a distributed transaction. In a real service, replace the maps with an atomic insert and a durable job store, and make the queue write part of the admission transaction or an outbox flow. The public Node.js endpoint can use the same contract; the language does not change the delivery guarantee. I prefer keeping the code path boring. Boring paths are easier to replay during a reconciliation incident.

## What should rate-limited processing do when retries and duplicates collide?

Treat each failure class separately. A provider `429` is a pacing signal: honor `Retry-After` when supplied, then retry with capped exponential backoff. A network timeout is ambiguous: the provider may have accepted the request, so retry with the same operation ID and consult the provider or your own result store before creating a new effect. A malformed record is not transient; move it to a review path instead of consuming every retry.

The queue should expose enough state to answer an on-call question without reading application logs line by line: ready count, oldest age, in-flight count, retry count, dead-letter count, and completed intervals. Put the interval ID and operation ID in structured logs and tracing fields. Alert on age and failed business outcomes, not just on the cron process being alive.

At-least-once delivery is a normal queue contract. A dead-letter queue is useful for isolating poison messages, but it does not decide if replay is safe. Replaying a payment reconciliation job requires checking the idempotency record and the provider's result semantics. If the team cannot describe that replay procedure in a runbook, the design is not ready for a nightly page.

## Which recovery model fits this minute-based queue?

The choice should follow the failure contract and existing operating model.

| Model | Good fit | Trade-off |
| --- | --- | --- |
| Managed queue plus scheduler | The team needs durable buffering and a separate public admission edge | Pacing, idempotency, and replay remain application responsibilities |
| Kubernetes CronJob plus workers | The workload already lives in a cluster and private reachability is important | A CronJob does not replace queue state or duplicate-effect protection |
| Redis-backed Node.js workers | The service already operates Redis and wants one worker runtime | The team owns persistence, failover, and recovery policy |
| Workflow engine | Reconciliation has dependencies, joins, long waits, or human review | More workflow machinery than a bounded dispatch loop needs |

The catch is scope. This pattern is not suitable when the provider requires a strict global rate limit that the queue cannot coordinate across regions, or when every run is a dependency graph with durable step state. Stick with a workflow engine for that shape. Keep a cluster-native schedule when the endpoint must remain private. Your mileage may vary, but the operational test is stable: can an engineer replay one interval without guessing what already happened?

## Where the design stops working

Minute scheduling gives a release cadence, not an exact start time. Scheduler jitter, queue delay, worker contention, and provider backoff all affect completion. If missing intervals must be reconstructed, maintain a durable cursor and explicitly generate the gaps; do not assume a paused schedule will backfill them.

Also bound the batch. A backlog that grows without a cap can turn recovery into an API storm. Preserve the raw provider response outside the queue when it is large, keep queue messages small, and make retention and deletion policy explicit. For independent consumer groups or long-lived replay, a log-oriented design may fit better than a work queue.

This is the part I would put in the runbook: one interval key, one operation key per business effect, one shared limiter, and one documented replay decision. The implementation can change. Those invariants should not.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/
- https://nodejs.org/api/http.html
- https://www.rfc-editor.org/rfc/rfc9110
