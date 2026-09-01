# Support Reservation Expiry: Batch Enqueue Jobs in Node.js Background Processing

Short answer: enqueue one expiry job per reservation, make the expiry operation idempotent, and let a rate-limited worker own delivery retries. For customer support systems, this is safer than putting a whole batch in one message because a delayed or repeated delivery must not reopen a reservation that an agent has already handled.

Keep the scheduler boring. The important decision is the delivery guarantee: a queue normally gives you at-least-once delivery, so the application must make repeated work harmless. A cron trigger can publish due work, but it should not be the place where reservations are expired. That belongs in a worker with a bounded claim, a database transaction, and an audit record.

Ship one job.

## What should batch enqueue jobs protect in a reservation workflow?

The failure signal is easy to miss. An agent sees a reservation as held, a customer gets told that it is available, and the expiry process is still sitting behind a slow import or a provider rate limit. Queue depth alone will not tell you whether the customer-facing state is stale. Measure the age of the oldest due reservation and the time from enqueue to completion.

Use one message for one reservation. A batch publisher may send many messages in one request, but each message should contain a stable reservation ID, the hold-window version, and a due-at timestamp. Do not copy the entire reservation into the message. The database is the current record; the message is a durable request to check it.

The worker must re-check state before changing it. A reservation can be extended, cancelled, or confirmed after the scheduler placed the message. If the stored version no longer matches, acknowledge the job as stale and do nothing. If the reservation is still held and its due-at time has passed, update it and write an expiry event in the same transaction. The event gives support staff something inspectable when a customer disputes the timing.

That order matters. A retry after a worker timeout is expected behavior, not a rare edge case.

## How can Node.js background jobs handle batch publishing without duplicate expiry?

The publishing service can be written in Node.js, Go, or any language that can call the queue. The queue abstraction is the useful boundary. Here is a Go sketch for the part that matters: a bounded batch, a stable key for each reservation, and a worker that claims the state transition before acknowledging the message. The queue client is deliberately represented by a small interface so the delivery policy stays testable.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

type Reservation struct {
	ID      string
	Version int64
	DueAt   time.Time
	State   string
}

type ExpireJob struct {
	ReservationID string
	Version       int64
	DueAt         time.Time
}

type Queue interface {
	PublishBatch(context.Context, []ExpireJob) error
	Receive(context.Context) (ExpireJob, error)
	Ack(context.Context, ExpireJob) error
}

type Store interface {
	ExpireIfDue(context.Context, string, int64, time.Time) (bool, error)
}

func publishDue(ctx context.Context, q Queue, due []Reservation, batchSize int) error {
	for start := 0; start < len(due); start += batchSize {
		end := start + batchSize
		if end > len(due) {
			end = len(due)
		}
		jobs := make([]ExpireJob, 0, end-start)
		for _, r := range due[start:end] {
			jobs = append(jobs, ExpireJob{
				ReservationID: r.ID,
				Version:       r.Version,
				DueAt:         r.DueAt,
			})
		}
		if err := q.PublishBatch(ctx, jobs); err != nil {
			return fmt.Errorf("publish reservations %d-%d: %w", start, end-1, err)
		}
	}
	return nil
}

func runWorker(ctx context.Context, q Queue, store Store, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			job, err := q.Receive(ctx)
			if err != nil {
				log.Printf("receive: %v", err)
				continue
			}
			expired, err := store.ExpireIfDue(ctx, job.ReservationID, job.Version, job.DueAt)
			if err != nil {
				log.Printf("leave unacknowledged %s: %v", job.ReservationID, err)
				continue
			}
			if !expired {
				log.Printf("stale or already handled: %s", job.ReservationID)
			}
			if err := q.Ack(ctx, job); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("ack %s: %v", job.ReservationID, err)
			}
		}
	}
}
```

`ExpireIfDue` is not a read followed by a later write. It should be one conditional database transition, protected by the reservation ID and version. A second delivery then finds no matching held row, records no second customer-visible action, and can be acknowledged. The sample leaves a storage failure unacknowledged so the queue can redeliver it; the production retry policy should add a dead-letter path and an alert after a bounded number of attempts.

For a large customer-support import, persist a run ID and chunk number before publishing. Repeating a chunk must reuse deterministic reservation IDs and produce the same logical jobs. The publisher should stop when a batch call fails, rather than advancing its cursor as if the queue accepted data. This is where many “duplicate” incidents actually begin: the import cursor is committed, the publish response is lost, and the operator reruns the next chunk without knowing which work was accepted.

The minimum job contract is small:

| Field | Purpose | Failure it prevents |
| --- | --- | --- |
| reservation ID | Locate current state | Expiring a copied, stale snapshot |
| hold-window version | Reject an extension | Releasing a reservation after it was renewed |
| due-at timestamp | Bound the decision | Applying an old schedule to a new hold |
| run ID | Reconcile a batch | Losing the cursor after a publish timeout |

## Where do delivery guarantees meet rate limits and time windows?

The hold window is a business rule; the worker pace is an operational rule. Keep them separate. A reservation may be due now even when the worker is deliberately sending only a small number of state changes per second. Process due work in order of due-at time when possible, but expose lateness instead of silently increasing concurrency.

Retries need a ceiling. For transient rate-limit responses, exponential backoff spreads attempts over time; the delay should respect a server-provided retry interval when one exists. Add jitter so several workers do not wake on the same boundary. A permanent validation error should go to a reviewable failure stream, not spin forever.

There is a practical trade-off here. A faster worker lowers expiry lag but increases database and downstream load. A slower worker protects those dependencies but may violate the support team's promised hold window. Set the rate from measured capacity, reserve headroom for retries, and alert on both oldest-due age and attempt count. Three words: watch the lag.

Measure it.

The catch is that at-least-once delivery does not mean exactly-once customer behavior by itself. The uniqueness constraint, version check, and transaction are what make repeated messages safe. If those controls are unavailable, this design is not suitable for an irreversible side effect; use a workflow or datastore with an explicit idempotency primitive, and keep the reservation state transition there.

## What should verification and rollback look like after publishing?

Verify every boundary with counts tied to a run ID: reservations selected, jobs accepted, jobs received, state transitions committed, stale jobs skipped, retry attempts, and unacknowledged jobs. A dashboard showing zero queue depth is not proof that all reservations expired. Compare the oldest due reservation with the latest successful transition, and sample the audit trail from the support console.

Test the awkward paths before changing the worker rate. Deliver the same job twice. Extend the hold after enqueue. Confirm the reservation just before the worker claims it. Kill the worker after the transaction and before acknowledgement. The expected result is one state transition, an audit event, and a redelivery that becomes a no-op.

Rollback starts with the publisher. Stop selecting new reservations, preserve the run cursor, and leave already accepted jobs available for the previous consumer. If the message schema changes, deploy a consumer that understands both versions before switching writers. Do not delete the backlog to make a graph look healthy; it removes the evidence needed to reconcile support cases.

This runbook is a poor fit when the business needs a multi-step join, long-lived replay history, or private event fan-out managed by the queue itself. Use a workflow engine or a broker with those primitives when they are requirements. Stay with a small queue and a database transaction when the job is only “check this reservation, expire it once, and record why.”

I'm not sure one batch size will travel cleanly between queue implementations. Your mileage may vary with payload shape, database contention, and worker latency. Start with a conservative limit, measure staging behavior, and change it from observed lag rather than from a guessed throughput number.

## References

- https://vercel.com/docs/cron-jobs
- https://en.wikipedia.org/wiki/Exponential_backoff
