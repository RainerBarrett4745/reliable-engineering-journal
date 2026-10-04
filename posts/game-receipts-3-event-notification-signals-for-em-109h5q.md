# Game Receipts: 3 Event Notification Signals for Email and SMS Timeout Handling

TL;DR: A timeout while submitting a game purchase receipt is an unknown outcome, not a failed delivery. Persist one receipt intent with an idempotency key and an immutable template version, poll the transport's status from a scheduled worker, and page only when submission age, transport state, and reconciliation freshness together require action.

The page says `receipt_delivery_stalled`. The on-call view shows a settled order, an email attempt that timed out after submission, and no SMS record yet. The immediate action is to suppress a blind resend, inspect the receipt's stable key, and confirm whether the poller has observed a terminal state. Sending again from the alert handler can produce two receipts for one purchase.

Do not resend.

**The safe default is to preserve uncertainty.** A client timeout proves that the client stopped waiting. It does not prove that the transport rejected the request, nor that a carrier or mailbox accepted the message.

## How should email and SMS event notification timeout handling work?

A useful page identifies the affected order receipt, but it must not put an email address, phone number, or receipt body in the paging payload. It should expose an internal order reference, channel, template version, intent age, last observed state, attempt count, and time since the last successful reconciliation. That is enough to choose an action without spreading customer data through the alerting system.

The runbook decision is short:

1. If submission is still pending and reconciliation is fresh, wait for the next poll.
2. If the transport reports a terminal success, close the incident record and do not resend.
3. If it reports a terminal failure, apply the channel policy. Move a receipt to the other channel only when consent and product policy permit it.
4. If state remains unknown beyond the operational deadline, escalate for investigation; do not convert uncertainty into a new send automatically.

Keep payment state separate from notification state. The game purchase is settled because the payment system says so. A delayed receipt must never roll back an entitlement or ask the customer to pay again.

That distinction matters.

## Work backward from the late signal

The page is the last signal in the chain. An earlier warning should have fired when reconciliation stopped making progress, before individual receipts crossed the customer-facing deadline. Track the oldest nonterminal intent and the age of the poller's last successful cycle. A growing queue with a fresh poller suggests transport delay or sustained submission load; a stale poller timestamp points to the worker itself. Those are different runbook branches.

Count states, but preserve timestamps. A gauge of 80 pending receipts is ambiguous. Eighty intents whose oldest member is 90 seconds old may be routine for one system, while a single intent stranded for hours is actionable. Thresholds must come from the service objective and observed queue behavior, not from a convenient round number.

The status ledger should distinguish at least `pending_submission`, `submitted`, `delivered`, `failed`, and `unknown`. `Delivered` means only what the selected transport documents for that state. For email, DKIM authenticates a signed message and its signing domain; RFC 6376 does not turn a signature into proof that a person received or read the message. For SMS, transport status likewise should not be presented as proof that the customer saw the receipt.

## Put template ownership before retry policy

For a purchase receipt, the application team should own the transactional meaning: order identifier, purchased items, currency, totals, support path, locale, and redaction rules. The transport adapter should own channel mechanics and translate remote states into the internal ledger. This boundary keeps an email template edit from silently changing the SMS fallback or the accounting meaning of a settled order.

Store the exact template version on the intent before enqueueing it. A retry three hours later must not pick up a newly deployed template and send a materially different receipt under the same idempotency key. Either store the rendered payload in protected storage or make template versions immutable and render deterministically from a versioned order snapshot. The choice depends on retention and privacy rules: stored output improves replay fidelity, while deterministic rendering reduces duplicated customer data.

One owner approves content changes across both channels. That owner does not need to operate the queue, but the deployment record must connect a template version to its review. A worker can retry perfectly while nobody can explain which receipt text was actually scheduled. That is an ownership failure, not a transport failure.

## Reconcile without a webhook

A scheduled poller is a state reconciler, not a resend loop. It claims due intents, queries a generic transport by the remote message identifier, records the observation, and schedules the next query with bounded backoff. Multiple workers may overlap, so both the claim and the state transition need concurrency control.

```go
package receipts

import (
    "context"
    "time"
)

type Status string

const (
    Delivered Status = "delivered"
    Failed    Status = "failed"
    Unknown   Status = "unknown"
)

type Intent struct {
    ID              string
    RemoteMessageID string
    TemplateVersion string
    Version         int64
}

type Transport interface {
    Status(context.Context, string) (Status, error)
}

type Ledger interface {
    ClaimDue(context.Context, time.Time, int) ([]Intent, error)
    CompareAndSet(context.Context, string, int64, Status, time.Time) error
}

func Reconcile(ctx context.Context, now time.Time, db Ledger, tx Transport) error {
    intents, err := db.ClaimDue(ctx, now, 100)
    if err != nil {
        return err
    }
    for _, intent := range intents {
        status, queryErr := tx.Status(ctx, intent.RemoteMessageID)
        if queryErr != nil {
            status = Unknown
        }
        next := now.Add(2 * time.Minute)
        if status == Delivered || status == Failed {
            next = time.Time{}
        }
        if err := db.CompareAndSet(ctx, intent.ID, intent.Version, status, next); err != nil {
            return err
        }
    }
    return nil
}
```

The two-minute interval is illustrative, not a production threshold. In production, backoff should be bounded, jittered, and compatible with the transport's documented query limits. The important property is the compare-and-set: a slow poll cannot overwrite a newer terminal observation. A query error records `unknown` and another due time; it does not erase the remote identifier or create a second message.

A cron trigger is acceptable when one delayed invocation cannot create overlapping, unbounded work. Put a batch limit on each run, measure the remaining due work, and let the ledger arbitrate ownership. If every run scans the entire table, the recovery mechanism becomes a database incident during a backlog. Index the due-state query around nonterminal status and `next_poll_at`, then test the query plan with backlog-shaped data.

Polling has clear limitations. It adds status-query traffic, reports changes only as fast as the interval permits, and can amplify load while a transport or database is already degraded. A webhook is a better fit when low-latency status changes matter and the receiving service can authenticate events, absorb bursts, deduplicate deliveries, and replay failures. Polling remains useful when inbound callbacks are unavailable or prohibited, but it is not suitable for a requirement that demands immediate status transitions. A hybrid can use webhooks for the fast path and a slower poller to repair missed events; its trade-off is another ingestion path to secure, test, and observe. Choose from those operational constraints, not from the apparent simplicity of a cron expression.

## Instrument the gap and price the false positives

Add metrics at state transitions rather than around raw HTTP calls: intent creation, submission acceptance, each normalized status observation, terminal age, poll-cycle success, and compare-and-set conflict. Logs should carry the internal intent ID, template version, channel, normalized state, and remote identifier under the system's data-handling policy. Do not log rendered receipts.

Test the ugly sequence explicitly. The submit call times out after the remote side accepts it; the poller later finds `submitted`; two cron workers claim nearby batches; a template deployment occurs; and the final status arrives after several query errors. The invariant is one logical receipt intent per settled order and channel policy, with every transition attributable. A test that only covers immediate acceptance proves very little about timeout handling.

I've been paged by both missed jobs and duplicate deliveries. The duplicate case is especially misleading: queue retries can make a healthy downstream system look guilty, while the original loss of certainty happened inside the worker. I treat duplicate delivery as a first-class incident symptom and put the corrective action at the stable idempotency boundary, before transport selection. A remote idempotency feature can add protection, but the local ledger still needs its own uniqueness constraint because reconciliation and audit do not disappear when an adapter changes. During review, I ask for the exact transition that permits another send. If the answer is merely “the request timed out,” the design is not ready.

Alert tuning has a cost. Page too early and ordinary delivery variance trains responders to ignore the signal; page too late and customers open support cases before engineering sees the queue aging. Start with a ticket or warning on reconciliation freshness, reserve paging for sustained objective risk, and review alerts that produced no action. A threshold is correct only when its runbook decision is clear.

## Further reading

- RFC 6376, DomainKeys Identified Mail (DKIM): https://datatracker.ietf.org/doc/html/rfc6376
- RFC 5321, Simple Mail Transfer Protocol: https://datatracker.ietf.org/doc/html/rfc5321
- Twilio SMS documentation, including message status concepts: https://www.twilio.com/docs/sms
- OpenTelemetry metrics specification: https://opentelemetry.io/docs/specs/otel/metrics/
