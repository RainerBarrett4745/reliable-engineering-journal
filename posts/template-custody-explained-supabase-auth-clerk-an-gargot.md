# Template Custody Explained: Supabase Auth, Clerk, and NextAuth Password Email

TL;DR: If delivery events cannot arrive by webhook, choose template ownership before choosing an email API. Keep the password-recovery template and send intent in application-controlled code, record one durable message per recovery attempt, and poll delivery state only for diagnosis. Do not make a user wait for that polling loop.

This constraint showed up for me in a nearby e-commerce workflow: sending an order receipt after payment settles. I have been paged by missed jobs and duplicate deliveries. The useful lesson was narrower than "queues are hard." The system that decides an email should exist must retain enough state to reproduce that decision safely, while the template must have one clear owner.

Password recovery has the same operational shape, with a stricter security boundary. The auth system creates a short-lived recovery artifact; the mail path renders and submits it; later delivery observations may explain what happened. Those are three separate responsibilities. Treating a provider's event feed as the source of send intent reverses that order and leaves a polling gap exactly where the user expects immediate action.

## How should Supabase Auth, Clerk, or NextAuth handle custom password email?

A custom password-reset email is not merely a different logo. Ownership determines who reviews copy, who can deploy it, which system knows the recovery URL, and whether a retry can render the same logical message. Before comparing APIs, write down the owner of the subject, text body, HTML body, localization rules, and version identifier. One team and one repository should be accountable for the resulting artifact.

There are two defensible models. In an auth-owned model, the authentication service controls the recovery flow and template lifecycle; customization stays within that boundary. In an application-owned model, the auth layer issues the recovery artifact while application code renders and submits the message. The second model gives product teams tighter review and release control, but it also makes them responsible for secret handling, suppression policy, retries, and observability.

That is real operational load.

This is where names such as Supabase Auth, Clerk, and NextAuth can distract the evaluation. Their presence in an existing stack does not answer the ownership question. Evaluate the exact integration contract available to your deployment, rather than inferring parity from category labels or treating three products as interchangeable.

For the order-receipt system, payment settlement owns the business fact and the communications code owns the rendered receipt. For recovery mail, the corresponding invariant is: the component authorized to initiate recovery records the intent, and only the designated template owner turns that intent into content.

## The incident lesson: events are evidence, not a trigger

My first instinct around queued mail was to inspect the downstream delivery state whenever operators reported a missing message. That helps answer "what happened after submission?" It does not prove that the application ever created the job. Later, duplicate-delivery pages made the other half obvious: retrying an ambiguous request without a stable application key can create a second user-visible email.

So keep two ledgers. The application ledger records the logical message, template version, recipient reference, creation time, attempt count, and terminal application state. The provider-facing ledger records the submission identifier and the latest observed delivery state. A polling cursor belongs with the second ledger. It must never manufacture an entry in the first.

No webhook does not change that rule. Poll delivery observations on a bounded schedule, advance a durable cursor only after persisting a page, and tolerate seeing the same observation again.

Polling can lag.

Recovery initiation cannot depend on the next poll, because the poller is reconciliation machinery rather than the command path.

For a concrete schedule, an initial 15-second interval can be an engineering starting point, not a universal promise. Add bounded backoff when the remote service rejects or throttles requests, and set a maximum reconciliation age based on the provider's documented event-retention window. The exact interval and retention limit must come from the selected API's current contract and your measured traffic; the supplied product names alone establish neither.

## A minimal preventative path

The important code path is the transaction around intent creation, followed by an idempotent worker. This Go sketch leaves the database and email transport behind interfaces so their contracts remain visible. `Claim` must permit only one active worker for a job, and `MarkSent` must preserve the remote identifier. The transport should use a stable idempotency key if its documented API supports one; otherwise the local claim prevents concurrent sends but cannot erase every ambiguous network outcome.

```go
package recovery

import (
    "context"
    "fmt"
)

type Job struct {
    ID              string
    Recipient       string
    RecoveryURL     string
    TemplateVersion string
}

type Store interface {
    CreateOnce(ctx context.Context, dedupeKey string, job Job) error
    Claim(ctx context.Context, jobID string) (Job, bool, error)
    MarkSent(ctx context.Context, jobID, remoteID string) error
    MarkRetryable(ctx context.Context, jobID, reason string) error
}

type Mailer interface {
    Send(ctx context.Context, idempotencyKey, to, subject, textBody string) (string, error)
}

func Deliver(ctx context.Context, store Store, mailer Mailer, jobID string) error {
    job, claimed, err := store.Claim(ctx, jobID)
    if err != nil || !claimed {
        return err
    }

    subject := "Reset your password"
    body := fmt.Sprintf("Use this recovery link: %s", job.RecoveryURL)
    remoteID, err := mailer.Send(ctx, job.ID, job.Recipient, subject, body)
    if err != nil {
        return store.MarkRetryable(ctx, job.ID, err.Error())
    }
    return store.MarkSent(ctx, job.ID, remoteID)
}
```

Do not log `RecoveryURL`, the rendered body, or credentials. The sketch also deliberately omits token generation: that belongs to the authentication boundary, not the email adapter. In production, classify transport errors according to the selected API's documented semantics, cap attempts, and move exhausted jobs to an operator-visible terminal state.

Blind retries are not a recovery policy.

The same worker pattern handles a settled-payment receipt, but its data policy differs. A receipt may be regenerated from durable order facts; a recovery URL should be handled as a sensitive, time-bound value. Shared infrastructure does not imply shared retention.

## Selection tests that survive a provider change

Start with a contract exercise, not a feature grid. Submit one test message, force a client timeout, repeat with the same idempotency key, and record whether the documented behavior lets you distinguish accepted, rejected, and unknown outcomes. Then stop the poller for longer than one interval and verify that it resumes from a durable cursor without skipping or duplicating observations. These tests expose the failure boundaries that matter to an on-call engineer.

Use the following decision criteria:

1. **Template custody:** Can the chosen owner version, review, localize, and roll back the complete message without an untracked edit in a second control plane?
2. **Submission ambiguity:** Does the API document an idempotency mechanism or a lookup that resolves a timed-out request? If not, record the residual duplicate risk explicitly.
3. **Pull reconciliation:** Can delivery observations be listed with stable identifiers, ordering rules, pagination, and a documented retention window? A dashboard is not a polling contract.
4. **Operational evidence:** Can an operator join the application job ID, remote submission ID, template version, and latest observation without exposing the recovery secret?
5. Failure control: Are timeout behavior, retryable responses, throttling, and terminal rejection states documented well enough to encode rather than guess?

Measure queue age, time from intent creation to accepted submission, retry count, jobs with unknown outcomes, and age of the polling cursor. Alert on user impact signals such as old unsent intents and stalled cursor progress. Raw request volume is useful capacity data, but it will not tell you that a particular recovery message never left the application.

## Limitations, trade-offs, and alternatives

Application-owned templates are not a fit when the auth boundary cannot issue recovery artifacts without also controlling delivery, or when the team cannot safely operate the queue, secrets, suppression handling, and on-call path. In that case, the alternative is to keep the template with the auth system and accept its customization boundary. This trade-off reduces flexibility while keeping responsibility in one place. Clear ownership beats nominal flexibility.

Polling is also unnecessary when delivery state has no operational consumer. If support and automated remediation never use it, submission records plus application metrics may be enough. Conversely, if rapid delivery-state reaction is a hard requirement, an integration with a documented push mechanism is a better match than aggressive polling.

The decision rule is short: own the template only where you can own its failures. Record the send intent before transport, make retries converge on one logical message, and keep polling downstream as evidence. That rule works for a password-recovery message and for the order receipt sent after payment settlement, even though their retention and security policies should remain separate.

## Sources

- https://postmarkapp.com/guides/transactional-email-best-practices
- https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API
