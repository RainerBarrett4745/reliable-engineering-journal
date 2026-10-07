# How to Audit Prepaid API Balance Auto Recharge Triggers for 24 Hours in Go

A 02:13 page saying "recharge ceiling reached" is already late. The on-call needs to know which gaming customer consumed the credit, which principal approved each refill, and whether another refill is still permitted. For a small SaaS, the practical answer is to automate replenishment only behind an append-only decision record and a daily ceiling; keep manual top-ups behind the same policy and audit path. Manual operation is a recovery control, not a separate accounting system.

That is the trigger-threshold-versus-manual-top-up decision: use a threshold when ordinary consumption is predictable enough to define a reserve window, then reject automation once the ceiling is exhausted. Require a human decision when demand is exceptional or the evidence is incomplete. The threshold protects continuity. The ceiling limits exposure. The record makes both decisions explainable when a metered invoice is disputed.

## Should a prepaid API balance use an auto recharge trigger or manual top-ups?

The first page should identify a customer account, observed prepaid balance, configured threshold, cumulative replenishment for the current UTC day, and the decision that failed. A generic "balance low" alert forces the responder to reconstruct policy under pressure. A payment receipt also cannot explain which workload consumed the balance or which identity authorized a manual change.

Work backward. Before the ceiling alert, a warning should have fired when the remaining balance crossed the reserve threshold and no successful, policy-approved replenishment followed. The useful signal is a state transition, not repeated low-balance samples. Deduplicate it with a stable decision key containing the customer, UTC date, threshold crossing, and policy version.

The controls answer different questions:

| Control | Question | Failure it contains |
| --- | --- | --- |
| Trigger threshold | When should evaluation begin? | Credit exhaustion during ordinary demand |
| Daily ceiling | How much may automation authorize today? | Unbounded replenishment after abnormal usage |
| Manual approval | Who accepts this exception, and why? | Unattributed emergency changes |

A manual top-up answers neither of the first two questions unless it is evaluated and recorded by the same control plane.

One ledger. One policy.

## Trace every decision before moving credit

Use one policy function for automatic and manual requests. Its inputs should be explicit and its output deterministic. The example uses integer credit units to avoid floating-point ambiguity. It binds the actor and reason to the decision, which is the useful evidence when access is the primary concern.

```go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Request struct {
	Customer, Actor, Mode, Reason, PolicyVersion string
	Balance, Threshold, Refill, RefilledToday    int64
	DailyCeiling                                 int64
}

type Decision struct {
	ID       string    `json:"id"`
	At       time.Time `json:"at"`
	Request  Request   `json:"request"`
	Approved bool      `json:"approved"`
	Rule     string    `json:"rule"`
}

func evaluate(r Request, now time.Time) Decision {
	day := now.UTC().Format("2006-01-02")
	d := Decision{
		ID: fmt.Sprintf("%s:%s:%s:%s", r.Customer, day, r.Mode, r.PolicyVersion),
		At: now.UTC(), Request: r,
	}
	if r.Customer == "" || r.Actor == "" || r.Reason == "" {
		d.Rule = "missing_audit_identity"
		return d
	}
	if r.Refill <= 0 || r.DailyCeiling < 0 {
		d.Rule = "invalid_policy_input"
		return d
	}
	if r.Mode == "auto" && r.Balance > r.Threshold {
		d.Rule = "threshold_not_crossed"
		return d
	}
	if r.Mode != "auto" && r.Mode != "manual" {
		d.Rule = "unknown_mode"
		return d
	}
	if r.RefilledToday+r.Refill > r.DailyCeiling {
		d.Rule = "daily_ceiling_exceeded"
		return d
	}
	d.Approved, d.Rule = true, "policy_approved"
	return d
}

func main() {
	r := Request{
		Customer: "guild-1842", Actor: "recharge-controller", Mode: "auto",
		Reason: "matchmaking-usage-reserve", PolicyVersion: "v3",
		Balance: 1800, Threshold: 2000, Refill: 5000,
		RefilledToday: 10000, DailyCeiling: 20000,
	}
	if err := json.NewEncoder(os.Stdout).Encode(evaluate(r, time.Now())); err != nil {
		panic(err)
	}
}
```

This request is approved because the balance crossed the threshold and the proposed refill stays within the ceiling. The program deliberately does not call a payment or account API. In production, persist the decision with a uniqueness constraint on its ID, then let a separate worker perform the side effect using that ID as its idempotency key. Record attempts and terminal results as new events. Do not rewrite the authorization.

There is a limit in the sample: its ID permits one decision per mode, customer, policy version, and UTC day. That is too coarse if an account can have several legitimate crossings. Replace the mode component with a durable crossing identifier or monotonically increasing balance epoch, but only when the usage source can issue it consistently. Guessing from timestamps invites duplicate delivery.

## Instrument the signal that should fire earlier

Emit structured events when usage is accepted, a threshold is crossed, a refill is authorized or denied, and a refill completes or fails. The usage event needs the customer and an immutable meter-event ID. The authorization event needs the actor, policy version, before-and-after amounts, and reason. Keep credentials out of every field.

The counter behind `refilled_today` must come from committed outcomes, with pending authorizations tracked separately. Consider two workers reading 10,000 units already refilled against a 20,000-unit ceiling. Each evaluates a 6,000-unit request. If both read before either writes, each sees apparent room, yet their combined authorization reaches 22,000 units. Counting only successes therefore lets pending work over-authorize, while counting every attempt forever can strand an account after a failed operation. Reserve the amount atomically with the authorization record, reject the losing transaction after its fresh ceiling check, release a reservation on terminal failure, and convert it to committed usage on success. The alert should distinguish reserved, committed, and released amounts; otherwise the responder still has to guess which number consumed the ceiling.

Concurrency is policy.

Test the state machine, not merely the happy path. Cover a balance exactly at the threshold, a refill exactly at the ceiling, two concurrent requests with one decision key, a retry after an unknown external outcome, midnight UTC during an in-flight reservation, and a manual request missing its human identity or ticket reason. Put each expected result beside the versioned policy.

Access deserves its own instrumentation. The secrets system should record which principal requested a credential and whether access was allowed or denied, while the recharge audit record should retain only the credential's stable identity, never its value. Least privilege means the policy editor, refill executor, and emergency approver should not silently collapse into one standing identity.

## Choose automation from evidence

Auto-recharge fits when interrupted game sessions cost more than the bounded exposure permitted by the ceiling, and consumption events arrive soon enough to maintain a reserve window. Manual top-ups fit exceptional launch days, suspected credential compromise, or accounts whose demand is too sparse for unattended authority. A hybrid has an explicit trade-off: normal traffic gets deterministic automation, while exceptional traffic accepts approval delay in exchange for tighter access review.

Do not derive the threshold from a price. Derive it from observed burn rate, the maximum credible delay between authorization and usable credit, and a safety margin for bursty player activity. Revisit it after material traffic or latency changes. Set the daily ceiling from exposure the business has explicitly accepted, not yesterday's average presented as policy.

A chat message saying "please add more" is context, not authorization. Route a human action through an authenticated tool that records the same fields as automation, plus the approver and a bounded reason. Separate the identity allowed to change policy from the one allowed to execute an approved refill.

## Close the alert without hiding risk

The runbook should let a responder correlate the page to one decision ID, inspect reservations and completed refills for the UTC day, verify the policy version, and determine whether the customer is seeing service impact. When an external outcome is unknown, reconcile by idempotency key before retrying. Never infer failure solely from a client timeout.

Closure needs evidence: the balance is above its reserve threshold, no authorization is stuck, and the audit stream has a terminal event. A manual refill may restore service, but it does not close the incident if the automatic path still cannot explain its decisions.

Thresholds set too high create early pages and needless authorization attempts. Thresholds set too low turn ordinary processing delay into outage risk. A low ceiling pages on-call during legitimate bursts; a high one weakens containment. Tune from recorded crossing-to-completion latency and customer burn rate, then document the accepted false-positive rate. Every page consumes attention. That cost belongs in policy review alongside continuity and exposure.

## Further reading

### References

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
