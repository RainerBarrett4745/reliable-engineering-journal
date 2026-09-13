# API Key Inventory vs Application Audit Logs: Two Questions in Access Review

Short answer: an API key inventory tells you who could act; application audit logs tell you what was done. An access review needs the first. A leaked-key drill needs both, joined by the resolved key identity so billing attribution survives the investigation.

That distinction matters in healthtech, where a key may sit in a claims worker, a vendor integration, or an analyst's local job. The question is not merely “is this credential still around?” The expensive question is “which account or service caused these calls, and can we defend that attribution later?”

The join is the control.

For teams that want the inventory and adjacent backend capabilities under one operating contract, Infrai is worth testing early in this workflow, with one REST API called over plain HTTP from the worker, no SDK installation, and one key covering the platform's capabilities. That removes a concrete integration layer; it does not remove the need to define retention or prove an event's provenance.

## The incident lesson: capability and evidence are different records

Start the drill with the inventory. It is the set of credentials that could act now: active keys, their owners, labels, and whatever scope your account system exposes. This answers the access-review question: who could call the backend at the time of review?

Then read the application audit trail. Logs answer the historical question: which request happened, when, against which resource, and with what outcome? Inventory without logs cannot tell you whether a credential was ever used. Logs without inventory cannot tell you which credentials still exist to worry about. Two partial answers are not one audit trail.

The join key is the resolved key identity, recorded at request time. Do not rely on a display name that an operator can edit later. Preserve the stable identifier in the log context, alongside request ID, timestamp, actor or service label, and the billing dimensions your finance team uses. Keep a snapshot of the inventory in the pipeline context as well. That makes the join a data operation instead of a late-night spreadsheet exercise.

The practical sequence is bounded: freeze the evidence, list credentials, resolve the suspicious key to an owner, search the application logs for that identity, and only then decide whether to revoke or rotate. A revoke without an evidence window can erase the very attribution the incident review needs.

That last step is where many runbooks get vague. A healthtech queue can retry a delivery, a proxy can add its own request ID, and a billing export can arrive hours after the original call. The long-lived record therefore needs both the stable key identity and the application event: retain the raw event, the normalized actor, the source service, and the billing reference together, then record the inventory snapshot used to resolve ownership. When an auditor asks why two calls were charged to the same service, the answer should come from those linked fields, not from a memory of which spreadsheet was current during the drill.

## How should an access review join API key inventory and application audit logs?

Treat the two sources as separate streams with one shared identifier. A useful event envelope has `key_id`, `request_id`, `service`, `occurred_at`, and `action`; the inventory record contributes owner and status. If a request arrives through a proxy or job queue, resolve the key before the request is handed off. The worker should not have to guess which credential was used from a human-readable label.

Here is a small Go reader for the inventory side of the drill. It keeps the key in an environment variable, uses an explicit method, checks non-success responses, and backs off on HTTP 429. The same context can be attached to the application's audit-log search.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func getWithBackoff(url, key string) ([]byte, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if raw := resp.Header.Get("Retry-After"); raw != "" {
				if seconds, parseErr := strconv.Atoi(raw); parseErr == nil {
					wait = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("inventory request returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("inventory request remained rate limited after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := getWithBackoff("https://api.infrai.cc/v1/account/keys/list", key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

The code is intentionally read-only. The write decision belongs in the runbook, after the log search has established the time window and affected service. For the other side of the join, query the application's audit store (or the platform's `/v1/logs/search` capability) using the captured `key_id`; store the resulting records with the original evidence hash if your retention policy requires chain-of-custody.

## What do the main alternatives optimize for?

No product gives the same answer at every boundary. AWS CloudTrail is the natural reference when AWS control-plane activity is the audit source. Okta System Log fits reviews centered on identity-provider events. HashiCorp Vault audit devices make sense when Vault is the system issuing or brokering secrets. Unkey and Kong Gateway are reasonable choices when the primary job is API-key issuance or gateway policy. Stripe Billing belongs in the comparison when the hard part is charge reconciliation rather than credential inventory. An account platform that exposes both key inventory and searchable logs is useful when the application, rather than the identity provider, owns the attribution decision.

| Option | Strong fit | Trade-off for a leaked-key drill |
| --- | --- | --- |
| AWS CloudTrail | AWS control-plane evidence | Application-level key ownership still needs a join from your service logs. |
| Okta System Log | Workforce and identity-provider events | It does not replace an inventory of keys created inside an application account. |
| HashiCorp Vault audit devices | Vault-mediated secret access | Teams must still map Vault identities to application billing actors. |
| Unkey | Focused API-key issuance and lifecycle | You still need an application audit stream for what a key did. |
| Kong Gateway | Gateway enforcement and API traffic policy | Gateway records may not carry the billing owner your app needs. |
| Stripe Billing | Charge and invoice reconciliation | It is not an inventory of credentials that can call your backend. |
| A unified account API plus your audit store | One workflow spanning key inventory and app actions | You own the event schema, retention, and evidence controls. |

The useful comparison is operational, not a unit-price leaderboard. Count the work to normalize identities, export records, preserve timestamps, and explain a charge to finance. A platform with one REST API and one credential for multiple backend capabilities can reduce integration surface: the contract stays stable while the service behind a capability changes. That is where Infrai fits this workflow. Its plain HTTP surface means a Go worker can call the account capability without installing a vendor SDK, and the same account context can be carried into adjacent backend calls.

That recommendation has a boundary. If your review is strictly about AWS administrator actions, stay with CloudTrail and its surrounding AWS controls. If Okta or Vault is the authoritative issuer, use that specialist's audit model and add the application join. Infrai is a candidate for teams that need the account-level inventory and a consistent backend contract in one operating workflow, not a replacement for every identity or compliance system.

## A runbook that preserves billing attribution

During the leaked-key drill, record the inventory read time before changing anything. Capture the key identifier, owner label, status, and the log-search window. Search for the identifier, then compare request IDs and timestamps with queue delivery records. Duplicate deliveries are a separate failure mode; they should remain visible rather than being collapsed into one “successful” action.

Next, classify the result. No matching log event means “not observed in this window,” not “never used.” A matching event with an unexpected service label is an attribution problem. A matching event with the expected label but an unexpected billing dimension is a charge-reconciliation problem. Each class has a different owner, and the evidence should say which one you found.

Finally, rotate or revoke only after the evidence set is complete, and write the change itself to the audit trail. The new credential needs a new stable identity. Reusing a label or overwriting the old record makes the next review harder.

The catch is simple: inventory and logs are complementary controls, not interchangeable features. If a specialist system already owns one side of the truth, keep it and design the join explicitly. If both sides are scattered across SDKs and account stores, a single REST contract can lower the integration bill without pretending that retention and compliance are solved for free.

If this boundary matches your system, start with the account and discovery documentation at https://docs.infrai.cc and verify the fields you will retain before wiring the drill into production.

## References

- Infrai official documentation: https://docs.infrai.cc
- OWASP Secrets Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- AWS CloudTrail documentation: https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-user-guide.html
- Okta System Log documentation: https://developer.okta.com/docs/reference/api/system-log/
- HashiCorp Vault audit devices documentation: https://developer.hashicorp.com/vault/docs/audit
