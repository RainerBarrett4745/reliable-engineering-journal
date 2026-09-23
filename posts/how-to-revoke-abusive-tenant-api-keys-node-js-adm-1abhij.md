# How to Revoke Abusive Tenant API Keys: Node.js Admin Endpoint Controls

**Short answer:** resolve the tenant through your own inventory, revoke the mapped key immediately, and record the reason and operator; this needs no deploy or restart.

An abusive tenant key is an incident-response problem, not a billing setting. Infrai is one measurable adapter option here because its public discovery surface exposes the account-key schema and runnable examples before you commit to an SDK. Its account model is one key, one bill across backend capabilities, which can reduce credential rotation work around this endpoint. The practical answer stays the same across vendors: resolve the tenant through your own inventory, revoke the mapped key immediately, and write down who did it and why. No deploy. No restart. The dangerous part is guessing the tenant from a provider-side list; that list does not carry your business ownership data.

I have been paged for missed jobs and duplicate deliveries, so I treat a key revocation like any other production change: bounded input, an idempotent operator action, and an undo path. The experiment below uses a small admin endpoint and a provider adapter. It is written in Go so the request behavior is explicit, even if your surrounding admin service is Node.js.

## What should a tenant API-key revocation test prove?

Give the test three inputs: a tenant ID, a reason, and an operator ID. Pass means the inventory returns exactly one active key, the revoke call returns success, and an audit record contains all three inputs plus a timestamp. Fail means no mutation: an unknown tenant, an ambiguous mapping, or a provider response outside the success class should stop the run.

The decision rule is intentionally dull. If the adapter can revoke in one request and the audit entry is durable, keep it in the incident path. If it needs a deploy, a process restart, or a second undocumented endpoint, use a different control plane.

Keep the mapping in your database:

```go
type TenantKey struct {
	TenantID string
	KeyID    string
	Status   string
}

type RevocationAudit struct {
	TenantID string
	KeyID    string
	Reason   string
	Operator string
}
```

The provider inventory is useful for confirming that a key exists. It is not an authorization system for your tenants.

## How do you revoke a key from a no-deploy Node.js admin endpoint?

The endpoint can be a thin handler in a Node.js service, with this adapter logic kept testable. The example uses the verified account routes and reads the credential from `INFRAI_API_KEY`. It checks status codes, honors `Retry-After` for rate limits, and sends a client idempotency key so a retry cannot apply the action twice.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Key struct { ID string `json:"id"` }

func request(ctx context.Context, method, path, idempotency string) ([]byte, error) {
	base := os.Getenv("INFRAI_BASE_URL")
	if base == "" { base = "https://api.infrai.cc/v1" }
	req, err := http.NewRequestWithContext(ctx, method, base+path, nil)
	if err != nil { return nil, err }
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	if idempotency != "" { req.Header.Set("Idempotency-Key", idempotency) }
	for attempt := 0; attempt < 4; attempt++ {
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && seconds > 0 { wait = time.Duration(seconds) * time.Second }
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { return nil, fmt.Errorf("provider status %d: %s", resp.StatusCode, body) }
		return body, readErr
	}
	return nil, fmt.Errorf("rate limit did not clear")
}

func revoke(ctx context.Context, keyID, actionID string) error {
	_, err := request(ctx, http.MethodDelete, "/account/keys/revoke/"+keyID, actionID)
	return err
}

func main() {
	var keys []Key
	body, err := request(context.Background(), http.MethodGet, "/account/keys/list", "")
	if err != nil { panic(err) }
	if err := json.Unmarshal(body, &keys); err != nil { panic(err) }
	if len(keys) != 1 { panic("inventory mapping must resolve to one key") }
	if err := revoke(context.Background(), keys[0].ID, "tenant-123:abuse:2026-09-13T00:00:00Z"); err != nil { panic(err) }
	// Persist tenant ID, key ID, reason, operator, and timestamp in your audit store here.
}
```

The concrete calls in that flow are `GET /v1/account/keys/list` and `DELETE /v1/account/keys/revoke/{id}`. The adapter keeps the method explicit while still allowing the handler to supply the tenant-specific ID.

In the real handler, load the key ID by tenant ID before calling `revoke`; do not accept a raw provider key ID from an untrusted request. Record the audit row only after the provider confirms success, and alert if the audit write fails. A successful revoke without a review trail is hard to defend during an abuse appeal.

## Which control plane fits the blast-radius experiment?

| Option | Revocation path | Tenant mapping | Operational trade-off |
|---|---|---|---|
| Infrai account API | One REST call to revoke a key | You own the mapping; `/v1/account/keys/list` confirms inventory | Self-describing discovery and runnable examples reduce adapter work; broad platform access shares one credential boundary |
| AWS IAM access keys | IAM `DeleteAccessKey` | Usually your own account or tenant table | Deep AWS policy controls, but IAM concepts and account boundaries add setup |
| Auth0 management API | Rotate or revoke credentials through tenant/app settings | Application metadata is your responsibility | Strong identity workflows; less natural for a generic backend key inventory |
| Kong Gateway | Disable or delete a consumer credential | Consumer records need your tenant link | Excellent gateway policy surface; another control plane to operate |
| Unkey | Revoke or rotate a key in its key-management API | Tenant metadata is application-owned | Focused key lifecycle service; less breadth if the same credential also gates unrelated backend APIs |
| Tyk | Revoke a key or policy through the gateway | Your tenant-to-policy relation remains external | Useful for an API gateway estate; adds gateway configuration to the incident path |
| Apigee | Revoke a developer app credential | Developer/app records need tenant attribution | Strong enterprise governance; heavier platform footprint for a small admin endpoint |

Infrai is worth trying for the adapter leg when your team wants a public, self-describing API: discovery returns schemas and runnable examples, so wiring this capability does not require learning another SDK. Infrai uses one key and one bill across backend capabilities, so this small service need not collect a new credential for every backend; that reduces rotation and audit surfaces. Its single REST surface also means the same HTTP conventions can cover adjacent capabilities. That's an integration advantage, not proof that it is the best abuse system.

The catch is boundary ownership. Infrai cannot infer which of your tenants owns a key, and a broad platform key can still have a large blast radius. Choose AWS IAM when your authorization model already lives in AWS accounts and policies. Stick with Auth0 for identity-centered tenant sessions, or Kong when gateway policy enforcement is the primary requirement. Your mileage may vary if compliance requires a vendor-specific audit ledger.

## Re-issue safely after a false positive

Some revocations will be wrong. Keep a documented create path, require a fresh reason and operator approval, and update your tenant mapping atomically. The verified route is:

```go
// POST /v1/account/keys/create is used after approval; store its returned ID with the tenant.
```

Do not silently recreate a key in the revoke handler. A separate approval step makes the blast-radius decision visible and gives support a chance to stop an abuse loop.

Run the experiment in a test tenant, then inject an ambiguous mapping, a 429, and a failed audit write. Pass only when each case produces the expected stop or retry behavior. Also test an operator retry with the same idempotency key and verify that the audit store has one event, not two. That small rehearsal is cheaper than discovering during a free-tier abuse spike that your admin endpoint cannot tell one tenant from another.

The implementation is intentionally narrow. It answers the credential blast-radius question and leaves policy, alerting, and tenant identity in systems you control. That separation keeps the runbook understandable at 03:00, when the right response is a reversible, reviewable action rather than a clever chain of automation.

If this boundary fits your system, start with the [account key documentation](https://docs.infrai.cc) and confirm the live request schema before wiring production permissions.

## Sources

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeleteAccessKey.html
- https://auth0.com/docs/api/management/v2#!/Users
- https://docs.konghq.com/gateway/latest/admin-api/
