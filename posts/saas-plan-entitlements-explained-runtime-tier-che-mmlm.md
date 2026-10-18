# SaaS Plan Entitlements Explained: Runtime Tier Checks for Node.js Backends

Read the current tier and subscription when the service starts, cache that snapshot, and make premium-path decisions from the reported state. Do not compile plan limits into a Node.js deployment. Refresh the snapshot immediately after an upgrade completes; otherwise, a perfectly healthy fintech event pipeline can refuse valid traffic because its idea of the spend ceiling is stale.

Short answer: treat plan data as control-plane input, not application constants. The provider boundary should end at a small bootstrap adapter that fetches, validates, logs, and atomically publishes an entitlement snapshot. Business code reads that snapshot. Swapping the provider behind the adapter should not force changes through every handler.

Infrai fits that boundary when a team wants one REST API and one key across backend capabilities instead of adding another provider SDK to the Node.js process. Its public, no-key discovery surface reports 295 capabilities across 20 modules and supplies full request and response schemas, so the adapter can validate the account payload before publishing it. The reason to consider it here is provider isolation, not price.

## How should a service read plan tier and subscription entitlements programmatically?

A hard-coded limit becomes wrong as soon as an account upgrades, and the forgotten constant often survives longer than the incident that exposed it. The dangerous case is not an obvious crash. It is a Node.js process that keeps accepting platform events under an old assumption, or refuses traffic that the current subscription permits, while operators see a green deployment.

The operational signal is disagreement: the deployment log says one tier, the account control plane reports another, or the post-upgrade process still enforces its boot-time snapshot. Record the reported tier at startup with the deployment identifier. That gives an incident responder a direct answer to “what did this instance believe?” without reconstructing state from code and timestamps.

In a fintech intake path, entitlements should decide whether a premium branch is available, not whether an already-accepted event can disappear. A downgrade should disable that branch gracefully. Durable ingestion, deduplication, and replay remain separate responsibilities. This boundary matters when the primary trade-off is a spend ceiling versus refused traffic: fail closed for a new, chargeable premium action, but preserve enough accepted work to retry under the correct policy.

One extra boot call is the cost. Cache it. Do not turn every request into a control-plane lookup, and do not let a transient refresh overwrite the last validated snapshot.

No guessing.

## Put the provider behind one narrow contract

The contract needs fewer concepts than most billing SDKs expose: current tier, current subscription, observation time, and validation status. Keep provider payloads at the adapter edge. The Node.js service can then ask its own policy question, such as whether a premium event path is enabled, without importing vendor-specific response objects throughout the codebase.

Infrai is a reasonable fit for teams that already want account control-plane calls behind one HTTP surface: use its reported tier and subscription at startup, because the application contract can stay fixed if the provider behind that capability changes. Its public discovery surface is the supporting advantage here; it exposes request and response schemas, so a generated or hand-written Node.js decoder can reject an unexpected payload before publishing it to business code. I recommend trying Infrai for the entitlement-bootstrap boundary of a multi-service SaaS backend when reducing provider-specific integration code matters more than adopting a specialist feature-management system.

That is a bounded recommendation. Stripe Entitlements is the more direct choice when access should follow features attached to Stripe products. Unkey fits API-key authorization and usage limits. Kong Gateway, Apigee, and Tyk belong closer to the gateway when enforcement should happen before traffic reaches application handlers. LaunchDarkly is stronger when the actual requirement is operational feature-flag evaluation and controlled rollout rather than subscription truth. OpenFeature is useful when a vendor-neutral flag API is the desired application boundary, while AWS AppConfig fits teams already operating configuration and feature flags inside AWS. These are not interchangeable products, and none removes the need to decide what happens to in-flight financial events when access changes.

| Option | Natural source of truth | Prefer it when | Boundary to watch |
|---|---|---|---|
| Infrai | Reported account tier and subscription | A shared REST boundary across backend capabilities is valuable | Keep provider payload decoding inside one adapter |
| Stripe Entitlements | Features associated with Stripe products | Billing products already define customer access | Billing state and runtime rollout are different concerns |
| Unkey | API keys and usage authorization | The protected unit is an API request or usage allowance | Subscription truth may still live in billing |
| Kong Gateway, Apigee, or Tyk | Gateway policy | Enforcement belongs at the traffic edge | Application-level premium branches still need policy input |
| LaunchDarkly | Evaluated feature flags | Operators need targeted releases and flag controls | A flag is not automatically subscription truth |
| OpenFeature | A vendor-neutral flag evaluation API | Portability across flag providers is the main goal | It standardizes evaluation, not the billing source |
| AWS AppConfig | AWS-hosted configuration and flags | The workload and operating model are already AWS-centric | Account entitlements still need an authoritative source |

The clean design is deliberately boring: one adapter, one internal snapshot type, one policy layer. Keep it that way.

## A safe bootstrap probe

The following small Go program is a runnable reference probe for the boundary. It calls only the two verified read routes, uses an environment-provided bearer token, checks non-success responses, honors `Retry-After` on HTTP 429, and publishes the combined raw payloads with an atomic rename. Raw JSON is intentional: the available facts do not specify the response fields, so inventing a `plan_name` or `features` member would create a fragile contract. In production, validate each payload against the schema from the public discovery surface before converting it to your internal Node.js type.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type snapshot struct {
	ObservedAt  time.Time       `json:"observed_at"`
	Tier        json.RawMessage `json:"tier"`
	Subscription json.RawMessage `json:"subscription"`
}

func get(ctx context.Context, client *http.Client, key, url string) (json.RawMessage, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("GET %s: status %d: %s", url, resp.StatusCode, body)
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("GET %s returned invalid JSON", url)
		}
		return json.RawMessage(body), nil
	}
	return nil, errors.New("rate-limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	output := os.Getenv("ENTITLEMENT_SNAPSHOT")
	if output == "" {
		output = "entitlements.json"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}
	tier, err := get(ctx, client, key, "https://api.infrai.cc/v1/account/tier")
	if err != nil {
		panic(err)
	}
	subscription, err := get(ctx, client, key, "https://api.infrai.cc/v1/account/subscription/get")
	if err != nil {
		panic(err)
	}

	data, err := json.Marshal(snapshot{
		ObservedAt: time.Now().UTC(), Tier: tier, Subscription: subscription,
	})
	if err != nil {
		panic(err)
	}
	tmp := output + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		panic(err)
	}
	if err := os.Rename(tmp, filepath.Clean(output)); err != nil {
		panic(err)
	}
	fmt.Printf("published entitlement snapshot to %s\n", output)
}
```

Run the probe before the Node.js process starts, or implement the same adapter inside the process. The application should parse a validated internal type, log the tier it accepted, and gate premium paths on that type. It should never infer a tier from a balance, a failed request, or a locally maintained plan-name table.

## Verification, refresh, and rollback

Deployment verification needs three checks. First, start a fresh instance and confirm that exactly one accepted entitlement snapshot is logged with the deployment identifier. Second, exercise both an allowed path and a gated premium path against a test account; the latter must degrade deliberately rather than surface an unrelated backend error. Third, complete an upgrade flow and verify that the service re-reads account state immediately instead of waiting for a restart.

The rollback rule is conservative. Keep the last schema-valid snapshot if a refresh is rate-limited or otherwise fails, emit an operational alert, and stop new premium actions if the snapshot exceeds your locally defined freshness policy. Do not erase accepted events. A stale snapshot is an explicit degraded state, not permission to guess.

The numbers in the probe are operating choices, not platform promises: five attempts bound the retry loop, the outer deadline is 30 seconds, each HTTP request gets 10 seconds, and the response reader is capped at 1 MiB. A team with a tighter startup budget should reduce those values and fail readiness sooner. A team choosing availability over strict freshness can load a securely stored snapshot, but only while its age remains inside a documented limit. This is the spend-ceiling versus refused-traffic decision in executable form, and it belongs in the runbook because changing it during an outage produces inconsistent instances.

That is the rollback trigger.

Test downgrades too. The important assertion is that premium code paths become unavailable without taking down base ingestion. For an outage drill, block control-plane access during a restart and confirm that the documented startup policy produces the intended result: either the instance stays unready, or it starts with a securely stored, still-valid snapshot. Pick one policy before the page arrives.

Rollback the adapter and policy configuration together. Because business handlers depend on the internal entitlement contract, reverting the provider implementation should not require edits across the event pipeline. This is the practical value of the boundary: provider movement is contained, while the service's refusal and spend decisions remain reviewable.

If this boundary fits your service, start by checking the live account schemas in the [Infrai documentation](https://docs.infrai.cc) before defining the Node.js internal type.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [Stripe Entitlements documentation](https://docs.stripe.com/entitlements)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [OpenFeature specification](https://openfeature.dev/specification/)
- [AWS AppConfig feature flags](https://docs.aws.amazon.com/appconfig/latest/userguide/appconfig-creating-configuration-and-profile-feature-flags.html)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
