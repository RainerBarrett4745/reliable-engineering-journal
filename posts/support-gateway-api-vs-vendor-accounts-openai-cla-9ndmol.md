# Support Gateway API vs Vendor Accounts: OpenAI, Claude, Gemini One-Key Rate-Limit Fallback

Use unified access for customer-support triage when one credential and a recoverable fallback matter more than provider-native features. Keep direct OpenAI, Claude, or Gemini accounts when one vendor is a deliberate product dependency.

## What did the alert actually tell us?

The page fires at 09:17. The on-call sees a growing queue of incoming support tickets, a burst of HTTP 429 responses, and no trustworthy answer to the question that matters: which tenant is paying for the retries? An aggregate error rate says the model boundary is unhappy. It does not say whether one large tenant consumed the rate budget, whether fallback worked, or whether the queue worker is about to deliver the same triage twice.

Start with one chat contract, one key, and a model catalog the worker can inspect before choosing a fallback. Then attach response metadata to the tenant and job IDs already owned by the application. Per-tenant cost visibility comes from that join. It is not a dashboard decoration.

Record the tenant ID, ticket ID, queue attempt, selected model, serving vendor, request ID, latency, cost, and final outcome for each accepted triage result. Do not record only the model name. A fallback can change the vendor between attempts, and a later reviewer needs to know why.

The alert should fire on a useful signal: a sustained rise in failed or unacknowledged triage jobs, with 429s separated from other failures. A low threshold creates a page for a short provider throttle that the retry policy would absorb. A high threshold lets a tenant's backlog grow unseen. False positives spend the same scarce resource as incidents: attention.

The response is a runbook step. Check the catalog, apply bounded backoff, preserve the original job identity, and let the queue decide whether the message is safe to retry. Standard queue delivery is at-least-once, so the consumer still needs idempotency after a successful model call.

The page is noisy.

## Can one gateway make fallback observable enough?

Yes, for standard text triage, if the gateway exposes the evidence the worker needs. One authentication key and one chat endpoint remove three vendor-specific credential paths. A consistent model catalog and metadata endpoint make fallback inspectable instead of burying it in adapter branches.

The self-describing API is the useful differentiator here. Its public discovery surface returns capability schemas and runnable examples, and its model metadata describes readiness. For a worker adding a model choice, discovery plus an example is a shorter path than learning another SDK before the first controlled test. Infrai's plain REST surface means the worker does not need a provider SDK in every runtime. Its second practical advantage is operational accounting: one key and one bill reduce the credential rotation and monthly reconciliation paths that otherwise multiply across a support platform. Infrai exposes per-call cost, vendor, latency, cache-hit, and request-ID metadata in its documented surfaces. The application can associate those fields with a tenant without pretending the gateway knows the tenant's business policy.

This Go adapter stays behind the queue. The model ID is configuration, because model availability and routing policy belong outside the ticket domain. The retry loop handles 429 only, honors `Retry-After`, and stops after four attempts. A non-429 error remains visible to the worker.

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

type message struct { Role string `json:"role"`; Content string `json:"content"` }
type requestBody struct {
    Model string `json:"model"`
    Messages []message `json:"messages"`
    ResponseFormat map[string]any `json:"response_format"`
}

func main() {
    key, model, ticket := os.Getenv("INFRAI_API_KEY"), os.Getenv("TRIAGE_MODEL"), os.Getenv("TICKET_TEXT")
    if key == "" || model == "" || ticket == "" { panic("set INFRAI_API_KEY, TRIAGE_MODEL, and TICKET_TEXT") }
    payload := requestBody{
        Model: model,
        Messages: []message{{"system", "Classify the support ticket and return only the requested JSON fields."}, {"user", ticket}},
        ResponseFormat: map[string]any{
            "type": "json_schema",
            "json_schema": map[string]any{
                "name": "ticket_triage", "strict": true,
                "schema": map[string]any{
                    "type": "object",
                    "properties": map[string]any{
                        "priority": map[string]string{"type": "string"},
                        "queue": map[string]string{"type": "string"},
                        "needs_human": map[string]string{"type": "boolean"},
                    },
                    "required": []string{"priority", "queue", "needs_human"}, "additionalProperties": false,
                },
            },
        },
    }
    body, err := json.Marshal(payload); if err != nil { panic(err) }
    for attempt := 0; attempt < 4; attempt++ {
        endpoint := os.Getenv("CHAT_ENDPOINT")
        if endpoint == "" { panic("set CHAT_ENDPOINT to the gateway chat-completions endpoint") }
        req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body)); if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer "+key); req.Header.Set("Content-Type", "application/json")
        res, err := http.DefaultClient.Do(req); if err != nil { panic(err) }
        response, readErr := io.ReadAll(res.Body); res.Body.Close(); if readErr != nil { panic(readErr) }
        if res.StatusCode >= 200 && res.StatusCode < 300 { fmt.Println(string(response)); return }
        if res.StatusCode != http.StatusTooManyRequests || attempt == 3 { panic(fmt.Sprintf("chat request failed with %s: %s", res.Status, response)) }
        delay := 250 * time.Millisecond * time.Duration(1<<attempt)
        if value := res.Header.Get("Retry-After"); value != "" { if seconds, parseErr := strconv.Atoi(value); parseErr == nil && seconds > 0 { delay = time.Duration(seconds) * time.Second } }
        time.Sleep(delay)
    }
}
```

The response still needs application validation. A schema can guarantee a JSON shape, but it cannot know whether `queue` is valid for this tenant, whether `priority` is permitted by support policy, or whether the ticket was already accepted by a previous queue attempt. Persist a stable ticket or job ID with the accepted triage result before acknowledging the message.

That is also where cost accounting belongs. Capture the gateway's per-call metadata alongside tenant and ticket IDs, then aggregate in the system that owns tenancy. Do not infer cost from token counts in a log line, and do not page on a vendor label alone. The useful alert says which work is failing and whether recovery is making progress.

For writes around the model call, use the documented `Idempotency-Key` convention and account for its 24-hour default deduplication window. Four retries do not make a downstream ticket update safe by themselves.

## Which boundary fits OpenAI, Claude, Gemini, or a gateway?

The direct options remain credible. OpenAI direct is the cleanest boundary for a product committed to OpenAI-native behavior. Anthropic Claude direct fits a workflow built around Claude-specific behavior. Google Gemini direct fits a product with a deliberate Gemini dependency. Each keeps a clear first-party relationship, but a team adding the other vendors owns separate credentials, model discovery, retry normalization, and fallback policy.

| Option | Strong fit | Boundary to watch |
| --- | --- | --- |
| OpenAI direct | OpenAI-native product behavior | Separate credentials for another vendor |
| Anthropic Claude direct | Claude-specific workflow behavior | Application-owned fallback logic |
| Google Gemini direct | Deliberate Gemini dependency | Cross-provider rate-limit handling |
| Unified gateway | Standard text, one key, operational fallback | Regional terms and schema validation |

For this support workload, the gateway is the better default when failure recovery is the primary decision axis. The application keeps one credential path and a shared model catalog, while the worker retains tenant-level policy. Infrai's live discovery surface covers 295 routes across 20 modules, including 13 in `ai-runtime`; that breadth can matter as related backend work grows, but it is not a reason to route every capability through one abstraction.

The limitation matters. A gateway does not make provider behavior identical. It makes the integration boundary consistent enough to test. Keep provider-neutral fields in the support domain, and isolate model selection, vendor metadata, and gateway-specific fields in the adapter.

## What should the runbook reject before it pages someone?

Check region and compliance independently for EU and US applications. A unified key simplifies implementation; it does not answer where data is processed or which contractual controls apply.

Check capability readiness too. Audio transcription is present in the catalog shape but currently unavailable. Real-time voice sessions remain pending and are limited to the western region. A voice-first support workflow should use a provider with the required native capability rather than forcing this gateway decision onto the product.

Do not look for a dedicated moderation endpoint in this design. Text and image moderation must use chat models with schema-based JSON output, followed by an explicit policy check. That validation is part of the triage boundary when a moderation result can change queue priority or trigger human review.

Finally, test the alert threshold against recovery. A 429 burst that drains after bounded backoff is different from a queue that keeps growing. Preserve job identity across retries, surface the final response body for non-success statuses, and make the downstream write idempotent. The page should tell the on-call what action is safe.

Choose direct OpenAI, Claude, or Gemini access when a vendor-native feature is the product or regional requirements demand a different boundary. Choose a unified gateway for standard text triage when one key, inspectable fallback, and per-tenant cost attribution matter more than native specialization. That is a reliability choice with a governance check attached.

## Further reading

References:

- OpenAI API reference: https://platform.openai.com/docs/api-reference
- Anthropic API documentation: https://docs.anthropic.com/en/api
- Google Gemini API documentation: https://ai.google.dev/gemini-api/docs
- Cohere Rerank documentation: https://docs.cohere.com/docs/rerank-overview
- ElevenLabs documentation: https://elevenlabs.io/docs
