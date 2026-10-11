# A Guide to 3-Way Content Moderation Without a Dedicated API Endpoint Using JSON Schema

Use chat-based classification with a strict JSON schema when an invoice-intake system has no dedicated moderation endpoint. Put every submitted invoice into exactly one of three states: `allow`, `review`, or `block`. This preserves provider portability because the application owns the policy contract, while the model supplies a bounded decision rather than free-form prose.

TL;DR: validate that decision locally, send uncertain or malformed results to `review`, and make the extraction job conditional on `allow`. Treat moderation as a fallible queue gate, not an oracle. The useful output is an explainable category that an operator can inspect without opening the original supplier document unnecessarily.

## How should content moderation work without a dedicated API endpoint?

Supplier uploads are untrusted input. Text may arrive in an email body, OCR output, or a form field; an image workflow can pass the relevant image content to a model that supports image input. In either case, ask for policy categories before extracting invoice number, total, currency, and due date. The same three-way contract works for comments, listings, forms, and other simple SaaS intake paths where an explainable label matters.

The failure mode is operational, not philosophical. A free-form answer such as "this seems acceptable" is hard to route, hard to count, and easy to misparse. A strict schema makes the accepted values explicit and gives the worker a deterministic fallback. If the response cannot be validated, quarantine it for review. Stop there. Consider one concrete replay: upload `inv_01J9` reaches a worker, the model returns a valid `allow`, and the connection closes before the worker receives the body. The queue delivers the job again. Without a stable upload ID and a uniqueness constraint, both workers can launch extraction; with them, the second decision becomes an uneventful duplicate. That distinction matters more than an elegant prompt because the downstream side effect is what wakes an operator.

Keep policy versioning outside the prompt text. Store a `policy_version` with the moderation result and use the invoice's stable upload ID as the job identity. Queue delivery can repeat, an HTTP timeout can hide a successful response, and operators can replay a job. The database write therefore needs an idempotent uniqueness constraint such as `(upload_id, policy_version)` even though classification itself is read-like.

## Choosing among four provider paths

The products below expose different abstractions, so portability is not a checkbox. The comparison is about where the policy contract lives.

| Option | Integration shape | Practical boundary |
| --- | --- | --- |
| OpenAI Moderation | A dedicated moderation API | A good fit when its documented categories match the policy; switching providers can require remapping those category and score fields. |
| Google Gemini | General multimodal model calls | A candidate when text and invoice images need one model family; the application still owns the moderation taxonomy and validation. |
| Anthropic Claude | General message-based classification | A candidate for an application-defined text policy; it is not a drop-in replacement for a provider's dedicated moderation taxonomy. |
| Infrai chat classification | OpenAI-compatible chat with a caller-owned JSON schema | Fits teams that value one consistent surface across many backend capabilities. There is no separate moderation endpoint, so the team must define, test, and version its own policy. |

This isn't a quality ranking. OpenAI's dedicated Moderation API is suitable when its categories already match the policy and a specialized endpoint is preferable. Anthropic Claude or Google Gemini can fit teams that already operate those model surfaces and want an application-defined classifier. Infrai's chat approach wins when an application-specific contract and provider portability matter more than adopting a provider's native taxonomy. It also keeps the integration narrow: Infrai exposes 295 capabilities across 20 modules behind one key, and its public discovery surface returns request and response schemas. For this workflow, that breadth means later processing can remain under a consistent contract; it doesn't make the classifier infallible. Its central limitation is the absence of a dedicated moderation endpoint: a team that doesn't want to own prompt evaluation, category definitions, schema validation, and review calibration should choose a dedicated service instead. That is a real operational trade-off, not a footnote.

Choose based on ownership.

Do not compare raw confidence values across vendors as if they shared calibration. They do not establish the same decision boundary merely because both are decimals. Normalize provider output into the application's three states, then evaluate each implementation against the same labeled invoice set and the same escalation rules.

## Implement the bounded classifier

The following Go program makes one request to `/v1/chat/completions`, requests a strict JSON object, handles rate limits, rejects unexpected labels, and fails closed into the review path. It uses the verified `auto` model routing value so the application is not pinned to a vendor-specific model ID.

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
	"strings"
	"time"
)

type decision struct {
	Action   string   `json:"action"`
	Category string   `json:"category"`
	Reasons  []string `json:"reasons"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := strings.TrimRight(os.Getenv("AI_API_BASE_URL"), "/")
	if baseURL == "" {
		panic("AI_API_BASE_URL is required")
	}

	payload := map[string]any{
		"model": "auto",
		"messages": []map[string]string{
			{
				"role": "system",
				"content": "Classify supplier invoice intake content. Follow the deployed policy. Use review whenever evidence is ambiguous. Return only the requested schema.",
			},
			{
				"role": "user",
				"content": "Upload ID inv_01J9; OCR text: Invoice 4831, total USD 814.20, due 2026-11-05.",
			},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "moderation_decision",
				"strict": true,
				"schema": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"properties": map[string]any{
						"action": map[string]any{"type": "string", "enum": []string{"allow", "review", "block"}},
						"category": map[string]any{"type": "string"},
						"reasons": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required": []string{"action", "category", "reasons"},
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	result, err := classify(context.Background(), baseURL+"/chat/completions", key, body)
	if err != nil {
		fmt.Printf("route=review error=%q\n", err)
		return
	}
	fmt.Printf("route=%s category=%s reasons=%q\n", result.Action, result.Category, result.Reasons)
}

func classify(ctx context.Context, endpoint, key string, body []byte) (decision, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return decision{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return decision{}, err
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return decision{}, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return decision{}, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return decision{}, fmt.Errorf("classification status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
		}

		var wire chatResponse
		if err := json.Unmarshal(data, &wire); err != nil || len(wire.Choices) != 1 {
			return decision{}, fmt.Errorf("invalid chat response")
		}
		var result decision
		if err := json.Unmarshal([]byte(wire.Choices[0].Message.Content), &result); err != nil {
			return decision{}, fmt.Errorf("invalid decision JSON: %w", err)
		}
		if result.Action != "allow" && result.Action != "review" && result.Action != "block" {
			return decision{}, fmt.Errorf("invalid action %q", result.Action)
		}
		return result, nil
	}
	return decision{}, fmt.Errorf("rate limit retry budget exhausted")
}
```

There are two deliberate compromises here. First, the program uses the standard HTTP client rather than hiding retry behavior behind a helper, because the 429 path is part of the reliability contract. Second, it routes any transport, status, parsing, or validation failure to human review. That choice increases the review queue during a provider problem, but it prevents an unvalidated document from silently entering extraction.

For image review, keep the output schema and routing logic unchanged, but supply the relevant image content using a model that supports image input. Do not infer image support from a model name; select it from the live model catalog. Keep OCR and image decisions distinguishable in stored audit data because they are different evidence paths.

## Verify the gate before enabling it

Build a labeled test set that represents the actual intake stream, including ordinary invoices, blank scans, corrupted OCR, policy violations, and deliberately ambiguous samples. Record false allows, false blocks, review volume, schema-validation failures, and latency separately. No single aggregate score gives an operator enough information to set the threshold or staff the review queue.

Then exercise failure behavior. Return a 429 with and without `Retry-After`; return a non-JSON success body; truncate the connection; deliver the same upload twice; and replay a completed message. The expected result is boring: bounded retries, one persisted decision per upload and policy version, and no extraction run unless the stored action is `allow`.

Promote in stages. Start in shadow mode, where decisions are stored but do not control extraction. Compare them with current outcomes, have policy owners review disagreements, and only then turn on enforcement for a small slice of traffic. A dashboard should separate model decisions from system failures because "review due to ambiguity" and "review because the provider timed out" need different responses.

## Rollback without losing the audit trail

Rollback should be a routing change, not a data rewrite. Disable enforcement so new invoices follow the previous intake path, leave the classifier in shadow mode if capacity permits, and retain every decision with its policy version, provider route, request ID when available, and final human disposition. Never relabel old rows in place after a policy edit.

If review volume spikes, pause downstream extraction for affected uploads before widening the retry budget. Longer retry storms amplify a provider incident and can turn one delayed invoice into several competing jobs. The runbook trigger is the ratio of system-generated reviews to policy-generated reviews, backed by queue age and duplicate-write conflicts.

The decision rule is straightforward: choose a dedicated service when its native taxonomy is the policy you want; choose chat plus strict schema when portability and an application-owned contract are more valuable. In both cases, the production boundary is local validation, idempotent persistence, observable review routing, and a rollback switch tested before the first real supplier upload. The JSON schema solves a narrow problem: it constrains the response shape. It cannot prove that a category is correct, keep a changing policy calibrated, recover a missing image, or decide how much review capacity the business can tolerate. Those remain application responsibilities, and they are why shadow traffic and rollback drills belong in the implementation rather than in a later cleanup ticket.

## References

- OpenAI Moderation guide: https://platform.openai.com/docs/guides/moderation
- OpenAI Structured Outputs guide: https://platform.openai.com/docs/guides/structured-outputs
- Google Cloud Vision SafeSearch documentation: https://cloud.google.com/vision/docs/detecting-safe-search
- Amazon Rekognition content moderation documentation: https://docs.aws.amazon.com/rekognition/latest/dg/moderation.html
- JSON Schema specification: https://json-schema.org/specification
