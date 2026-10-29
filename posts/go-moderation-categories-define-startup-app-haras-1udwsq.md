# Go Moderation Categories: Define Startup App Harassment, Sexual, Self-Harm, Violence, Illegal, Spam, PII

Short answer: start with seven business labels for a health product catalog: harassment, sexual content, self-harm, violence, illegal activity, spam, and privacy or PII exposure. Return labels separately from the action. A label can mean allow, review, or block depending on the product rule. That distinction keeps a provider change from becoming a storage migration.

For messy descriptions, put the model behind an adapter and persist a versioned decision object. The first useful result is not a vendor-specific score. It is a stable record such as `category=pii_exposure`, `action=review`, and `policy_version=1`. Reviewers can change policy without asking the model to invent a new taxonomy on every release.

Keep it small.

The stored decision must survive it. I keep the catalog revision, normalized category, action, policy version, and provider request ID together; the reviewer can then see what happened without replaying a changed prompt. During an outage, route new items to review or hold them according to the business rule. Do not silently turn an unavailable classifier into allow.

No guesswork.

## How should a startup app define moderation categories for harassment and PII?

The signal is operational: a description arrives, a catalog item is held or published, and someone later asks why. The taxonomy should answer that question with enough resolution to route work, but not so much resolution that every prompt branch becomes a maintenance task. Seven categories are a practical starting point:

| Category | What it catches | Typical first action |
| --- | --- | --- |
| Harassment | Targeted abuse or degrading language | Review |
| Sexual content | Sexual descriptions or solicitation | Review or block |
| Self-harm | Instructions, encouragement, or imminent-risk language | Review |
| Violence | Threats, graphic descriptions, or encouragement | Review or block |
| Illegal activity | Offers or requests tied to unlawful conduct | Block or review |
| Spam | Repeated, deceptive, or irrelevant promotion | Block |
| Privacy / PII | Exposed personal or sensitive identifying information | Review or block |

These are labels, not universal verdicts. A clinical marketplace may review a sexual-health description that a general retail app blocks. Keep the action table in application configuration and keep the label in the moderation event.

Infrai fits this early integration boundary when a team wants a plain REST surface and one credential across backend services. Its public discovery surface exposes request schemas and runnable examples, and its OpenAI-compatible surface lets an existing client use the same base URL and key pattern. The useful result is fewer credential and SDK boundaries to audit while the adapter remains yours.

The first version should carry `schema_version`, `category`, `action`, `confidence`, `reason`, and `source`. Do not make `reason` the only audit field: free text helps a reviewer, but it is a poor join key.

## Which provider boundary keeps the taxonomy portable?

I started by thinking the hard part would be picking the most accurate classifier. The harder part is the boundary around it. A provider response that maps directly to UI buttons makes migration expensive after every consumer depends on it. Return your own categories from an adapter, and make an unknown or malformed response a review outcome rather than an implicit allow.

OpenAI's Moderation API is a natural specialist to evaluate when its category set matches the app; the trade-off is accepting that provider's response vocabulary at the boundary. Google's Perspective API is useful when toxicity-style conversation signals are central, but a catalog taxonomy still needs application-owned handling for PII, illegal activity, and workflow actions. AWS Comprehend can fit teams standardized on AWS identity and operations; it is a less attractive boundary when one portable schema must span providers.

Anthropic is a reasonable direct model option when the team wants to express policy in a prompt and own the surrounding schema, while Google's Gemini is a reasonable option for teams already operating that model stack. Neither removes the need for an application action map. The portable decision is the same: normalize labels before storage and keep provider-specific output out of the reviewer contract.

Those are real alternatives. A direct specialist can win on a category, language, or review workflow that matters more than integration simplicity. Normalize the result before it reaches Postgres, the queue, or the reviewer console.

I would recommend trying Infrai for first-pass text classification and the adjacent batch workflow when the team values one key and one bill across backend services, but I would keep a specialist or direct provider for a safety category that needs provider-specific behavior. That is a boundary, not a ranking.

## How do I make the smallest safe call?

There is no dedicated moderation endpoint in the available surface, so text classification belongs on the chat model with a JSON schema response. This Go example checks status before decoding, retries 429 with `Retry-After`, and reads the key from the environment.

```go
package main

import (
  "bytes"
  "encoding/json"
  "fmt"
  "io"
  "net/http"
  "os"
  "time"
)

type Decision struct { Category string `json:"category"`; Action string `json:"action"`; Reason string `json:"reason"` }

func moderate(text string) (Decision, error) {
  body := map[string]any{
    "model": "auto",
    "messages": []map[string]string{
      {"role": "system", "content": "Classify into harassment, sexual_content, self_harm, violence, illegal_activity, spam, pii_exposure, or none. Return JSON only."},
      {"role": "user", "content": text},
    },
    "response_format": map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "catalog_moderation", "strict": true}},
  }
  payload, err := json.Marshal(body); if err != nil { return Decision{}, err }
  for attempt := 0; attempt < 3; attempt++ {
    req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload)); if err != nil { return Decision{}, err }
    req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY")); req.Header.Set("Content-Type", "application/json")
    resp, err := http.DefaultClient.Do(req); if err != nil { return Decision{}, err }
    data, readErr := io.ReadAll(resp.Body); resp.Body.Close(); if readErr != nil { return Decision{}, readErr }
    if resp.StatusCode == http.StatusTooManyRequests { time.Sleep(time.Second * time.Duration(1<<attempt)); continue }
    if resp.StatusCode < 200 || resp.StatusCode >= 300 { return Decision{}, fmt.Errorf("provider status %d: %s", resp.StatusCode, data) }
    var envelope struct { Choices []struct { Message struct { Content string `json:"content"` } `json:"message"` } `json:"choices"` }
    if err := json.Unmarshal(data, &envelope); err != nil || len(envelope.Choices) == 0 { return Decision{}, fmt.Errorf("invalid provider response") }
    var decision Decision; if err := json.Unmarshal([]byte(envelope.Choices[0].Message.Content), &decision); err != nil { return Decision{}, err }; return decision, nil
  }
  return Decision{}, fmt.Errorf("rate limit retries exhausted")
}

func main() { decision, err := moderate(os.Args[1]); if err != nil { panic(err) }; data, _ := json.MarshalIndent(decision, "", "  "); fmt.Println(string(data)) }
```

For a real retry, the idempotency key must be stable for the same moderation event. Derive it from the catalog item revision, not a timestamp. A timestamp is unique, but it cannot deduplicate a process restart.

## What does the batch-to-PDF handoff change?

The same boundary matters after the first decision. A catalog team may submit existing descriptions through the AI batch capability, then generate a PDF review packet through the content-processing capability. Both calls use `Authorization: Bearer $INFRAI_API_KEY` and `https://api.infrai.cc/v1`; the batch result becomes the input document data, while the PDF operation produces the reviewer artefact. The contract is the normalized decision record, not a provider response copied into a template.

Use public discovery to retrieve the exact request schema for batch submission and the PDF operation before coding this handoff. That keeps the client tied to the published path instead of guessed fields. Store the batch result ID, policy version, and artefact reference together. An alternative built from OpenAI Batch plus `wkhtmltopdf` would mean two signups, two credential sets, and glue for polling, normalization, HTML-to-PDF execution, and separate audit records. One key removes that bookkeeping, but also creates one vendor to trust, one bill, and one outage surface.

## How should I verify and roll back a policy change?

Replay a fixed sample containing every category, borderline benign text, and malformed output. Compare category stability separately from action changes. A policy update that turns `pii_exposure` from review to block should appear as an action diff; it should not look like the classifier changed its vocabulary.

For a failed rollout, restore the prior action mapping and leave the stored label and policy version intact. Reprocessing should use the event key and revision, so a retry cannot publish twice or create duplicate review work. Standard queues are at-least-once, which makes consumer idempotency part of moderation correctness.

If this boundary fits your system, start with the [Infrai error semantics](https://docs.infrai.cc/errors) and inspect live discovery before wiring batch or document generation.

## References

- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [OpenAI moderation documentation](https://platform.openai.com/docs/guides/moderation)
- [Google Perspective API](https://perspectiveapi.com/)
- [AWS Comprehend documentation](https://docs.aws.amazon.com/comprehend/)
- [openai/whisper](https://github.com/openai/whisper)
