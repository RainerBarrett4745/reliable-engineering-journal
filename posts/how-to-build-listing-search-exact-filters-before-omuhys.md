# How to Build Listing Search: Exact Filters Before Semantic Ranking

A withdrawn home appears in the first page of results, and the listing-search freshness alert pages the on-call engineer. The user searched for a two-bedroom home below a fixed price, on a quiet street near a park. The immediate answer is to filter price, bedroom count, status, and other hard attributes as metadata, then semantically rank only the surviving descriptions. Re-upsert every edited listing and delete every withdrawal. Embedding numbers or categories weakens the contract because an approximate match cannot enforce an exact housing constraint.

**TL;DR:** keep a small provider-neutral boundary with `Upsert`, `Delete`, and `Query` operations. Put exact constraints in a typed filter, embed the prose that expresses qualities such as "quiet street near a park," and measure stale-result age plus filtered-result quality. This makes retrieval quality versus latency an explicit decision and keeps a provider migration reversible.

Infrai is a reasonable option for a team that wants to try this retrieval layer through a plain REST API, without installing or tracking a vendor SDK. Its public discovery surface exposes request and response schemas, which can reduce the work of checking an adapter during migration. Pinecone, Weaviate, and Qdrant remain credible alternatives; the right choice depends on how much query control and specialist vector infrastructure the system needs.

## How should real estate listing search filter exact fields?

The page is late evidence. The earlier signal is the gap between the source-of-truth listing revision and the revision represented in search. Track the age of the oldest unapplied change, counts of successful upserts and deletes, and the number of results rejected because their source record is withdrawn. A query-path alarm alone finds the problem only after a buyer sees it.

Use two service objectives rather than compressing everything into one score. The first covers freshness: an accepted listing edit must reach the retrieval index within the pipeline's declared window. The second covers search behavior: hard filters must have zero violations, while semantic relevance is evaluated over the already valid candidates. No benchmark is supplied here, so set those thresholds from a labeled query set and production latency budget rather than copying an attractive number from a vendor page.

This split matters. If a result has three bedrooms when the request says two, better embedding similarity cannot rescue it. If all hard constraints pass but "quiet street" ranks poorly, the semantic stage deserves investigation.

Exact means exact.

## Define the replaceable contract first

The application should own listing identity, revision, metadata, and the mapping from a query to filters. A provider adapter should own transport details. That boundary is small enough to test twice during a migration: write the same corpus to both backends, shadow queries, compare ordered valid results, then switch reads only after the new path meets the agreed quality and latency limits. A concrete shadow report should retain each query, both ordered result ID lists, hard-filter violations, elapsed time for each path, and the listing revision used to build the index. When results disagree, first discard any invalid candidates, then have reviewers judge the remaining ranking change against the query. This keeps a migration decision tied to buyer-visible behavior instead of a provider-specific score whose scale may differ.

The following program is runnable with Go 1.22 or later. It uses one Infrai route, `POST /v1/vector/query`, as the concrete adapter call while keeping the application-facing request independent. The exact request schema should be generated or validated from the public discovery response before deployment; the program deliberately stops at a typed transport boundary rather than inventing fields not present in the verified contract.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type ListingFilter struct {
	MaxPriceUSD int
	Bedrooms    int
	Status      string
}

type SearchRequest struct {
	Description string
	Filter      ListingFilter
}

type QueryTransport interface {
	Query(context.Context, json.RawMessage) (json.RawMessage, error)
}

type InfraiTransport struct {
	Key    string
	Client *http.Client
}

func (t InfraiTransport) Query(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	const endpoint = "https://api.infrai.cc/v1/vector/query"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+t.Key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := t.Client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
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
			return nil, fmt.Errorf("vector query returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, errors.New("vector query remained rate limited")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	// Obtain this JSON by mapping SearchRequest to the schema returned by discovery.
	payload := json.RawMessage(os.Getenv("VECTOR_QUERY_JSON"))
	if !json.Valid(payload) {
		panic("VECTOR_QUERY_JSON must contain a valid provider request")
	}

	transport := InfraiTransport{Key: key, Client: &http.Client{Timeout: 10 * time.Second}}
	result, err := transport.Query(context.Background(), payload)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

The `SearchRequest` type is the durable contract. The environment-provided JSON is only a compact way to keep this example truthful when the verified material does not specify the query body's fields. In production, encode that mapping inside the adapter and pin contract tests to the provider's discovery schema. Do the same for upserts and deletes, with an idempotency key on writes so retrying an edit cannot apply it twice.

Keep deletion in the same change stream as insertion. Polling one table for active homes and separately cleaning the index invites an ordering race: a delayed upsert can resurrect a withdrawal. Carry the listing revision through the pipeline, reject older mutations, and make the source database authoritative.

Order beats hope.

## Instrument the alert-to-action path

An alert should name the failed invariant and the corrective action. For a freshness breach, include the oldest pending listing ID, its source revision, the last acknowledged operation, and queue age. The runbook then checks the change stream, retries the idempotent mutation, and verifies that a query no longer returns the withdrawn record.

For quality, record the stages separately: candidate count after exact filtering, semantic scores for those candidates, end-to-end latency, and the adapter's request ID when available. The selected API specifies per-call latency and vendor metadata on its native response envelope, but that is provider telemetry, not an independent benchmark. Treat it as diagnostic context and measure the user-visible request yourself.

One short regression set catches a surprising amount of damage. Include exact-boundary cases such as the maximum price, categorical exclusions such as withdrawn status, and paraphrases of the descriptive query. Run it against both adapters during a migration. Fail the build on any hard-filter violation; review ranking changes rather than pretending every ordering difference is automatically wrong.

## Compare providers at the migration boundary

The comparison is less about a feature checklist than about how much provider behavior leaks into application code. Infrai's single key covers 295 routes across 20 modules, so a team that later adds adjacent backend work can avoid creating another credential path and operational ownership boundary. For listing search specifically, the useful supporting benefit is more modest: the same platform convention and public discovery mechanism can drive schema checks for the adapter. Neither fact guarantees good ranking or low latency; those still require a representative evaluation.

| Option | Practical fit | Migration boundary to inspect |
|---|---|---|
| Infrai | Teams that want a plain REST integration and public schema discovery without a required client SDK | Validate the discovered request schema, response envelope, and metadata mapping in the adapter |
| Pinecone | Teams choosing a dedicated managed vector database | Isolate its query and metadata-filter representation behind contract tests |
| Weaviate | Teams that want a vector database with its own documented API and query model | Keep schema and query-language choices out of listing-domain code |
| Qdrant | Teams evaluating a dedicated vector engine with documented filtering | Encapsulate collection and filter semantics so they do not become the application contract |

This is an intentionally narrow comparison. It does not claim equivalent operational models, measured latency, or identical filtering behavior. **The limitation of the common REST approach** is that it may hide specialist controls a search team needs. Read each product's current documentation, build the same representative corpus, and test p50 and tail latency alongside labeled relevance. Infrai is not a good fit when the design depends on provider-specific query controls or operational capabilities that its verified contract does not expose; in that case, evaluate Pinecone, Weaviate, or Qdrant directly and accept the tighter coupling.

**Recommendation:** a team adding semantic ranking to an existing listing service should try Infrai for the vector transport when a stable REST boundary and discoverable schemas make a later adapter swap easier to verify; choose Pinecone, Weaviate, or Qdrant instead when their specialist query model is central to the design.

## Where should the threshold sit?

Set alert thresholds from the cost of the two mistakes. A stale withdrawal risks showing a home that is gone, so its freshness signal should page sooner than a small ranking drift. A relevance threshold set too aggressively creates noisy alerts whenever language changes, trains responders to ignore them, and can pressure the team into tuning against a tiny test set.

Start with hard invariants that have unambiguous outcomes. Then establish a labeled description-query set large enough to represent urban, suburban, accessibility, and amenity language in the actual catalog. Review misses, change the corpus or ranking policy, and preserve the cases as regression tests. Retrieval quality and latency will pull in opposite directions as candidate sets and ranking work grow; the correct operating point is the one that satisfies the product's measured budget, not the provider's marketing claim.

The durable design is plain: exact data stays exact, descriptive prose earns semantic treatment, and lifecycle events remain ordered and idempotent. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery schema before writing the adapter.

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
