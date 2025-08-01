# AI-RAG-PR-SAMPLE

Sample repository for testing the FinOps Copilot PR Review feature.

Contains intentional IBM Cloud Logs cost anti-patterns across Go and Python services.

## Structure

```
handlers/       REST API handlers (Go)
services/       Embedding and vector store service (Go)
retrieval/      RAG retrieval pipeline (Go)
ingestion/      Document ingestion (Go)
pipeline/       Python inference pipeline
```

## PR branches

| Branch | Expected decision | Anti-pattern |
|---|---|---|
| `fix/payload-logging-block` | BLOCK | Object dumps in a loop |
| `fix/retry-warn` | WARN | Logging on every retry attempt |
| `fix/polling-warn` | WARN | Per-iteration polling logs |
| `fix/clean-pass` | PASS | Structured key=value logs only |
