# ADR-002: Custom HubSpot Destination via @dlt.destination

- **Status:** Proposed
- **Date:** 2026-03-06

## Context

DLT has no native HubSpot destination connector. Options: (a) custom DLT destination via `@dlt.destination` decorator, (b) post-pipeline API calls outside DLT, (c) DLT sink destination. The pipeline must write contacts and companies to HubSpot with batch processing (max 100 records/request).

## Decision

Use DLT's `@dlt.destination(batch_size=100, loader_file_format="jsonl")` decorator to create a custom HubSpot destination.

- Route by `table["name"]` to determine entity type (`contacts` or `companies`)
- Inject `api_key` via `dlt.secrets.value`
- Use HubSpot batch APIs: `/crm/v3/objects/{entity}/batch/create` and `/batch/update`
- Log individual record failures from 400 responses without halting the pipeline
- Raise on auth failures (401) or rate limits (429)

## Rationale

- Keeps everything within DLT's pipeline framework — source and destination managed by DLT
- `batch_size=100` aligns exactly with HubSpot's batch API limit
- DLT handles state management, retry logic, and load package tracking
- Cleaner than manually calling APIs after pipeline extraction completes

## Consequences

- Upsert logic (search → create/update) must be implemented inside the destination function — see [ADR-003](003-upsert-strategy.md)
- HubSpot rate limits (~100 req/10s) may need backoff handling for large datasets
- Entity-specific routing logic lives in one function — may need refactoring if more entities are added
