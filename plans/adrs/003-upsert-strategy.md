# ADR-003: Upsert Strategy (Search → Create/Update)

- **Status:** Proposed
- **Date:** 2026-03-06

## Context

When writing records to HubSpot, duplicates must be avoided. The archived pipeline uses a search-before-write pattern: batch search for existing records, then split into create vs. update. The POC covers contacts (matched by `email`) and companies (matched by `name`).

## Decision

Implement upsert inside the custom HubSpot destination ([ADR-002](002-custom-hubspot-destination.md)) with the following flow per batch:

1. **Search** — POST to `/crm/v3/objects/{entity}/search` with identifier values (`email` for contacts, `name` for companies) using `IN` operator, max 100 values
2. **Partition** — Split batch items into "create" (no match) and "update" (match found, attach HubSpot `id`)
3. **Create** — POST to `/crm/v3/objects/{entity}/batch/create` with `{"inputs": [{"properties": {...}}]}`
4. **Update** — POST to `/crm/v3/objects/{entity}/batch/update` with `{"inputs": [{"id": "...", "properties": {...}}]}`
5. **Log** — Report counts of created/updated/failed records

## Rationale

- Matches the proven pattern from the archived pipeline (`send_to_hubspot_contacts()`)
- HubSpot's batch search + batch create/update is the most efficient approach (fewer API calls than per-record)
- Email is HubSpot's natural dedup key for contacts; company name is sufficient for this POC
- Separating create and update avoids HubSpot's 409 conflict errors on batch upsert

## Consequences

- Each batch costs 1 search + up to 2 write API calls (3 requests per 100 records)
- Search by `name` for companies is fuzzy — may need refinement for production (e.g., domain-based matching)
- If HubSpot adds a native batch upsert endpoint, this strategy could be simplified
