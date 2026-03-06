# Plan: Snowflake → HubSpot DLT Pipeline (POC)

Build a DLT-based pipeline that reads contacts and companies from Snowflake via its SQL REST API (`rest_api` source) and writes them to HubSpot using a custom DLT destination (`@dlt.destination`). The pipeline supports partition-based pagination on the Snowflake side, batch processing (100/request) on the HubSpot side, and upsert logic. Data flows as-is — all transformations happen in Snowflake beforehand.

> **Decisions are captured in ADRs** — see [plans/adrs/](adrs/).

---

## Decisions Summary

| Decision | ADR |
|----------|-----|
| Snowflake access via REST API with DLT | [ADR-001](adrs/001-dlt-rest-api-snowflake.md) |
| Custom HubSpot destination via `@dlt.destination` | [ADR-002](adrs/002-custom-hubspot-destination.md) |
| Upsert strategy (search → create/update) | [ADR-003](adrs/003-upsert-strategy.md) |

---

## Phase 1: Project Setup & Configuration

1. **Project structure** — Create `pipeline/` directory at repo root with: `snowflake_source.py`, `hubspot_destination.py`, `pipeline.py`, `auth.py`, `paginator.py`, `__init__.py`
2. **Dependencies** — `requirements.txt` with: `dlt>=1.0.0`, `cryptography`, `PyJWT`, `python-dotenv`, `requests`
3. **Config template** — `.env.template` with Snowflake connection vars, `HUBSPOT_API_KEY`, table name vars

## Phase 2: Snowflake REST API Source *(depends on Phase 1)*

4. **JWT Auth** (`pipeline/auth.py`) — Custom `requests.auth.AuthBase` subclass: loads RSA key from `rsa_key.p8`, generates JWT with SHA256 fingerprint-based `iss` claim, sets `X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT` header, caches token until near-expiry
5. **Partition Paginator** (`pipeline/paginator.py`) — Custom `BasePaginator` subclass for Snowflake's partition-based pagination: initial POST returns `partitionInfo` array, subsequent GETs fetch `/api/v2/statements/{handle}?partition={N}`
6. **Data Transformation** — Response hook converting Snowflake's array-of-arrays format (`resultSetMetaData.rowType` + `data`) into dicts with lowercased keys matching HubSpot properties
7. **Source Config** (`pipeline/snowflake_source.py`) — DLT `rest_api_source()` targeting Snowflake SQL REST API with POST method, custom auth + paginator, two resources (contacts, companies). See [ADR-001](adrs/001-dlt-rest-api-snowflake.md)
8. **Fallback: Manual `@dlt.resource`** — If declarative `rest_api` can't handle POST→GET method switch between initial query and partition fetches, implement as `@dlt.resource` generator. Still DLT-native, just imperative. **Recommended starting point.**

## Phase 3: HubSpot Custom Destination *(parallel with Phase 2)*

9. **Destination function** (`pipeline/hubspot_destination.py`) — `@dlt.destination(batch_size=100)` that routes by `table["name"]` to contacts or companies API. See [ADR-002](adrs/002-custom-hubspot-destination.md)
10. **Upsert logic** — Batch search → split into create/update → call `/batch/create` and `/batch/update`. See [ADR-003](adrs/003-upsert-strategy.md)
11. **Error handling** — Log individual record failures from HubSpot 400 responses without halting; raise on 401/429

## Phase 4: Pipeline Orchestration *(depends on Phase 2 + 3)*

12. **Main pipeline** (`pipeline/pipeline.py`) — Wire source → destination via `dlt.pipeline()`, load `.env`, CLI: `python -m pipeline.pipeline [contacts|companies|all]`
13. **Logging & summary** — Use DLT's built-in logging; print records read/created/updated/failed per entity

## Phase 5: Testing & Validation *(depends on Phase 4)*

14. **Connection tests** (`pipeline/test_setup.py`) — Verify env vars, JWT generation, Snowflake REST API connectivity, HubSpot API connectivity
15. **Integration test** — Run pipeline with `LIMIT 5`, verify records in HubSpot, re-run to verify upsert (no duplicates)
16. **Pipeline state** — `dlt pipeline snowflake_to_hubspot info` to verify DLT state

---

## Verification

1. JWT auth generates valid tokens — test with `SELECT CURRENT_TIMESTAMP()` against Snowflake REST API
2. Partition paginator correctly iterates all partitions — mock Snowflake response with 3 partitions
3. Data transformation converts array-of-arrays to dicts with correct lowercased column names
4. HubSpot destination correctly splits batch into create vs. update based on search results
5. Full pipeline run with `LIMIT 10` — verify records appear in HubSpot
6. Pipeline re-run — verify no duplicates (upsert idempotency)
7. CLI: `contacts` runs only contacts, `all` runs both entities

---

## Reference Files

- [archived-pipeline/minimal_pipeline.py](../archived-pipeline/minimal_pipeline.py) — HubSpot batch API patterns, Snowflake RSA auth, data flow
- [archived-pipeline/.env.template](../archived-pipeline/.env.template) — Required environment variables
- [archived-pipeline/test_setup.py](../archived-pipeline/test_setup.py) — Connection test patterns

## Further Considerations

1. **Start with `@dlt.resource`** (Step 8) over declarative `rest_api` (Step 7). Snowflake REST API's POST→GET method switch is non-standard. Manual resource is more reliable for this.
2. **HubSpot rate limits**: ~100 requests/10 seconds for private apps. Add backoff/sleep if processing large datasets.
3. **Column mapping**: Snowflake = uppercase (`EMAIL`), HubSpot = lowercase (`email`). Auto-lowercased in transformation step. Configurable mapping table could be added later.
