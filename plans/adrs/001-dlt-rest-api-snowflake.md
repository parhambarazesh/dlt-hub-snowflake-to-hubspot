# ADR-001: Snowflake Access via REST API with DLT

- **Status:** Proposed
- **Date:** 2026-03-06

## Context

The pipeline needs to read data from Snowflake. DLT offers several approaches: `sql_database` source (direct SQL connector), `rest_api` source (declarative HTTP config), or manual `@dlt.resource` with direct API calls. The user prefers using DLT's `rest_api` source targeting Snowflake's SQL REST API.

## Decision

Use Snowflake's SQL REST API (`/api/v2/statements`) as the data source, accessed through DLT's `rest_api` source or a manual `@dlt.resource` fallback.

- **Auth**: JWT key-pair authentication using the existing RSA private key (`rsa_key.p8`). Custom `AuthBase` subclass generates JWT with SHA256 fingerprint-based `iss` claim.
- **Pagination**: Custom `BasePaginator` subclass handling Snowflake's partition-based model (POST returns `partitionInfo`, subsequent GETs fetch partitions).
- **Data transform**: Response hook converting column-metadata + array-of-arrays into dicts with lowercased keys.
- **Fallback**: If declarative `rest_api` config can't handle the POST→GET method switch, use `@dlt.resource` with manual HTTP calls (still DLT-native).

## Rationale

- REST API avoids needing `snowflake-connector-python` as a runtime dependency — lighter footprint
- Keeps the pipeline HTTP-based end-to-end, consistent with the `rest_api` source pattern
- JWT key-pair auth reuses the existing RSA key infrastructure from the archived pipeline
- Starting with `@dlt.resource` fallback is pragmatic — Snowflake's POST→GET pagination pattern is non-standard

## Consequences

- Must implement custom JWT auth and partition paginator (not built into DLT)
- Must handle Snowflake's array-of-arrays response format conversion
- Snowflake REST API has concurrency limits and async query patterns to be aware of
