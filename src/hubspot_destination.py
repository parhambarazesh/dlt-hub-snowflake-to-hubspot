"""Custom DLT destination that writes records to HubSpot via the batch API.

Implements the upsert strategy from ADR-003:
  1. Batch search for existing records (by email for contacts, name for companies)
  2. Split into create vs. update lists
  3. Batch create new records
  4. Batch update existing records
"""

import logging
from typing import Any

import dlt
import requests

logger = logging.getLogger(__name__)

HUBSPOT_BASE_URL = "https://api.hubapi.com"
HUBSPOT_BATCH_LIMIT = 100
HUBSPOT_TIMEOUT = 30

# Maps DLT table name → (HubSpot object type, search property name)
ENTITY_CONFIG = {
    "contacts": ("contacts", "email"),
    "companies": ("companies", "name"),
}


def _hubspot_headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _batch_search(
    entity_type: str,
    search_property: str,
    values: list[str],
    headers: dict[str, str],
) -> dict[str, str]:
    """Search HubSpot for existing records. Returns {identifier_value: hubspot_id}."""
    existing: dict[str, str] = {}
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/{entity_type}/search"

    for i in range(0, len(values), HUBSPOT_BATCH_LIMIT):
        batch_values = values[i : i + HUBSPOT_BATCH_LIMIT]
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": search_property,
                            "operator": "IN",
                            "values": batch_values,
                        }
                    ]
                }
            ],
            "properties": [search_property],
            "limit": HUBSPOT_BATCH_LIMIT,
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=HUBSPOT_TIMEOUT)

        if resp.status_code == 401:
            raise RuntimeError(f"HubSpot authentication failed (401): {resp.text[:200]}")
        if resp.status_code == 429:
            raise RuntimeError(f"HubSpot rate limit exceeded (429): {resp.text[:200]}")

        if resp.status_code == 200:
            for result in resp.json().get("results", []):
                val = result["properties"].get(search_property)
                if val:
                    existing[val] = result["id"]
        else:
            logger.warning(
                "HubSpot search failed (%d): %s", resp.status_code, resp.text[:200]
            )

    return existing


def _batch_create(
    entity_type: str,
    inputs: list[dict[str, Any]],
    headers: dict[str, str],
) -> int:
    """Create records in HubSpot. Returns count of successfully created records."""
    if not inputs:
        return 0

    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/{entity_type}/batch/create"
    created = 0

    for i in range(0, len(inputs), HUBSPOT_BATCH_LIMIT):
        batch = inputs[i : i + HUBSPOT_BATCH_LIMIT]
        resp = requests.post(
            url, headers=headers, json={"inputs": batch}, timeout=HUBSPOT_TIMEOUT
        )

        if resp.status_code == 401:
            raise RuntimeError(f"HubSpot authentication failed (401): {resp.text[:200]}")
        if resp.status_code == 429:
            raise RuntimeError(f"HubSpot rate limit exceeded (429): {resp.text[:200]}")

        if resp.status_code in (200, 201):
            batch_created = len(resp.json().get("results", []))
            created += batch_created
            logger.info(
                "Created %d/%d %s (batch %d)",
                batch_created,
                len(batch),
                entity_type,
                i // HUBSPOT_BATCH_LIMIT + 1,
            )
        else:
            logger.error(
                "Batch create failed (%d): %s", resp.status_code, resp.text[:300]
            )

    return created


def _batch_update(
    entity_type: str,
    inputs: list[dict[str, Any]],
    headers: dict[str, str],
) -> int:
    """Update records in HubSpot. Returns count of successfully updated records."""
    if not inputs:
        return 0

    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/{entity_type}/batch/update"
    updated = 0

    for i in range(0, len(inputs), HUBSPOT_BATCH_LIMIT):
        batch = inputs[i : i + HUBSPOT_BATCH_LIMIT]
        resp = requests.post(
            url, headers=headers, json={"inputs": batch}, timeout=HUBSPOT_TIMEOUT
        )

        if resp.status_code == 401:
            raise RuntimeError(f"HubSpot authentication failed (401): {resp.text[:200]}")
        if resp.status_code == 429:
            raise RuntimeError(f"HubSpot rate limit exceeded (429): {resp.text[:200]}")

        if resp.status_code == 200:
            batch_updated = len(resp.json().get("results", []))
            updated += batch_updated
            logger.info(
                "Updated %d/%d %s (batch %d)",
                batch_updated,
                len(batch),
                entity_type,
                i // HUBSPOT_BATCH_LIMIT + 1,
            )
        else:
            logger.error(
                "Batch update failed (%d): %s", resp.status_code, resp.text[:300]
            )

    return updated


@dlt.destination(
    batch_size=HUBSPOT_BATCH_LIMIT,
    loader_file_format="jsonl",
    name="hubspot",
    naming_convention="direct",
    skip_dlt_columns_and_tables=True,
    max_table_nesting=0,
)
def hubspot_destination(
    items: list[dict[str, Any]],
    table: dict[str, Any],
    api_key: str = dlt.secrets.value,
) -> None:
    """Write a batch of items to HubSpot using the upsert pattern.

    Routing is determined by ``table["name"]`` which maps to the DLT resource
    name (``contacts`` or ``companies``).
    """
    table_name = table["name"]

    if table_name not in ENTITY_CONFIG:
        logger.debug("Skipping unknown table: %s", table_name)
        return

    entity_type, search_property = ENTITY_CONFIG[table_name]
    headers = _hubspot_headers(api_key)

    # Build properties dicts — filter out None/empty values, stringify
    records: list[dict[str, str]] = []
    for item in items:
        props = {
            k: str(v)
            for k, v in item.items()
            if v is not None and str(v).strip() != ""
        }
        if props:
            records.append(props)

    if not records:
        return

    # Step 1: Collect identifier values for search
    search_values = [
        r[search_property]
        for r in records
        if search_property in r and r[search_property]
    ]

    # Step 2: Search for existing records
    existing = {}
    if search_values:
        existing = _batch_search(entity_type, search_property, search_values, headers)
    logger.info(
        "Found %d existing %s out of %d total",
        len(existing),
        entity_type,
        len(records),
    )

    # Step 3: Split into create / update
    to_create: list[dict[str, Any]] = []
    to_update: list[dict[str, Any]] = []

    for record in records:
        identifier = record.get(search_property, "")
        if identifier in existing:
            to_update.append({"id": existing[identifier], "properties": record})
        else:
            to_create.append({"properties": record})

    # Step 4: Execute batch operations
    created = _batch_create(entity_type, to_create, headers)
    updated = _batch_update(entity_type, to_update, headers)

    logger.info(
        "%s batch complete: %d created, %d updated, %d total",
        entity_type,
        created,
        updated,
        len(records),
    )
