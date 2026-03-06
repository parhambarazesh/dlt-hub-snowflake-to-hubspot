"""DLT resource that reads from Snowflake via the SQL REST API.

Uses the manual @dlt.resource approach (ADR-001 fallback) because Snowflake's
partition-based pagination (POST initial query, then GET each partition) does
not fit DLT's declarative rest_api config cleanly.

Data flows:
  POST /api/v2/statements  →  first partition + partitionInfo
  GET  /api/v2/statements/{handle}?partition=1  →  partition 1
  GET  /api/v2/statements/{handle}?partition=2  →  partition 2
  ...
"""

import logging
import os
import time
from typing import Any, Generator

import dlt
import requests

from src.auth import SnowflakeJWTAuth

logger = logging.getLogger(__name__)

SNOWFLAKE_API_TIMEOUT = 120  # seconds


def _build_base_url(account: str) -> str:
    """Build the Snowflake REST API base URL from the account identifier."""
    # Account may already contain the full host or just the locator
    if ".snowflakecomputing.com" in account.lower():
        host = account.rstrip("/")
    else:
        host = f"{account}.snowflakecomputing.com"
    return f"https://{host}"


def _rows_to_dicts(
    row_types: list[dict[str, Any]],
    data: list[list[Any]],
) -> list[dict[str, Any]]:
    """Convert Snowflake's array-of-arrays format into list of dicts.

    Column names are lowercased so they match HubSpot property naming.
    """
    col_names = [col["name"].lower() for col in row_types]
    return [dict(zip(col_names, row)) for row in data]


def _submit_query(
    session: requests.Session,
    base_url: str,
    statement: str,
    database: str,
    schema: str,
    warehouse: str,
    role: str,
) -> dict[str, Any]:
    """Submit an async SQL query via Snowflake REST API and poll until done."""
    url = f"{base_url}/api/v2/statements"
    body = {
        "statement": statement,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
        "role": role,
        "timeout": SNOWFLAKE_API_TIMEOUT,
    }

    resp = session.post(url, json=body, timeout=SNOWFLAKE_API_TIMEOUT)
    resp.raise_for_status()
    result = resp.json()

    # Poll if the query is still running (202 = pending)
    statement_handle = result.get("statementHandle")
    while resp.status_code == 202 or result.get("code") == "333334":
        logger.info("Query still running, polling… (handle=%s)", statement_handle)
        time.sleep(2)
        resp = session.get(
            f"{url}/{statement_handle}",
            timeout=SNOWFLAKE_API_TIMEOUT,
        )
        resp.raise_for_status()
        result = resp.json()

    return result


def _fetch_partition(
    session: requests.Session,
    base_url: str,
    statement_handle: str,
    partition: int,
) -> dict[str, Any]:
    """Fetch a single result partition by index."""
    url = f"{base_url}/api/v2/statements/{statement_handle}"
    resp = session.get(
        url,
        params={"partition": partition},
        timeout=SNOWFLAKE_API_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


@dlt.resource(write_disposition="replace")
def snowflake_table(
    table_name: str,
    resource_name: str | None = None,
    limit: int | None = None,
    account: str = dlt.config.value,
    user: str = dlt.config.value,
    database: str = dlt.config.value,
    schema: str = dlt.config.value,
    warehouse: str = dlt.config.value,
    role: str = dlt.config.value,
    private_key_path: str = dlt.config.value,
) -> Generator[list[dict[str, Any]], None, None]:
    """Yield rows from a Snowflake table via the SQL REST API.

    Each yield is one partition worth of dict-rows (lowercased keys).
    """
    # Override DLT resource name so each table gets its own destination table
    if resource_name:
        snowflake_table.__qualname__ = resource_name  # type: ignore[attr-defined]

    base_url = _build_base_url(account)
    auth = SnowflakeJWTAuth(account=account, user=user, private_key_path=private_key_path)

    session = requests.Session()
    session.auth = auth
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
    })

    statement = f"SELECT * FROM {database}.{schema}.{table_name}"
    if limit:
        statement += f" LIMIT {limit}"

    logger.info("Submitting query: %s", statement)
    result = _submit_query(session, base_url, statement, database, schema, warehouse, role)

    # Extract column metadata from the first response
    row_types = result["resultSetMetaData"]["rowType"]
    total_rows = result["resultSetMetaData"].get("numRows", 0)
    logger.info("Query returned %s total rows", total_rows)

    # Yield first partition
    data = result.get("data", [])
    if data:
        yield _rows_to_dicts(row_types, data)

    # Fetch remaining partitions
    partition_info = result.get("resultSetMetaData", {}).get("partitionInfo", [])
    statement_handle = result.get("statementHandle")
    if statement_handle and len(partition_info) > 1:
        for i in range(1, len(partition_info)):
            logger.info(
                "Fetching partition %d/%d (handle=%s)",
                i + 1,
                len(partition_info),
                statement_handle,
            )
            part_result = _fetch_partition(session, base_url, statement_handle, i)
            part_data = part_result.get("data", [])
            if part_data:
                yield _rows_to_dicts(row_types, part_data)


def contacts_resource(
    table_name: str = dlt.config.value,
    limit: int | None = None,
    **kwargs: Any,
):
    """DLT resource for the contacts table."""
    return snowflake_table(
        table_name=table_name,
        resource_name="contacts",
        limit=limit,
        **kwargs,
    )


def companies_resource(
    table_name: str = dlt.config.value,
    limit: int | None = None,
    **kwargs: Any,
):
    """DLT resource for the companies table."""
    return snowflake_table(
        table_name=table_name,
        resource_name="companies",
        limit=limit,
        **kwargs,
    )
