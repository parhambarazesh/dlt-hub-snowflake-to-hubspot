"""Main pipeline: Snowflake → HubSpot via DLT.

Usage:
    python -m src.pipeline                  # sync all (contacts + companies)
    python -m src.pipeline contacts         # sync contacts only
    python -m src.pipeline companies        # sync companies only
    python -m src.pipeline --limit 10       # limit rows per table (for testing)
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import dlt
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent

# Support direct script execution (`python src/pipeline.py`) by ensuring the
# repository root is on sys.path so `import src...` succeeds.
if __package__ in (None, ""):
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

from src.hubspot_destination import hubspot_destination
from src.snowflake_source import snowflake_table

logger = logging.getLogger(__name__)


def _load_env_files() -> None:
    """Load environment variables.

    Priority order:
    1) `.env`
    2) `.env.template` (fallback when users keep credentials there)
    """
    load_dotenv(dotenv_path=REPO_ROOT / ".env", override=False)
    load_dotenv(dotenv_path=REPO_ROOT / ".env.template", override=False)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Set it in .env (recommended) or .env.template."
        )
    return value


def _normalize_snowflake_account(account: str) -> str:
    """Normalize account value to Snowflake account identifier format.

    Accepted inputs:
    - account locator only: `xy12345`
    - account + region: `xy12345.eu-west-1`
    - full host: `xy12345.eu-west-1.snowflakecomputing.com`
    - full URL: `https://xy12345.eu-west-1.snowflakecomputing.com`
    """
    value = account.strip()

    if value.startswith("https://"):
        value = value[len("https://") :]
    elif value.startswith("http://"):
        value = value[len("http://") :]

    value = value.rstrip("/")

    if value.endswith(".snowflakecomputing.com"):
        value = value[: -len(".snowflakecomputing.com")]

    # Many templates use a literal `.region` placeholder. Strip it so values
    # like `xy12345.region.snowflakecomputing.com` become `xy12345`.
    if ".region." in value.lower():
        value = value.replace(".region.", ".")
        logger.warning(
            "SNOWFLAKE_ACCOUNT contained '.region' placeholder; normalized to '%s'. "
            "Set the exact account identifier in .env for production.",
            value,
        )
    elif value.lower().endswith(".region"):
        value = value[: -len(".region")]
        logger.warning(
            "SNOWFLAKE_ACCOUNT contained '.region' placeholder; normalized to '%s'.",
            value,
        )

    if ".region" in value.lower():
        raise RuntimeError(
            "SNOWFLAKE_ACCOUNT still looks invalid after normalization. "
            "Use account locator or account.region, for example: "
            "'xy12345' or 'xy12345.eu-west-1'."
        )

    return value


def _resolve_repo_path(path_str: str) -> str:
    """Resolve path relative to repo root unless it is already absolute."""
    path = Path(path_str)
    if path.is_absolute():
        return str(path)
    return str((REPO_ROOT / path).resolve())


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync data from Snowflake to HubSpot",
    )
    parser.add_argument(
        "entity",
        nargs="?",
        default="all",
        choices=["contacts", "companies", "all"],
        help="Which entity to sync (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of rows fetched per table (for testing)",
    )
    return parser.parse_args(argv)


def _snowflake_config() -> dict:
    """Read Snowflake config from environment variables."""
    private_key_path = os.environ.get(
        "SNOWFLAKE_PRIVATE_KEY_PATH",
        "src/rsa_key.p8",
    )
    return {
        "account": _normalize_snowflake_account(_require_env("SNOWFLAKE_ACCOUNT")),
        "user": _require_env("SNOWFLAKE_USER"),
        "database": _require_env("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "warehouse": _require_env("SNOWFLAKE_WAREHOUSE"),
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "private_key_path": _resolve_repo_path(private_key_path),
    }


def run(entity: str = "all", limit: int | None = None) -> None:
    """Run the Snowflake → HubSpot pipeline."""
    _load_env_files()

    sf_config = _snowflake_config()
    hubspot_api_key = _require_env("HUBSPOT_API_KEY")

    pipeline = dlt.pipeline(
        pipeline_name="snowflake_to_hubspot",
        destination=hubspot_destination(api_key=hubspot_api_key),
    )

    entities_to_sync = (
        ["contacts", "companies"] if entity == "all" else [entity]
    )

    table_env_map = {
        "contacts": "SNOWFLAKE_CONTACTS_TABLE",
        "companies": "SNOWFLAKE_COMPANIES_TABLE",
    }

    for entity_name in entities_to_sync:
        env_var = table_env_map[entity_name]
        table_name = os.environ.get(env_var)
        if not table_name:
            logger.warning(
                "Skipping %s: %s environment variable not set",
                entity_name,
                env_var,
            )
            continue

        logger.info("Syncing %s from table %s …", entity_name, table_name)

        resource = snowflake_table(
            table_name=table_name,
            resource_name=entity_name,
            limit=limit,
            **sf_config,
        )

        info = pipeline.run(resource, table_name=entity_name)
        logger.info("Load info for %s:\n%s", entity_name, info)

    print("\nPipeline run complete.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = _parse_args()
    run(entity=args.entity, limit=args.limit)


if __name__ == "__main__":
    main()
