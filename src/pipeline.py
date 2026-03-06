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

from src.hubspot_destination import hubspot_destination
from src.snowflake_source import snowflake_table

logger = logging.getLogger(__name__)


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
    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "private_key_path": os.environ.get(
            "SNOWFLAKE_PRIVATE_KEY_PATH",
            str(Path(__file__).parent / "rsa_key.p8"),
        ),
    }


def run(entity: str = "all", limit: int | None = None) -> None:
    """Run the Snowflake → HubSpot pipeline."""
    load_dotenv()

    sf_config = _snowflake_config()

    pipeline = dlt.pipeline(
        pipeline_name="snowflake_to_hubspot",
        destination=hubspot_destination(api_key=os.environ["HUBSPOT_API_KEY"]),
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
