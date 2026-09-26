"""Grants a Databricks App's service principal read access to a catalog.

Apps run as their own service principal, which needs USE CATALOG, USE SCHEMA and SELECT on the data it queries.
Run once after the app is first deployed.

Usage:
    python scripts/grant_app_access.py --app metlink-departures-sim --catalog metlink_sim
"""

import argparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--app", required=True, help="app name, e.g. metlink-departures-dev")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--warehouse-id", help="SQL warehouse to run the grants on (default: first available)")
    parser.add_argument("--profile", help="Databricks CLI profile")
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.profile)
    principal = w.apps.get(args.app).service_principal_client_id
    warehouse_id = args.warehouse_id or next(iter(w.warehouses.list())).id
    for statement in (
        f"GRANT USE CATALOG ON CATALOG `{args.catalog}` TO `{principal}`",
        f"GRANT USE SCHEMA, SELECT ON CATALOG `{args.catalog}` TO `{principal}`",
    ):
        result = w.statement_execution.execute_statement(
            statement=statement, warehouse_id=warehouse_id, wait_timeout="50s"
        )
        if result.status.state != StatementState.SUCCEEDED:
            raise SystemExit(f"{statement} failed: {result.status.error.message if result.status.error else ''}")
    print(f"granted read access on {args.catalog} to app {args.app} ({principal})")


if __name__ == "__main__":
    main()
