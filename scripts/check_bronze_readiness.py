#!/usr/bin/env python3
"""Validate that all supermarket Bronze tables contain a complete target date."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from dataclasses import asdict, dataclass
from typing import Any, Iterable

SUPERMARKETS = ("alcampo", "carrefour", "dia", "mercadona")


@dataclass(frozen=True)
class TableReadiness:
    supermarket: str
    table: str
    latest_date: str | None
    target_rows: int
    ready: bool
    reason: str | None = None
    query_error: str | None = None


def evaluate_readiness(
    rows: Iterable[dict[str, Any]], target_date: dt.date, min_rows: int
) -> list[TableReadiness]:
    """Evaluate query results without requiring a BigQuery client (easy to test)."""
    results: list[TableReadiness] = []
    for row in rows:
        latest = row.get("latest_date")
        if isinstance(latest, dt.date):
            latest_text = latest.isoformat()
        elif latest is None:
            latest_text = None
        else:
            latest_text = str(latest)
        target_rows = int(row.get("target_rows") or 0)
        reasons = []
        if latest_text != target_date.isoformat():
            reasons.append("latest_date_is_not_target_date")
        if target_rows < min_rows:
            reasons.append("target_rows_below_minimum")
        results.append(
            TableReadiness(
                supermarket=str(row["supermarket"]),
                table=str(row["table"]),
                latest_date=latest_text,
                target_rows=target_rows,
                ready=not reasons,
                reason=",".join(reasons) or None,
                query_error=row.get("query_error"),
            )
        )
    return results


def _parse_args() -> argparse.Namespace:
    today = dt.date.today().isoformat()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT"))
    parser.add_argument("--dataset", default=os.getenv("BRONZE_DATASET", "dwh_bronze_dev"))
    parser.add_argument("--target-date", default=os.getenv("TARGET_DATE", today))
    parser.add_argument("--min-rows", type=int, default=int(os.getenv("BRONZE_MIN_ROWS", "1")))
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if not args.project:
        print("ERROR: set --project, GCP_PROJECT_ID, or GOOGLE_CLOUD_PROJECT", file=sys.stderr)
        return 2
    try:
        target_date = dt.date.fromisoformat(args.target_date)
    except ValueError:
        print(f"ERROR: invalid --target-date: {args.target_date}", file=sys.stderr)
        return 2
    if args.min_rows < 1:
        print("ERROR: --min-rows must be at least 1", file=sys.stderr)
        return 2

    try:
        from google.cloud import bigquery
    except ImportError:
        print("ERROR: install google-cloud-bigquery to run the readiness check", file=sys.stderr)
        return 2

    client = bigquery.Client(project=args.project)
    query_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("target_date", "DATE", target_date)]
    )
    rows: list[dict[str, Any]] = []
    for supermarket in SUPERMARKETS:
        table = f"bronze_{supermarket}_products_p"
        query = f"""
            SELECT MAX(date) AS latest_date,
                   COUNTIF(date = @target_date) AS target_rows
            FROM `{args.project}.{args.dataset}.{table}`
        """
        try:
            result = next(iter(client.query(query, job_config=query_config).result()))
            rows.append({"supermarket": supermarket, "table": table, **dict(result.items())})
        except Exception as exc:  # BigQuery exposes useful table/query details in the exception.
            rows.append(
                {
                    "supermarket": supermarket,
                    "table": table,
                    "latest_date": None,
                    "target_rows": 0,
                    "query_error": str(exc),
                }
            )

    evaluated = evaluate_readiness(rows, target_date, args.min_rows)
    payload = {
        "target_date": target_date.isoformat(),
        "min_rows": args.min_rows,
        "ready": all(item.ready for item in evaluated) and len(evaluated) == len(SUPERMARKETS),
        "tables": [asdict(item) for item in evaluated],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0 if payload["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
