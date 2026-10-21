#!/usr/bin/env python3
"""Report which supermarket Bronze tables hold a complete load for the target date.

A load is complete when the target date has at least `--min-share` of the
supermarket's usual volume (the median daily row count over the previous
`--baseline-days`). By default the check only blocks the build when no
supermarket is complete: dbt's silver_product_current keeps each supermarket on
its own latest complete date, so one blocked scraper must not freeze the other
three. `--require-all` restores the old all-or-nothing gate.
"""

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
    baseline_rows: int | None
    ready: bool
    reason: str | None = None
    query_error: str | None = None


def evaluate_readiness(
    rows: Iterable[dict[str, Any]], target_date: dt.date, min_rows: int, min_share: float
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
        baseline = row.get("baseline_rows")
        baseline_rows = int(baseline) if baseline is not None else None
        reasons = []
        if latest_text != target_date.isoformat():
            reasons.append("latest_date_is_not_target_date")
        if target_rows < min_rows:
            reasons.append("target_rows_below_minimum")
        if baseline_rows and target_rows < min_share * baseline_rows:
            reasons.append("target_rows_below_usual_volume")
        results.append(
            TableReadiness(
                supermarket=str(row["supermarket"]),
                table=str(row["table"]),
                latest_date=latest_text,
                target_rows=target_rows,
                baseline_rows=baseline_rows,
                ready=not reasons,
                reason=",".join(reasons) or None,
                query_error=row.get("query_error"),
            )
        )
    return results


def overall_ready(evaluated: list[TableReadiness], require_all: bool) -> bool:
    if require_all:
        return len(evaluated) == len(SUPERMARKETS) and all(item.ready for item in evaluated)
    return any(item.ready for item in evaluated)


def _parse_args() -> argparse.Namespace:
    today = dt.date.today().isoformat()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT"))
    parser.add_argument("--dataset", default=os.getenv("BRONZE_DATASET", "dwh_bronze_dev"))
    parser.add_argument("--target-date", default=os.getenv("TARGET_DATE", today))
    parser.add_argument("--min-rows", type=int, default=int(os.getenv("BRONZE_MIN_ROWS", "1")))
    parser.add_argument("--min-share", type=float, default=float(os.getenv("BRONZE_MIN_SHARE", "0.5")))
    parser.add_argument("--baseline-days", type=int, default=int(os.getenv("BRONZE_BASELINE_DAYS", "14")))
    parser.add_argument(
        "--require-all",
        action="store_true",
        default=os.getenv("READINESS_REQUIRE_ALL", "false").lower() == "true",
    )
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
        query_parameters=[
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
            bigquery.ScalarQueryParameter("baseline_days", "INT64", args.baseline_days),
        ]
    )
    rows: list[dict[str, Any]] = []
    for supermarket in SUPERMARKETS:
        table = f"bronze_{supermarket}_products_p"
        query = f"""
            WITH daily AS (
                SELECT date, COUNT(*) AS n
                FROM `{args.project}.{args.dataset}.{table}`
                WHERE date BETWEEN DATE_SUB(@target_date, INTERVAL @baseline_days DAY) AND @target_date
                GROUP BY date
            )
            SELECT
                (SELECT MAX(date) FROM `{args.project}.{args.dataset}.{table}`) AS latest_date,
                COALESCE((SELECT n FROM daily WHERE date = @target_date), 0) AS target_rows,
                (SELECT APPROX_QUANTILES(n, 2)[OFFSET(1)] FROM daily WHERE date < @target_date) AS baseline_rows
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
                    "baseline_rows": None,
                    "query_error": str(exc),
                }
            )

    evaluated = evaluate_readiness(rows, target_date, args.min_rows, args.min_share)
    payload = {
        "target_date": target_date.isoformat(),
        "min_rows": args.min_rows,
        "min_share": args.min_share,
        "require_all": args.require_all,
        "incomplete": [item.supermarket for item in evaluated if not item.ready],
        "ready": overall_ready(evaluated, args.require_all),
        "tables": [asdict(item) for item in evaluated],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0 if payload["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
