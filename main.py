#!/usr/bin/env python3
"""Orchestrates daily supermarket scrapers."""

import argparse
import os
import subprocess
import sys
from typing import List


def run_carrefour(args: argparse.Namespace) -> None:
    cmd: List[str] = [
        sys.executable,
        "scrape_carrefour_food.py",
        "--out-dir",
        args.out_dir,
        "--sleep",
        str(args.sleep),
    ]

    if args.max_categories is not None:
        cmd += ["--max-categories", str(args.max_categories)]
    if args.max_pages is not None:
        cmd += ["--max-pages", str(args.max_pages)]
    if args.max_products is not None:
        cmd += ["--max-products", str(args.max_products)]
    if args.allow_duplicates:
        cmd += ["--allow-duplicates"]

    print("[RUN] carrefour")
    subprocess.run(cmd, check=True, env=os.environ.copy())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="files", help="Output directory")
    parser.add_argument("--sleep", type=float, default=1.0, help="Sleep between requests (seconds)")
    parser.add_argument("--max-categories", type=int, default=None)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--max-products", type=int, default=None)
    parser.add_argument("--allow-duplicates", action="store_true")
    parser.add_argument(
        "--only",
        default="",
        help="Comma-separated list of scrapers to run (e.g. carrefour)",
    )
    args = parser.parse_args()

    only = {s.strip().lower() for s in args.only.split(",") if s.strip()}

    # Add new scrapers here as you grow
    if not only or "carrefour" in only:
        run_carrefour(args)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
