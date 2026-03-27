#!/usr/bin/env python3
"""CLI for pension PE performance data management."""

import argparse
import sys
import json

from app.database import init_db
from app.ingestion.calpers import CalPERSScraper
from app.normalization.normalize import ingest_records
from app.export.exporter import export_csv, export_json

SCRAPERS = {
    "calpers": CalPERSScraper,
}


def cmd_refresh(args):
    init_db()
    sources = list(SCRAPERS.keys()) if args.source == "all" else [args.source]

    for src in sources:
        scraper_cls = SCRAPERS.get(src)
        if not scraper_cls:
            print(f"Unknown source: {src}")
            continue
        print(f"Refreshing {src}...")
        try:
            scraper = scraper_cls()
            raw_records = scraper.run()
            normalized = ingest_records(raw_records)
            print(f"  {src}: {len(normalized)} records ingested")
        except Exception as e:
            print(f"  {src}: ERROR - {e}")


def cmd_export(args):
    init_db()
    if args.format == "csv":
        data = export_csv()
        if args.output:
            with open(args.output, "w") as f:
                f.write(data)
            print(f"Exported CSV to {args.output}")
        else:
            print(data)
    elif args.format == "json":
        data = export_json()
        output = json.dumps(data, indent=2)
        if args.output:
            with open(args.output, "w") as f:
                f.write(output)
            print(f"Exported JSON to {args.output}")
        else:
            print(output)


def cmd_stats(args):
    init_db()
    from app.database import get_connection
    import statistics

    with get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM funds").fetchone()[0]
        irrs = [r[0] for r in conn.execute(
            "SELECT net_irr FROM funds WHERE net_irr IS NOT NULL"
        ).fetchall()]
        tvpis = [r[0] for r in conn.execute(
            "SELECT tvpi FROM funds WHERE tvpi IS NOT NULL"
        ).fetchall()]
        pensions = conn.execute(
            "SELECT pension_fund, COUNT(*) FROM funds GROUP BY pension_fund"
        ).fetchall()

    print(f"Total funds: {total}")
    if irrs:
        print(f"IRR  - Median: {statistics.median(irrs):.2f}%  Mean: {statistics.mean(irrs):.2f}%")
    if tvpis:
        print(f"TVPI - Median: {statistics.median(tvpis):.2f}x  Mean: {statistics.mean(tvpis):.2f}x")
    print("\nBy pension fund:")
    for row in pensions:
        print(f"  {row[0]}: {row[1]} funds")


def cmd_serve(args):
    import uvicorn
    init_db()
    uvicorn.run("app.main:app", host=args.host, port=args.port, reload=args.reload)


def main():
    parser = argparse.ArgumentParser(description="Pension PE Performance CLI")
    sub = parser.add_subparsers(dest="command")

    p_refresh = sub.add_parser("refresh", help="Refresh data from sources")
    p_refresh.add_argument("--source", default="all", help="Source to refresh (or 'all')")

    p_export = sub.add_parser("export", help="Export data")
    p_export.add_argument("--format", choices=["csv", "json"], default="csv")
    p_export.add_argument("--output", "-o", help="Output file path")

    sub.add_parser("stats", help="Show summary statistics")

    p_serve = sub.add_parser("serve", help="Start the web server")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.add_argument("--reload", action="store_true")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    {"refresh": cmd_refresh, "export": cmd_export, "stats": cmd_stats, "serve": cmd_serve}[args.command](args)


if __name__ == "__main__":
    main()
