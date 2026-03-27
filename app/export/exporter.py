from __future__ import annotations

import csv
import json
import io

from app.database import get_connection


EXPORT_COLUMNS = [
    "fund_name", "gp_name", "pension_fund", "vintage_year", "strategy",
    "commitment_usd", "contributed_usd", "distributed_usd", "nav_usd",
    "tvpi", "dpi", "rvpi", "net_irr", "as_of_date", "source_url",
]


def _build_query(filters: dict | None = None) -> tuple[str, list]:
    """Build SQL query with optional filters."""
    clauses = []
    params = []

    if filters:
        if filters.get("pension_fund"):
            clauses.append("pension_fund = ?")
            params.append(filters["pension_fund"])
        if filters.get("gp_name"):
            clauses.append("gp_name LIKE ?")
            params.append(f"%{filters['gp_name']}%")
        if filters.get("strategy"):
            clauses.append("strategy = ?")
            params.append(filters["strategy"])
        if filters.get("vintage_min"):
            clauses.append("vintage_year >= ?")
            params.append(filters["vintage_min"])
        if filters.get("vintage_max"):
            clauses.append("vintage_year <= ?")
            params.append(filters["vintage_max"])
        if filters.get("irr_min") is not None:
            clauses.append("net_irr >= ?")
            params.append(filters["irr_min"])
        if filters.get("irr_max") is not None:
            clauses.append("net_irr <= ?")
            params.append(filters["irr_max"])
        if filters.get("tvpi_min") is not None:
            clauses.append("tvpi >= ?")
            params.append(filters["tvpi_min"])
        if filters.get("tvpi_max") is not None:
            clauses.append("tvpi <= ?")
            params.append(filters["tvpi_max"])

    where = " AND ".join(clauses) if clauses else "1=1"
    cols = ", ".join(EXPORT_COLUMNS)
    query = f"SELECT {cols} FROM funds WHERE {where} ORDER BY gp_name, fund_name"
    return query, params


def export_csv(filters: dict | None = None) -> str:
    """Export filtered fund data as CSV string."""
    query, params = _build_query(filters)
    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(EXPORT_COLUMNS)
    for row in rows:
        writer.writerow([row[col] for col in EXPORT_COLUMNS])
    return output.getvalue()


def export_json(filters: dict | None = None) -> list[dict]:
    """Export filtered fund data as list of dicts."""
    query, params = _build_query(filters)
    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()

    return [{col: row[col] for col in EXPORT_COLUMNS} for row in rows]
