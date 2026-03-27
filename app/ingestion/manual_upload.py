from __future__ import annotations

import json
from io import BytesIO

import pandas as pd

from app.models import RawRecord
from app.ingestion.utils import parse_currency, parse_percentage, parse_multiple


# Column name mappings - maps common variations to our standard names
COLUMN_ALIASES = {
    "fund_name": ["fund_name", "fund", "name", "fund name", "description"],
    "vintage_year": ["vintage_year", "vintage", "vy", "vintage year", "year"],
    "commitment_usd": ["commitment_usd", "commitment", "capital committed", "committed"],
    "contributed_usd": ["contributed_usd", "contributed", "capital contributed", "cash in", "paid-in", "paid in"],
    "distributed_usd": ["distributed_usd", "distributed", "capital distributed", "cash out", "distributions"],
    "nav_usd": ["nav_usd", "nav", "market value", "remaining value", "fair value"],
    "net_irr": ["net_irr", "irr", "net irr", "since inception irr", "si irr"],
    "investment_multiple": ["investment_multiple", "multiple", "tvpi", "investment multiple"],
}


def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map source column names to standardized names."""
    col_map = {}
    lower_cols = {c.lower().strip(): c for c in df.columns}

    for standard_name, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias.lower() in lower_cols:
                col_map[lower_cols[alias.lower()]] = standard_name
                break

    return df.rename(columns=col_map)


def parse_upload(file_bytes: bytes, filename: str, pension_fund: str, as_of_date: str) -> list[RawRecord]:
    """Parse an uploaded CSV or Excel file into RawRecords."""
    buf = BytesIO(file_bytes)

    if filename.endswith((".xlsx", ".xls")):
        df = pd.read_excel(buf, engine="openpyxl")
    elif filename.endswith(".csv"):
        df = pd.read_csv(buf)
    else:
        raise ValueError(f"Unsupported file type: {filename}")

    df = _map_columns(df)
    records = []

    for _, row in df.iterrows():
        fund_name = str(row.get("fund_name", "")).strip()
        if not fund_name:
            continue

        vintage = None
        raw_vintage = row.get("vintage_year")
        if pd.notna(raw_vintage):
            try:
                vintage = int(float(raw_vintage))
            except (ValueError, TypeError):
                pass

        raw_data = {k: (str(v) if pd.notna(v) else None) for k, v in row.items()}

        records.append(RawRecord(
            pension_fund=pension_fund,
            raw_fund_name=fund_name,
            vintage_year=vintage,
            commitment_usd=_safe_float(row.get("commitment_usd")),
            contributed_usd=_safe_float(row.get("contributed_usd")),
            distributed_usd=_safe_float(row.get("distributed_usd")),
            nav_usd=_safe_float(row.get("nav_usd")),
            net_irr=_safe_float(row.get("net_irr")),
            investment_multiple=_safe_float(row.get("investment_multiple")),
            as_of_date=as_of_date,
            source_url=f"upload:{filename}",
            raw_json=json.dumps(raw_data),
        ))

    return records


def _safe_float(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        result = parse_currency(val)
        if result is not None:
            return result
        result = parse_percentage(val)
        if result is not None:
            return result
        return parse_multiple(val)
    return None
