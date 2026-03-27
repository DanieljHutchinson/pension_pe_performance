from __future__ import annotations

import json
import re

from app.database import get_connection
from app.models import RawRecord, FundRecord
from app.normalization.gp_extractor import extract_gp_name
from app.normalization.strategy_classifier import classify_strategy


def normalize_fund_name(name: str) -> str:
    """Clean and standardize fund name."""
    name = name.strip()
    # Normalize L.P. variations
    name = re.sub(r"\bL\.\s*P\.\s*$", "L.P.", name)
    name = re.sub(r"\bLP\s*$", "L.P.", name)
    # Normalize LLC variations
    name = re.sub(r"\bL\.\s*L\.\s*C\.\s*$", "LLC", name)
    # Normalize whitespace
    name = re.sub(r"\s+", " ", name)
    return name


def generate_dedup_key(gp_name: str, fund_name: str, vintage_year: int | None) -> str:
    """Generate a normalized key for deduplication."""
    parts = []
    for text in (gp_name, fund_name):
        cleaned = text.lower().strip()
        # Remove entity suffixes and punctuation
        cleaned = re.sub(r"[,.'\"()\-]", "", cleaned)
        cleaned = re.sub(r"\b(lp|llc|inc|ltd|corp|co)\b", "", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        parts.append(cleaned)
    vy = str(vintage_year) if vintage_year else "none"
    return f"{parts[0]}|{parts[1]}|{vy}"


def compute_multiples(contributed: float | None, distributed: float | None, nav: float | None):
    """Compute TVPI, DPI, RVPI from cash flow data."""
    tvpi = dpi = rvpi = None
    if contributed and contributed > 0:
        dist = distributed or 0
        n = nav or 0
        tvpi = round((dist + n) / contributed, 2)
        dpi = round(dist / contributed, 2)
        rvpi = round(n / contributed, 2)
    return tvpi, dpi, rvpi


def normalize_record(raw: RawRecord) -> FundRecord:
    """Transform a raw record into a normalized fund record."""
    fund_name = normalize_fund_name(raw.raw_fund_name)
    gp_name = extract_gp_name(fund_name)
    strategy = classify_strategy(fund_name)

    tvpi, dpi, rvpi = compute_multiples(
        raw.contributed_usd, raw.distributed_usd, raw.nav_usd
    )

    # If source provides TVPI directly and we couldn't compute it, use theirs
    if tvpi is None and raw.investment_multiple is not None:
        tvpi = raw.investment_multiple

    dedup_key = generate_dedup_key(gp_name, fund_name, raw.vintage_year)

    return FundRecord(
        fund_name=fund_name,
        gp_name=gp_name,
        pension_fund=raw.pension_fund,
        vintage_year=raw.vintage_year,
        strategy=strategy,
        commitment_usd=raw.commitment_usd,
        contributed_usd=raw.contributed_usd,
        distributed_usd=raw.distributed_usd,
        nav_usd=raw.nav_usd,
        tvpi=tvpi,
        dpi=dpi,
        rvpi=rvpi,
        net_irr=raw.net_irr,
        as_of_date=raw.as_of_date,
        source_url=raw.source_url,
        dedup_key=dedup_key,
    )


def save_raw_records(records: list[RawRecord]):
    """Insert raw records into the raw_records table."""
    with get_connection() as conn:
        for r in records:
            conn.execute(
                """INSERT INTO raw_records
                   (pension_fund, raw_fund_name, vintage_year, commitment_usd,
                    contributed_usd, distributed_usd, nav_usd, net_irr,
                    investment_multiple, as_of_date, source_url, raw_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    r.pension_fund, r.raw_fund_name, r.vintage_year,
                    r.commitment_usd, r.contributed_usd, r.distributed_usd,
                    r.nav_usd, r.net_irr, r.investment_multiple,
                    r.as_of_date, r.source_url, r.raw_json,
                ),
            )


def save_normalized_records(records: list[FundRecord]):
    """Upsert normalized records into the funds table."""
    with get_connection() as conn:
        for r in records:
            conn.execute(
                """INSERT INTO funds
                   (fund_name, gp_name, pension_fund, vintage_year, strategy,
                    commitment_usd, contributed_usd, distributed_usd, nav_usd,
                    tvpi, dpi, rvpi, net_irr, as_of_date, source_url, dedup_key)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(dedup_key, pension_fund, as_of_date) DO UPDATE SET
                    fund_name=excluded.fund_name,
                    gp_name=excluded.gp_name,
                    strategy=excluded.strategy,
                    commitment_usd=excluded.commitment_usd,
                    contributed_usd=excluded.contributed_usd,
                    distributed_usd=excluded.distributed_usd,
                    nav_usd=excluded.nav_usd,
                    tvpi=excluded.tvpi,
                    dpi=excluded.dpi,
                    rvpi=excluded.rvpi,
                    net_irr=excluded.net_irr,
                    source_url=excluded.source_url,
                    updated_at=datetime('now')""",
                (
                    r.fund_name, r.gp_name, r.pension_fund, r.vintage_year,
                    r.strategy, r.commitment_usd, r.contributed_usd,
                    r.distributed_usd, r.nav_usd, r.tvpi, r.dpi, r.rvpi,
                    r.net_irr, r.as_of_date, r.source_url, r.dedup_key,
                ),
            )


def ingest_records(raw_records: list[RawRecord]) -> list[FundRecord]:
    """Full pipeline: save raw, normalize, save normalized."""
    save_raw_records(raw_records)
    normalized = [normalize_record(r) for r in raw_records]
    save_normalized_records(normalized)
    return normalized
