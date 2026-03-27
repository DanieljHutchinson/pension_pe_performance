from __future__ import annotations

import json

import httpx
from bs4 import BeautifulSoup

from app.config import SOURCES, USER_AGENT
from app.ingestion.base import BaseScraper
from app.ingestion.utils import parse_currency, parse_multiple, parse_percentage, parse_date_from_text
from app.models import RawRecord


class CalPERSScraper(BaseScraper):
    pension_fund = "CalPERS"

    def __init__(self):
        self.source_url = SOURCES["calpers"]["url"]
        self.html: str | None = None

    def fetch(self) -> None:
        response = httpx.get(
            self.source_url,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
            timeout=60,
        )
        response.raise_for_status()
        self.html = response.text

    def parse(self) -> list[RawRecord]:
        if not self.html:
            raise RuntimeError("No HTML fetched. Call fetch() first.")

        soup = BeautifulSoup(self.html, "html.parser")

        # Extract as_of_date from page content
        as_of_date = self._extract_date(soup)

        # Find the performance table
        table = self._find_table(soup)
        if not table:
            raise RuntimeError("Could not find performance data table on CalPERS page.")

        records = []
        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all(["td", "th"])
            cell_texts = [c.get_text(strip=True) for c in cells]

            # Skip header rows and empty rows
            if len(cell_texts) < 6:
                continue
            if cell_texts[0].lower() in ("fund", "fund name", ""):
                continue
            if "total" in cell_texts[0].lower() and "fund" not in cell_texts[0].lower():
                continue

            record = self._parse_row(cell_texts, as_of_date)
            if record:
                records.append(record)

        return records

    def _extract_date(self, soup: BeautifulSoup) -> str:
        """Try to find 'As of ...' date in the page."""
        text = soup.get_text()
        date = parse_date_from_text(text)
        return date or "2024-06-30"  # fallback to common CalPERS fiscal year end

    def _find_table(self, soup: BeautifulSoup):
        """Find the main data table. Try multiple strategies."""
        # Look for tables with performance-related headers
        for table in soup.find_all("table"):
            header_text = table.get_text().lower()
            if "vintage" in header_text or "capital committed" in header_text or "net irr" in header_text:
                return table

        # Fallback: largest table on the page
        tables = soup.find_all("table")
        if tables:
            return max(tables, key=lambda t: len(t.find_all("tr")))
        return None

    def _parse_row(self, cells: list[str], as_of_date: str) -> RawRecord | None:
        """Parse a table row into a RawRecord.

        Expected CalPERS columns:
        Fund | Vintage Year | Capital Committed | Cash In | Cash Out |
        Cash Out & Remaining Value | Net IRR | Investment Multiple
        """
        try:
            fund_name = cells[0].strip()
            if not fund_name:
                return None

            vintage_year = None
            try:
                vintage_year = int(cells[1].strip())
            except (ValueError, IndexError):
                pass

            commitment = parse_currency(cells[2]) if len(cells) > 2 else None
            cash_in = parse_currency(cells[3]) if len(cells) > 3 else None
            cash_out = parse_currency(cells[4]) if len(cells) > 4 else None

            # NAV = (Cash Out & Remaining Value) - Cash Out
            cash_out_plus_remaining = parse_currency(cells[5]) if len(cells) > 5 else None
            nav = None
            if cash_out_plus_remaining is not None and cash_out is not None:
                nav = cash_out_plus_remaining - cash_out

            net_irr = parse_percentage(cells[6]) if len(cells) > 6 else None
            inv_multiple = parse_multiple(cells[7]) if len(cells) > 7 else None

            raw_data = {
                "cells": cells,
                "fund_name": fund_name,
            }

            return RawRecord(
                pension_fund=self.pension_fund,
                raw_fund_name=fund_name,
                vintage_year=vintage_year,
                commitment_usd=commitment,
                contributed_usd=cash_in,
                distributed_usd=cash_out,
                nav_usd=nav,
                net_irr=net_irr,
                investment_multiple=inv_multiple,
                as_of_date=as_of_date,
                source_url=self.source_url,
                raw_json=json.dumps(raw_data),
            )
        except Exception:
            return None
