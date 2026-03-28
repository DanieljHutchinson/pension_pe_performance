from __future__ import annotations

import json
import re

import pdfplumber

from app.config import SOURCES
from app.ingestion.base import BaseScraper
from app.ingestion.utils import download_pdf
from app.models import RawRecord


# Column x-boundaries (derived from header positions in the PDF)
# Each column is defined as (start_x, end_x)
COLUMNS = {
    "fund_name":    (0, 260),
    "vintage_year": (260, 300),
    "strategy":     (300, 360),
    "committed":    (360, 425),
    "funded":       (425, 480),
    "distributed":  (480, 525),
    "market_value": (525, 575),
    "total_value":  (575, 640),
    "tvm":          (640, 685),
    "irr":          (685, 730),
}


def _collapse_chars(word_texts: list[str]) -> str:
    """Collapse character-spaced words: ['C','r','e','s','t'] -> 'Crest'."""
    return "".join(w.strip() for w in word_texts)


def _parse_number(text: str) -> float | None:
    if not text or text.strip() in ("-", "--", "", "NM", "$"):
        return None
    text = text.strip().replace("$", "").replace(",", "").replace(" ", "")
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]
    if text.startswith("-"):
        negative = True
        text = text[1:]
    try:
        val = float(text)
        return -val if negative else val
    except ValueError:
        return None


class SDCERSScraper(BaseScraper):
    pension_fund = "SDCERS"

    def __init__(self):
        source = SOURCES["sdcers"]
        self.source_url = source["url"]
        self.pdf_path = None

    def fetch(self) -> None:
        self.pdf_path = download_pdf(self.source_url, "sdcers_pe_latest.pdf")

    def parse(self) -> list[RawRecord]:
        if not self.pdf_path:
            raise RuntimeError("No PDF fetched. Call fetch() first.")

        pdf = pdfplumber.open(str(self.pdf_path))
        as_of_date = self._extract_date(pdf)
        records = []

        for page in pdf.pages:
            text = page.extract_text() or ""
            if "Schedule of Investments" not in text and "Schedule  of Investments" not in text:
                continue

            page_records = self._parse_page(page, as_of_date)
            records.extend(page_records)

        pdf.close()
        return records

    def _extract_date(self, pdf) -> str:
        for page in pdf.pages[:5]:
            text = page.extract_text() or ""
            match = re.search(r"period ending\s+(\w+\s+\d{1,2},?\s+\d{4})", text, re.IGNORECASE)
            if match:
                from app.ingestion.utils import parse_date_from_text
                date = parse_date_from_text(match.group(1))
                if date:
                    return date
        return "2024-06-30"

    def _parse_page(self, page, as_of_date: str) -> list[RawRecord]:
        """Parse fund-level data using word-level positional extraction."""
        words = page.extract_words(keep_blank_chars=True, x_tolerance=2, y_tolerance=2)

        # Group words by y-position into rows
        row_map: dict[float, list] = {}
        for w in words:
            y = round(w["top"], 0)
            matched_y = None
            for ey in row_map:
                if abs(ey - y) < 4:
                    matched_y = ey
                    break
            if matched_y is not None:
                row_map[matched_y].append(w)
            else:
                row_map[y] = [w]

        records = []
        for y in sorted(row_map):
            row_words = sorted(row_map[y], key=lambda w: w["x0"])
            record = self._parse_row(row_words, as_of_date)
            if record:
                records.append(record)

        return records

    def _extract_column(self, words: list[dict], col_name: str) -> str:
        """Extract text for a column by filtering words within the column's x-range."""
        x_start, x_end = COLUMNS[col_name]
        # Use tighter tolerance for numeric columns to prevent overlap
        tolerance = 5 if col_name in ("fund_name", "strategy") else 2
        col_words = [w for w in words if w["x0"] >= x_start - tolerance and w["x0"] < x_end]
        if not col_words:
            return ""

        sorted_words = sorted(col_words, key=lambda w: w["x0"])
        raw_texts = [w["text"] for w in sorted_words]

        # Check if this is character-spaced (mostly single chars)
        single_chars = sum(1 for t in raw_texts if len(t.strip()) <= 1)
        if single_chars > len(raw_texts) * 0.4 and len(raw_texts) > 3:
            # Character-spaced text: words with leading spaces indicate word boundaries
            # e.g., "C", "r", "e", "s", "t", "v", "i", "e", "w", " P", "a", "r", "t", ...
            result_parts = []
            current_word = ""
            for t in raw_texts:
                if t.startswith(" ") and current_word:
                    # Leading space = word boundary
                    result_parts.append(current_word)
                    current_word = t.strip()
                else:
                    current_word += t.strip()
            if current_word:
                result_parts.append(current_word)
            return " ".join(result_parts)
        else:
            return " ".join(t.strip() for t in raw_texts if t.strip())

    def _parse_row(self, words: list[dict], as_of_date: str) -> RawRecord | None:
        """Parse a row of words (positioned) into a RawRecord."""
        fund_name = self._extract_column(words, "fund_name").strip()
        vintage_str = self._extract_column(words, "vintage_year").strip()
        strategy = self._extract_column(words, "strategy").strip()
        committed_str = self._extract_column(words, "committed").strip()
        funded_str = self._extract_column(words, "funded").strip()
        distributed_str = self._extract_column(words, "distributed").strip()
        market_value_str = self._extract_column(words, "market_value").strip()
        tvm_str = self._extract_column(words, "tvm").strip()
        irr_str = self._extract_column(words, "irr").strip()

        # Validate: must have a fund name and vintage year
        if not fund_name or len(fund_name) < 3:
            return None

        # Parse vintage year
        vy_match = re.search(r"\b(19[9]\d|20[0-3]\d)\b", vintage_str)
        if not vy_match:
            return None
        vintage_year = int(vy_match.group(1))

        # Skip header/total/section rows
        skip_words = [
            "Schedule", "Past performance", "all underlying", "expenses",
            "Where indicated", "Subtotal", "Total", "Active PE", "Realized PE",
            "Private Equity", "Investment Type", "StepStone",
        ]
        for sw in skip_words:
            if sw.lower() in fund_name.lower():
                return None

        # Clean fund name (remove trailing periods/spaces)
        fund_name = fund_name.strip(" .")
        if fund_name.endswith(","):
            fund_name = fund_name[:-1]

        # Parse TVM
        tvm = None
        tvm_match = re.search(r"(\d+\.\d+)", tvm_str.replace("x", "").replace("X", ""))
        if tvm_match:
            tvm = float(tvm_match.group(1))

        # Parse IRR
        irr = None
        irr_clean = irr_str.replace("%", "").replace("NM", "").strip()
        if irr_clean:
            try:
                irr = float(irr_clean)
            except ValueError:
                # Try extracting just the number
                m = re.search(r"-?\d+\.?\d*", irr_clean)
                if m:
                    irr = float(m.group())

        # Parse financial numbers
        committed = _parse_number(committed_str)
        funded = _parse_number(funded_str)
        distributed = _parse_number(distributed_str)
        market_value = _parse_number(market_value_str)

        # Normalize strategy name
        strategy_clean = strategy.strip()
        if not strategy_clean:
            strategy_clean = None

        raw_data = {
            "fund_name": fund_name,
            "vintage": vintage_str,
            "strategy": strategy_clean,
            "committed": committed_str,
            "funded": funded_str,
            "distributed": distributed_str,
            "market_value": market_value_str,
            "tvm": tvm_str,
            "irr": irr_str,
        }

        return RawRecord(
            pension_fund=self.pension_fund,
            raw_fund_name=fund_name,
            vintage_year=vintage_year,
            commitment_usd=committed,
            contributed_usd=funded,
            distributed_usd=distributed,
            nav_usd=market_value,
            net_irr=irr,
            investment_multiple=tvm,
            as_of_date=as_of_date,
            source_url=self.source_url,
            raw_json=json.dumps(raw_data),
        )
