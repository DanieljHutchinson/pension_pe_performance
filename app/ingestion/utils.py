from __future__ import annotations

import re
from pathlib import Path

import httpx

from app.config import DOWNLOAD_DIR, USER_AGENT


def parse_currency(text: str) -> float | None:
    """Parse currency strings like '$1,234,567', '(1,234)', '-' into float."""
    if not text or not isinstance(text, str):
        return None
    text = text.strip()
    if text in ("-", "--", "N/A", "n/a", ""):
        return None

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]

    text = text.replace("$", "").replace(",", "").strip()
    if not text:
        return None

    try:
        value = float(text)
        return -value if negative else value
    except ValueError:
        return None


def parse_percentage(text: str) -> float | None:
    """Parse percentage strings like '8.5%', '(9.47)', '-' into float."""
    if not text or not isinstance(text, str):
        return None
    text = text.strip()
    if text in ("-", "--", "N/A", "n/a", ""):
        return None

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]

    text = text.replace("%", "").strip()
    if not text:
        return None

    try:
        value = float(text)
        return -value if negative else value
    except ValueError:
        return None


def parse_multiple(text: str) -> float | None:
    """Parse investment multiple like '1.45x', '1.45', '(0.8)'."""
    if not text or not isinstance(text, str):
        return None
    text = text.strip()
    if text in ("-", "--", "N/A", "n/a", ""):
        return None

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]

    text = text.replace("x", "").replace("X", "").strip()
    try:
        value = float(text)
        return -value if negative else value
    except ValueError:
        return None


def parse_date_from_text(text: str) -> str | None:
    """Extract date from text like 'As of June 30, 2025' -> '2025-06-30'."""
    months = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12",
    }
    pattern = r"(?:as\s+of\s+)?(\w+)\s+(\d{1,2}),?\s+(\d{4})"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        month_str = match.group(1).lower()
        day = int(match.group(2))
        year = match.group(3)
        month = months.get(month_str)
        if month:
            return f"{year}-{month}-{day:02d}"
    return None


def download_pdf(url: str, filename: str | None = None) -> Path:
    """Download a PDF and cache it in the downloads directory."""
    if filename is None:
        filename = url.split("/")[-1]
    dest = DOWNLOAD_DIR / filename
    if dest.exists():
        return dest

    response = httpx.get(url, headers={"User-Agent": USER_AGENT}, follow_redirects=True, timeout=60)
    response.raise_for_status()
    dest.write_bytes(response.content)
    return dest
