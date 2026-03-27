import re

# Manual overrides for known GP names that don't follow standard patterns
GP_OVERRIDES = {
    "kkr": "KKR",
    "tpg": "TPG",
    "eqt": "EQT",
    "cvc": "CVC",
    "ares": "Ares Management",
    "apollo": "Apollo Global Management",
    "blackstone": "Blackstone",
    "carlyle": "The Carlyle Group",
    "warburg pincus": "Warburg Pincus",
    "bain capital": "Bain Capital",
    "hellman & friedman": "Hellman & Friedman",
    "silver lake": "Silver Lake",
    "thoma bravo": "Thoma Bravo",
    "vista equity": "Vista Equity Partners",
    "advent international": "Advent International",
    "general atlantic": "General Atlantic",
    "leonard green": "Leonard Green & Partners",
    "permira": "Permira",
    "providence equity": "Providence Equity Partners",
    "welsh carson": "Welsh, Carson, Anderson & Stowe",
    "new enterprise": "New Enterprise Associates",
    "sequoia": "Sequoia Capital",
    "kleiner": "Kleiner Perkins",
    "andreessen": "Andreessen Horowitz",
    "greylock": "Greylock Partners",
    "benchmark": "Benchmark Capital",
    "insight partners": "Insight Partners",
    "gtcr": "GTCR",
    "madison dearborn": "Madison Dearborn Partners",
    "cerberus": "Cerberus Capital Management",
    "kohlberg": "Kohlberg & Company",
    "hig capital": "H.I.G. Capital",
    "clearlake": "Clearlake Capital",
    "genstar": "Genstar Capital",
    "berkshire partners": "Berkshire Partners",
}

# Patterns that indicate fund number suffixes
FUND_NUMBER_PATTERN = re.compile(
    r"""
    \s+                          # space before number
    (?:
        (?:Fund\s+)?             # optional "Fund" prefix
        (?:
            [IVXLCDM]+          # Roman numerals
            |[0-9]+             # Arabic numerals
        )
    )
    (?:\s*[A-D])?               # optional tranche letter
    (?:\s*[-,]\s*\w+)*          # optional sub-fund suffixes
    (?:\s*,?\s*(?:L\.?P\.?|LLC|Inc\.?|Ltd\.?))?  # entity suffix
    \s*$
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Common entity suffixes to strip
ENTITY_SUFFIXES = re.compile(
    r",?\s*(?:L\.?P\.?|LLC|L\.?L\.?C\.?|Inc\.?|Ltd\.?|Co\.?|Corp\.?)\s*$",
    re.IGNORECASE,
)


def extract_gp_name(fund_name: str) -> str:
    """Extract the GP/manager name from a fund name.

    Examples:
        'Apollo Investment Fund IX, L.P.' -> 'Apollo Global Management'
        'Blackstone Capital Partners VII L.P.' -> 'Blackstone'
        'Acme Ventures Fund III' -> 'Acme Ventures'
    """
    if not fund_name:
        return ""

    cleaned = fund_name.strip()

    # Check overrides first (case-insensitive prefix match)
    lower = cleaned.lower()
    for prefix, gp_name in GP_OVERRIDES.items():
        if lower.startswith(prefix):
            return gp_name

    # Strip entity suffixes
    name = ENTITY_SUFFIXES.sub("", cleaned).strip()

    # Try to find where the fund number/series starts
    # Look for Roman or Arabic numerals preceded by common patterns
    patterns = [
        r"^(.+?)\s+Fund\s+(?:[IVXLCDM]+|[0-9]+)",
        r"^(.+?)\s+Capital\s+Partners\s+(?:[IVXLCDM]+|[0-9]+)",
        r"^(.+?)\s+Partners\s+(?:[IVXLCDM]+|[0-9]+)",
        r"^(.+?)\s+Equity\s+Partners\s+(?:[IVXLCDM]+|[0-9]+)",
        r"^(.+?)\s+(?:[IVXLCDM]{2,}|[0-9]+)\s*$",  # trailing roman/arabic
        r"^(.+?)\s+\d{4}\s+Fund",  # "KKR 2006 Fund"
    ]

    for pattern in patterns:
        match = re.match(pattern, name, re.IGNORECASE)
        if match:
            gp = match.group(1).strip()
            # Strip trailing common words
            gp = re.sub(r"\s+(?:Private|Growth|Venture|Special|Strategic)\s*$", "", gp, flags=re.IGNORECASE)
            if len(gp) >= 2:
                return gp

    # Fallback: return the full name (minus entity suffix) as the GP
    return name
