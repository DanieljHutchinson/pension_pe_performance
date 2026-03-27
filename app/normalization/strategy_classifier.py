import re

# Strategy classification rules: (pattern, strategy)
# Checked in order; first match wins
STRATEGY_RULES = [
    (r"\bventure\b|\bvc\b|\bventure\s+capital\b", "Venture Capital"),
    (r"\bgrowth\s+equity\b|\bgrowth\b", "Growth Equity"),
    (r"\bsecondary\b|\bsecondaries\b|\bstrategic\s+partners\b|\bcoller\b|\blexington\b", "Secondaries"),
    (r"\bfund[\s-]*of[\s-]*funds?\b|\bfof\b", "Fund of Funds"),
    (r"\bco[\s-]*invest\b|\bco[\s-]*investment\b|\bdirect\b", "Co-Investment"),
    (r"\bcredit\b|\bdebt\b|\bmezzanine\b|\bdistressed\b|\bopportunit", "Credit/Distressed"),
    (r"\benergy\b|\bpower\b|\brenewable\b|\bmidstream\b|\binfrastructure\b", "Energy/Infrastructure"),
    (r"\breal\s+estate\b|\brealty\b|\bproperty\b", "Real Estate"),
    (r"\bbuyout\b|\bcapital\s+partners\b|\bequity\s+partners\b|\blbo\b", "Buyout"),
    (r"\btimber\b|\bnatural\s+resource\b|\bagriculture\b", "Natural Resources"),
]


def classify_strategy(fund_name: str) -> str:
    """Classify a fund's strategy based on its name.

    Returns one of: Buyout, Venture Capital, Growth Equity, Credit/Distressed,
    Secondaries, Fund of Funds, Co-Investment, Energy/Infrastructure,
    Real Estate, Natural Resources, Other.

    Defaults to 'Buyout' as the most common PE strategy.
    """
    if not fund_name:
        return "Other"

    lower = fund_name.lower()

    for pattern, strategy in STRATEGY_RULES:
        if re.search(pattern, lower):
            return strategy

    # Default: most PE funds without specific keywords are buyout
    return "Buyout"
