from thefuzz import fuzz

from app.database import get_connection


def find_cross_pension_matches(threshold: int = 88) -> list[dict]:
    """Find funds that appear across multiple pension sources.

    Uses fuzzy matching on gp_name + fund_name with exact vintage year match.
    Returns list of match groups.
    """
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT id, fund_name, gp_name, pension_fund, vintage_year, dedup_key
               FROM funds ORDER BY dedup_key"""
        ).fetchall()

    funds = [dict(r) for r in rows]
    matches = []
    matched_ids = set()

    for i, a in enumerate(funds):
        if a["id"] in matched_ids:
            continue
        group = [a]

        for j in range(i + 1, len(funds)):
            b = funds[j]
            if b["id"] in matched_ids:
                continue
            if a["pension_fund"] == b["pension_fund"]:
                continue
            if a["vintage_year"] != b["vintage_year"]:
                continue

            name_a = f"{a['gp_name'] or ''} {a['fund_name']}".strip()
            name_b = f"{b['gp_name'] or ''} {b['fund_name']}".strip()

            score = fuzz.token_sort_ratio(name_a.lower(), name_b.lower())
            if score >= threshold:
                group.append(b)
                matched_ids.add(b["id"])

        if len(group) > 1:
            matched_ids.add(a["id"])
            matches.append({
                "funds": group,
                "pension_funds": list({f["pension_fund"] for f in group}),
                "fund_name": a["fund_name"],
                "vintage_year": a["vintage_year"],
            })

    return matches


def get_dedup_summary() -> dict:
    """Return summary of cross-pension matches."""
    matches = find_cross_pension_matches()
    return {
        "total_match_groups": len(matches),
        "funds_matched": sum(len(m["funds"]) for m in matches),
        "matches": matches[:50],  # limit for display
    }
