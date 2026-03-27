from __future__ import annotations

import statistics
from typing import Optional

from fastapi import APIRouter, Query, UploadFile, File, Form, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse

from app.database import get_connection
from app.export.exporter import export_csv, export_json
from app.ingestion.calpers import CalPERSScraper
from app.ingestion.manual_upload import parse_upload
from app.normalization.normalize import ingest_records
from app.dedup.fuzzy_match import get_dedup_summary

router = APIRouter(prefix="/api")

SCRAPERS = {
    "calpers": CalPERSScraper,
}

VALID_SORT_COLS = {
    "fund_name", "gp_name", "pension_fund", "vintage_year", "strategy",
    "commitment_usd", "contributed_usd", "distributed_usd", "nav_usd",
    "tvpi", "dpi", "rvpi", "net_irr", "as_of_date",
}


def _build_where(
    pension_fund: str | None,
    gp_name: str | None,
    strategy: str | None,
    vintage_min: int | None,
    vintage_max: int | None,
    irr_min: float | None,
    irr_max: float | None,
    tvpi_min: float | None,
    tvpi_max: float | None,
) -> tuple[str, list]:
    clauses = []
    params = []
    if pension_fund:
        clauses.append("pension_fund = ?")
        params.append(pension_fund)
    if gp_name:
        clauses.append("gp_name LIKE ?")
        params.append(f"%{gp_name}%")
    if strategy:
        clauses.append("strategy = ?")
        params.append(strategy)
    if vintage_min is not None:
        clauses.append("vintage_year >= ?")
        params.append(vintage_min)
    if vintage_max is not None:
        clauses.append("vintage_year <= ?")
        params.append(vintage_max)
    if irr_min is not None:
        clauses.append("net_irr >= ?")
        params.append(irr_min)
    if irr_max is not None:
        clauses.append("net_irr <= ?")
        params.append(irr_max)
    if tvpi_min is not None:
        clauses.append("tvpi >= ?")
        params.append(tvpi_min)
    if tvpi_max is not None:
        clauses.append("tvpi <= ?")
        params.append(tvpi_max)
    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params


@router.get("/funds")
def list_funds(
    pension_fund: Optional[str] = None,
    gp_name: Optional[str] = None,
    strategy: Optional[str] = None,
    vintage_min: Optional[int] = None,
    vintage_max: Optional[int] = None,
    irr_min: Optional[float] = None,
    irr_max: Optional[float] = None,
    tvpi_min: Optional[float] = None,
    tvpi_max: Optional[float] = None,
    sort_by: str = "fund_name",
    sort_dir: str = "asc",
    page: int = 1,
    page_size: int = 50,
):
    if sort_by not in VALID_SORT_COLS:
        sort_by = "fund_name"
    if sort_dir.lower() not in ("asc", "desc"):
        sort_dir = "asc"

    where, params = _build_where(
        pension_fund, gp_name, strategy, vintage_min, vintage_max,
        irr_min, irr_max, tvpi_min, tvpi_max,
    )

    offset = (page - 1) * page_size

    with get_connection() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM funds WHERE {where}", params).fetchone()[0]
        rows = conn.execute(
            f"""SELECT * FROM funds WHERE {where}
                ORDER BY {sort_by} {sort_dir}
                LIMIT ? OFFSET ?""",
            params + [page_size, offset],
        ).fetchall()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": [dict(r) for r in rows],
    }


@router.get("/stats")
def get_stats(
    pension_fund: Optional[str] = None,
    gp_name: Optional[str] = None,
    strategy: Optional[str] = None,
    vintage_min: Optional[int] = None,
    vintage_max: Optional[int] = None,
    irr_min: Optional[float] = None,
    irr_max: Optional[float] = None,
    tvpi_min: Optional[float] = None,
    tvpi_max: Optional[float] = None,
):
    where, params = _build_where(
        pension_fund, gp_name, strategy, vintage_min, vintage_max,
        irr_min, irr_max, tvpi_min, tvpi_max,
    )

    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT net_irr, tvpi, vintage_year, strategy, pension_fund FROM funds WHERE {where}",
            params,
        ).fetchall()

    if not rows:
        return {"total_funds": 0}

    irrs = [r["net_irr"] for r in rows if r["net_irr"] is not None]
    tvpis = [r["tvpi"] for r in rows if r["tvpi"] is not None]

    def _stats(values):
        if not values:
            return None, None
        return round(statistics.median(values), 2), round(statistics.mean(values), 2)

    med_irr, mean_irr = _stats(irrs)
    med_tvpi, mean_tvpi = _stats(tvpis)

    # Group by vintage
    by_vintage = {}
    for r in rows:
        vy = r["vintage_year"]
        if vy is None:
            continue
        by_vintage.setdefault(vy, {"irrs": [], "tvpis": []})
        if r["net_irr"] is not None:
            by_vintage[vy]["irrs"].append(r["net_irr"])
        if r["tvpi"] is not None:
            by_vintage[vy]["tvpis"].append(r["tvpi"])

    vintage_stats = []
    for vy in sorted(by_vintage):
        d = by_vintage[vy]
        m_irr, a_irr = _stats(d["irrs"])
        m_tvpi, a_tvpi = _stats(d["tvpis"])
        vintage_stats.append({
            "vintage_year": vy, "count": len(d["irrs"]) + len(d["tvpis"]),
            "median_irr": m_irr, "mean_irr": a_irr,
            "median_tvpi": m_tvpi, "mean_tvpi": a_tvpi,
        })

    # Group by strategy
    by_strategy = {}
    for r in rows:
        s = r["strategy"] or "Other"
        by_strategy.setdefault(s, {"irrs": [], "tvpis": []})
        if r["net_irr"] is not None:
            by_strategy[s]["irrs"].append(r["net_irr"])
        if r["tvpi"] is not None:
            by_strategy[s]["tvpis"].append(r["tvpi"])

    strategy_stats = []
    for s in sorted(by_strategy):
        d = by_strategy[s]
        m_irr, a_irr = _stats(d["irrs"])
        m_tvpi, a_tvpi = _stats(d["tvpis"])
        strategy_stats.append({
            "group": s, "count": len(d["irrs"]),
            "median_irr": m_irr, "mean_irr": a_irr,
            "median_tvpi": m_tvpi, "mean_tvpi": a_tvpi,
        })

    # Group by pension fund
    by_pension = {}
    for r in rows:
        p = r["pension_fund"]
        by_pension.setdefault(p, {"irrs": [], "tvpis": []})
        if r["net_irr"] is not None:
            by_pension[p]["irrs"].append(r["net_irr"])
        if r["tvpi"] is not None:
            by_pension[p]["tvpis"].append(r["tvpi"])

    pension_stats = []
    for p in sorted(by_pension):
        d = by_pension[p]
        m_irr, a_irr = _stats(d["irrs"])
        m_tvpi, a_tvpi = _stats(d["tvpis"])
        pension_stats.append({
            "group": p, "count": len(d["irrs"]),
            "median_irr": m_irr, "mean_irr": a_irr,
            "median_tvpi": m_tvpi, "mean_tvpi": a_tvpi,
        })

    return {
        "total_funds": len(rows),
        "median_irr": med_irr,
        "mean_irr": mean_irr,
        "median_tvpi": med_tvpi,
        "mean_tvpi": mean_tvpi,
        "by_vintage": vintage_stats,
        "by_strategy": strategy_stats,
        "by_pension": pension_stats,
    }


@router.get("/export/csv")
def export_csv_endpoint(
    pension_fund: Optional[str] = None,
    gp_name: Optional[str] = None,
    strategy: Optional[str] = None,
    vintage_min: Optional[int] = None,
    vintage_max: Optional[int] = None,
    irr_min: Optional[float] = None,
    irr_max: Optional[float] = None,
    tvpi_min: Optional[float] = None,
    tvpi_max: Optional[float] = None,
):
    filters = {k: v for k, v in {
        "pension_fund": pension_fund, "gp_name": gp_name, "strategy": strategy,
        "vintage_min": vintage_min, "vintage_max": vintage_max,
        "irr_min": irr_min, "irr_max": irr_max,
        "tvpi_min": tvpi_min, "tvpi_max": tvpi_max,
    }.items() if v is not None}

    data = export_csv(filters if filters else None)
    return PlainTextResponse(
        content=data,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=pe_performance.csv"},
    )


@router.get("/export/json")
def export_json_endpoint(
    pension_fund: Optional[str] = None,
    gp_name: Optional[str] = None,
    strategy: Optional[str] = None,
    vintage_min: Optional[int] = None,
    vintage_max: Optional[int] = None,
    irr_min: Optional[float] = None,
    irr_max: Optional[float] = None,
    tvpi_min: Optional[float] = None,
    tvpi_max: Optional[float] = None,
):
    filters = {k: v for k, v in {
        "pension_fund": pension_fund, "gp_name": gp_name, "strategy": strategy,
        "vintage_min": vintage_min, "vintage_max": vintage_max,
        "irr_min": irr_min, "irr_max": irr_max,
        "tvpi_min": tvpi_min, "tvpi_max": tvpi_max,
    }.items() if v is not None}

    data = export_json(filters if filters else None)
    return JSONResponse(content=data)


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    pension_fund: str = Form(...),
    as_of_date: str = Form(...),
):
    content = await file.read()
    try:
        raw_records = parse_upload(content, file.filename, pension_fund, as_of_date)
        normalized = ingest_records(raw_records)
        return {"status": "ok", "records_ingested": len(normalized)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/refresh")
def refresh_source(source: str = "all"):
    results = {}
    sources = list(SCRAPERS.keys()) if source == "all" else [source]

    for src in sources:
        scraper_cls = SCRAPERS.get(src)
        if not scraper_cls:
            results[src] = {"status": "error", "message": f"Unknown source: {src}"}
            continue
        try:
            scraper = scraper_cls()
            raw_records = scraper.run()
            normalized = ingest_records(raw_records)
            results[src] = {"status": "ok", "records": len(normalized)}
        except Exception as e:
            results[src] = {"status": "error", "message": str(e)}

    return results


@router.get("/sources")
def list_sources():
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT pension_fund, COUNT(*) as count,
                      MAX(as_of_date) as latest_date,
                      MAX(created_at) as last_refresh
               FROM funds GROUP BY pension_fund"""
        ).fetchall()
    return [dict(r) for r in rows]


@router.get("/dedup")
def dedup_matches():
    return get_dedup_summary()


@router.get("/strategies")
def list_strategies():
    with get_connection() as conn:
        rows = conn.execute("SELECT DISTINCT strategy FROM funds ORDER BY strategy").fetchall()
    return [r["strategy"] for r in rows]


@router.get("/pension-funds")
def list_pension_funds():
    with get_connection() as conn:
        rows = conn.execute("SELECT DISTINCT pension_fund FROM funds ORDER BY pension_fund").fetchall()
    return [r["pension_fund"] for r in rows]
