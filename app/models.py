from pydantic import BaseModel, computed_field
from typing import Optional


class RawRecord(BaseModel):
    pension_fund: str
    raw_fund_name: str
    vintage_year: Optional[int] = None
    commitment_usd: Optional[float] = None
    contributed_usd: Optional[float] = None
    distributed_usd: Optional[float] = None
    nav_usd: Optional[float] = None
    net_irr: Optional[float] = None
    investment_multiple: Optional[float] = None
    as_of_date: str
    source_url: Optional[str] = None
    raw_json: Optional[str] = None


class FundRecord(BaseModel):
    id: Optional[int] = None
    fund_name: str
    gp_name: Optional[str] = None
    pension_fund: str
    vintage_year: Optional[int] = None
    strategy: Optional[str] = None
    commitment_usd: Optional[float] = None
    contributed_usd: Optional[float] = None
    distributed_usd: Optional[float] = None
    nav_usd: Optional[float] = None
    tvpi: Optional[float] = None
    dpi: Optional[float] = None
    rvpi: Optional[float] = None
    net_irr: Optional[float] = None
    as_of_date: str
    source_url: Optional[str] = None
    dedup_key: Optional[str] = None


class FundFilter(BaseModel):
    pension_fund: Optional[str] = None
    gp_name: Optional[str] = None
    strategy: Optional[str] = None
    vintage_min: Optional[int] = None
    vintage_max: Optional[int] = None
    irr_min: Optional[float] = None
    irr_max: Optional[float] = None
    tvpi_min: Optional[float] = None
    tvpi_max: Optional[float] = None
    sort_by: str = "fund_name"
    sort_dir: str = "asc"
    page: int = 1
    page_size: int = 50


class VintageStats(BaseModel):
    vintage_year: int
    count: int
    median_irr: Optional[float] = None
    mean_irr: Optional[float] = None
    median_tvpi: Optional[float] = None
    mean_tvpi: Optional[float] = None


class GroupStats(BaseModel):
    group: str
    count: int
    median_irr: Optional[float] = None
    mean_irr: Optional[float] = None
    median_tvpi: Optional[float] = None
    mean_tvpi: Optional[float] = None


class SummaryStats(BaseModel):
    total_funds: int
    median_irr: Optional[float] = None
    mean_irr: Optional[float] = None
    median_tvpi: Optional[float] = None
    mean_tvpi: Optional[float] = None
    by_vintage: list[VintageStats] = []
    by_strategy: list[GroupStats] = []
    by_pension: list[GroupStats] = []
