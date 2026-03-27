import sqlite3
from contextlib import contextmanager

from app.config import DB_PATH

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pension_fund TEXT NOT NULL,
    raw_fund_name TEXT NOT NULL,
    vintage_year INTEGER,
    commitment_usd REAL,
    contributed_usd REAL,
    distributed_usd REAL,
    nav_usd REAL,
    net_irr REAL,
    investment_multiple REAL,
    as_of_date TEXT NOT NULL,
    source_url TEXT,
    ingested_at TEXT DEFAULT (datetime('now')),
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS funds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fund_name TEXT NOT NULL,
    gp_name TEXT,
    pension_fund TEXT NOT NULL,
    vintage_year INTEGER,
    strategy TEXT,
    commitment_usd REAL,
    contributed_usd REAL,
    distributed_usd REAL,
    nav_usd REAL,
    tvpi REAL,
    dpi REAL,
    rvpi REAL,
    net_irr REAL,
    as_of_date TEXT NOT NULL,
    source_url TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    dedup_key TEXT,
    UNIQUE(dedup_key, pension_fund, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_funds_gp ON funds(gp_name);
CREATE INDEX IF NOT EXISTS idx_funds_vintage ON funds(vintage_year);
CREATE INDEX IF NOT EXISTS idx_funds_strategy ON funds(strategy);
CREATE INDEX IF NOT EXISTS idx_funds_pension ON funds(pension_fund);
CREATE INDEX IF NOT EXISTS idx_funds_dedup ON funds(dedup_key);
"""


def init_db():
    with get_connection() as conn:
        conn.executescript(SCHEMA_SQL)


@contextmanager
def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
